# Collector `Data` layout investigation (working notes)

Throwaway working doc. Tracks hypotheses about `felix/collector/stats.go`'s
`Data` struct, what was measured, what held up and what didn't.
Prototypes/benchmarks: `zz_layout_bench_test.go`, `zz_size_probe_test.go`
(same package, not for merge).

Targets: 10k+ new flows/s (want 100k/s), ~2M flows resident in `epStats`.
Machine: i9-8950HK, go1.27.1, benchmarks at 1M flows (≈ 90 ns per DRAM miss).

## Baseline (measured)

`Data` = **728 B** (12 cache lines); with map overhead **919 B/flow**.
`RuleTrace` = 192 B, two inline = 384 B (53% of `Data`). `Tuple` = 56 B.
`proxy.ServicePortName` = 64 B inline.

| off | size | field | notes |
|----:|-----:|-------|-------|
| 0 | 56 | Tuple | also the map key |
| 56 | 32 | SrcEp/DstEp | 2 interfaces |
| 88 | 1+16+8 | IsDNAT/PreDNATAddr/PreDNATPort | 7 B pad after |
| 120 | 64 | DstSvc | 4 string headers; empty for most flows |
| 184 | 2 | IsConnection/IsProxied | 6 B pad |
| 192 | 8 | NatOutgoingPort | |
| 200 | 64 | 4× conntrack Counter | 2× int each |
| 264 | 384 | Ingress/EgressRuleTrace | 10-slot `pathArray` each; one direction usually unused |
| 648 | 48 | Ingress/EgressPendingRuleIDs | nil unless staged policy |
| 696 | 24 | updatedAt/ruleUpdatedAt/lastPolicyEvalAt | ns monotime |
| 720 | 4 | Reported/UnreportedPacketInfo/dirty/Expired | 4 B pad |

At 2M flows: ~1.8 GB (1.46 GB `Data` + map). Every `Data` is pointerful
(2 ifaces, 4 strings, 2×(2 slices + 10 ptrs), 2 slices) so GC marks all of it:
**136 ms per GC cycle per 1M flows** with only this structure live.

## Hot paths and which cache lines they touch

1. **Conntrack update** (every live flow, every scan/poll period — the
   dominant path): map lookup → `getDataAndUpdateEndpoints` (Reported,
   UnreportedPacketInfo, SrcEp, DstEp) → IsDNAT, NatOutgoingPort → 4 counters,
   IsConnection, dirty, updatedAt. Lines **0,1,2,3,(4),10,11** = 7 of 12.
   Lines 0-3 are contiguous so the adjacent-line prefetcher hides some of it.
2. **`checkEpStats` sweep** (whole map, every `ExportingInterval`, default
   **1 s**): Tuple.Proto (0), dirty/Reported (11), ruleUpdatedAt (11),
   updatedAt (10). Map-iteration + pointer chase per flow.
3. **Policy re-eval sweep**: snapshot copies all pointers (16 MB at 2M), then
   per flow lastPolicyEvalAt (11), Tuple (0), a *second* map lookup to check
   the entry is still live, SrcEp/DstEp (0-1).
4. **nflog / packet info** (per new flow, per policy change): RuleTrace
   path/verdictIdx/lastMatchIdx/counters (lines 4-10).
5. **Report** (per flow per flush): everything.

## Results

### Memory / GC (1M flows, `TestZZBytesPerFlow*`)

| layout | B/flow | GC cycle (structure live) |
|--------|-------:|--------------------------:|
| current (`map[Tuple]*Data`) | 919 | 136 ms |
| v2 repacked (`map[Tuple]*dataV2`, 200 B, no trace) | 359 | 55 ms |
| v2 + one allocated trace (2-entry path) | 415 | — |
| v2 in slab (`[]dataV2` + `map[Tuple]uint32`) | 311 | **11 ms** |

At 2M flows the repack saves ~1 GB RSS. The slab's GC win is the pointer
density: one big object scanned linearly vs 2M small pointerful objects.

### Sweep (`checkEpStats` shape, per flow)

| layout | ns/flow | 2M flows |
|--------|--------:|---------:|
| current map iteration | 62 | 125 ms/s ≈ 12% core at 1 s interval |
| v2 map iteration (smaller struct) | 63 | same — **struct size irrelevant** |
| slab, linear over `[]dataV2` | 7.2 | 14 ms/s |
| dense hot-header array (16 B/flow) | 1.4 | 3 ms/s |

### Conntrack update (random flow of 1M, lookup + counter/timestamp writes)

| layout | ns/op |
|--------|------:|
| map lookup only (no Data access) | 165-190 |
| current | 465-495 |
| v2 map | 390-425 (≈ −12%) |
| v2 slab via `map[Tuple]uint32` index | 480-585 (**worse**: 3rd dependent miss) |
| v2 slab via `map[Tuple]*dataV2` pointer | 390 (= v2 map) |
| map with 40 B packed tuple key | 152-169 vs 166-169 (noise) |

The update path is ~2 DRAM misses (map slot, Data) either way; the repack
only trims the tail lines the prefetcher wasn't covering.

### New-flow allocation

| layout | ns/op | B/op | allocs |
|--------|------:|-----:|-------:|
| `NewData` | 170 | 768 | 1 |
| `dataV2` (trace nil) | 86 | 208 | 1 |
| `dataV2` + separately allocated trace | 152 | 328 | 3 |

A lazily allocated trace gives back most of the allocation win; traces want to
be inline-compact or slab-allocated, not separate heap objects.

## Hypothesis status

| id | hypothesis | status |
|----|------------|--------|
| H1 | Sweeps are dominated by map iteration + pointer chase, not field work; a dense side array makes them ~10× cheaper. | **confirmed** (9× slab linear, 45× hot header) |
| H2 | Coalescing 10 bools into a flags word is a rounding error alone. | confirmed by arithmetic (~20 B incl. padding); only worth it inside a repack |
| H3 | Timestamps as uint32 at 1/16 s relative to collector start are sufficient. | analysis: consumers are 10 s age, 5 s reporting delay, BPF conntrack timeouts (≤ hours), policy-eval min interval (~2 min); `lastPolicyEvalAt == 0` "never" sentinel survives if the epoch is set one tick before start. Wraps at 8.5 years. |
| H4 | DstSvc + pre-DNAT behind one pointer saves ~80 B for non-DNAT flows. | part of the 919→359 result; not isolated |
| H5 | One RuleTrace is almost always empty; `pathArray[10]` oversized. | confirmed on size (384 B → ~90-130 B per used direction); see alloc caveat |
| H6 | Repack (~200 B, hot on lines 0-2) cuts update-path lines 7→3 and ~1 GB RSS at 2M. | RSS confirmed; update-path CPU only −12% |
| H7 | Fewer pointers per Data cuts GC mark work proportionally. | confirmed: 136 → 55 → 11 ms per cycle per 1M |
| H8 | 40 B packed tuple key speeds lookups. | **refuted** — lookups are miss-bound |

## Didn't work / rejected

- **Slab addressed through `map[Tuple]uint32`**: adds a dependent miss on the
  update path (+20-40%). Reach the slab through `map[Tuple]*dataV2` instead
  (pointer into the slab): same update cost as a heap object, linear sweeps.
- **Smaller struct to speed the sweep**: zero effect while the sweep walks a
  Go map — iteration order is random and the value is a pointer. Only a
  contiguous slab (or side array) helps.
- **Packed 40 B tuple key**: no measurable change.
- **Lazily heap-allocating RuleTrace**: saves memory but costs 2 extra allocs
  per new flow, eating most of the alloc-path win. Prefer a compact inline
  trace (path `[3]*RuleID` inline + overflow slice pointer, ~80 B/dir) or
  slab-allocated traces.

## Non-layout finding

`getDataAndUpdateEndpoints` calls `findEndpointBestMatch` (two RLock'd map
lookups, plus NetworkSet lookups for unknown IPs) *before* the
`data.Reported && !UnreportedPacketInfo && !packetinfo` early return, which
discards them. On the steady-state conntrack path that's ~2 extra random
misses per update — same order as the whole Data access. Reordering the check
is a one-line win independent of layout.

## Suggested shape (if pursued)

1. `dataV2`-style repack: flags word, uint32 timestamps, `*natInfo`,
   compact inline traces `[2]traceV2`, `*pending`. ~280 B. Delivers the RSS
   and GC wins and the alloc-path win; API surface (`Data` methods) can stay.
2. Store `dataV2` in a slab (`[]dataV2` with free list) reached via
   `map[Tuple]*dataV2`; sweeps (`checkEpStats`, policy re-eval snapshot)
   iterate the slab linearly, and the re-eval sweep needs no snapshot or
   re-lookup (cursor + in-place liveness check). Slab must never move
   entries while pointers are live (chunked slab, not `append`-grown).
3. Optional: dense hot-header side array if sweep cost still matters after 2.
4. Move `findEndpointBestMatch` after the frozen-data early return.
