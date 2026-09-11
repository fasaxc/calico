package collector

// Throwaway prototypes + benchmarks for DATA-LAYOUT-NOTES.md.  Not for merge.

import (
	"encoding/binary"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	"github.com/gavv/monotime"
	"k8s.io/kubernetes/pkg/proxy"

	"github.com/projectcalico/calico/felix/calc"
	"github.com/projectcalico/calico/felix/collector/types/counter"
	"github.com/projectcalico/calico/felix/collector/types/tuple"
)

// ---------- prototype layouts ----------

type flagsV2 uint16

const (
	fDNAT flagsV2 = 1 << iota
	fConnection
	fProxied
	fReported
	fUnreportedPktInfo
	fDirty
	fExpired
)

// ruleTraceV2: path sized on demand rather than a 10-slot inline array.
type ruleTraceV2 struct {
	pktsCtr, bytesCtr         counter.Counter
	verdictIdx, lastMatchIdx int16
	flags                    uint8
	path                     []*calc.RuleID
	rulesToReport            []*calc.RuleID
}

type natInfoV2 struct {
	PreDNATAddr [16]byte
	PreDNATPort uint16
	DstSvc      proxy.ServicePortName
}

// dataV2: hot fields (counters, timestamps, flags) in the first 3 lines; cold, mostly-nil
// data behind pointers.
type dataV2 struct {
	Tuple        tuple.Tuple
	SrcEp, DstEp calc.EndpointData

	ct [4]counter.Counter // pkts, bytes, pktsRev, bytesRev

	updatedAt, ruleUpdatedAt, lastPolicyEvalAt uint32 // 1/16 s since collector start
	flags                                      flagsV2
	natOutgoingPort                            uint16

	nat     *natInfoV2
	traces  [2]*ruleTraceV2
	pending *[2][]*calc.RuleID
}

// hotHdr: the fields the sweeps read, kept in a dense side array (H1).
type hotHdr struct {
	updatedAt, ruleUpdatedAt uint32
	flags                    flagsV2
	proto                    uint8
	_                        uint8
	_                        uint32
}

var epochV2 = monotime.Now()

func nowV2() uint32 { return uint32((monotime.Now() - epochV2) / (time.Second / 16)) }

// ---------- fixtures ----------

func benchTuples(n int) []tuple.Tuple {
	ts := make([]tuple.Tuple, n)
	for i := range ts {
		var src, dst [16]byte
		copy(src[:], localIp1[:])
		copy(dst[:], remoteIp1[:])
		binary.BigEndian.PutUint32(dst[12:], uint32(i>>16)+0x0a000000)
		ts[i] = tuple.Make(src, dst, 6, 1024+int(i&0xffff), 80)
	}
	return ts
}

func buildCurrent(ts []tuple.Tuple) map[tuple.Tuple]*Data {
	m := make(map[tuple.Tuple]*Data, len(ts))
	for _, t := range ts {
		d := NewData(t, localEd1, remoteEd1)
		d.SetConntrackCounters(10, 1000)
		d.SetConntrackCountersReverse(5, 500)
		d.Reported = true
		m[t] = d
	}
	return m
}

func newDataV2(t tuple.Tuple) *dataV2 {
	now := nowV2()
	return &dataV2{Tuple: t, SrcEp: localEd1, DstEp: remoteEd1, updatedAt: now, ruleUpdatedAt: now, flags: fDirty | fReported}
}

func buildV2Map(ts []tuple.Tuple) map[tuple.Tuple]*dataV2 {
	m := make(map[tuple.Tuple]*dataV2, len(ts))
	for _, t := range ts {
		d := newDataV2(t)
		d.ct[0].Set(10)
		d.ct[1].Set(1000)
		d.ct[2].Set(5)
		d.ct[3].Set(500)
		m[t] = d
	}
	return m
}

type slabV2 struct {
	idx  map[tuple.Tuple]uint32
	data []dataV2
	hdr  []hotHdr
}

func buildSlab(ts []tuple.Tuple) *slabV2 {
	s := &slabV2{idx: make(map[tuple.Tuple]uint32, len(ts)), data: make([]dataV2, len(ts)), hdr: make([]hotHdr, len(ts))}
	for i, t := range ts {
		d := &s.data[i]
		*d = *newDataV2(t)
		d.ct[0].Set(10)
		d.ct[1].Set(1000)
		d.ct[2].Set(5)
		d.ct[3].Set(500)
		s.idx[t] = uint32(i)
		s.hdr[i] = hotHdr{updatedAt: d.updatedAt, ruleUpdatedAt: d.ruleUpdatedAt, flags: d.flags, proto: 6}
	}
	return s
}

// ---------- memory ----------

func heapDelta(build func() any) (bytes uint64, keep any) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	keep = build()
	runtime.GC()
	runtime.ReadMemStats(&after)
	return after.HeapAlloc - before.HeapAlloc, keep
}

func TestZZBytesPerFlow(t *testing.T) {
	const n = 1_000_000
	ts := benchTuples(n)
	report := func(name string, build func() any) {
		b, keep := heapDelta(build)
		start := time.Now()
		runtime.GC()
		gc := time.Since(start)
		t.Logf("%-10s %6.0f B/flow  GC(mark+sweep, this structure live)=%v", name, float64(b)/n, gc)
		runtime.KeepAlive(keep)
	}
	report("current", func() any { return buildCurrent(ts) })
	report("v2-map", func() any { return buildV2Map(ts) })
	report("v2-slab", func() any { return buildSlab(ts) })
}

// ---------- sweep (checkEpStats shape) ----------

const sweepN = 1_000_000

func BenchmarkSweepCurrent(b *testing.B) {
	m := buildCurrent(benchTuples(sweepN))
	minRule := monotime.Now() - 5*time.Second
	now := monotime.Now()
	b.ResetTimer()
	var hits int
	for range b.N {
		for _, d := range m {
			_ = d.Tuple.Proto
			if d.IsDirty() && (d.Reported || d.RuleUpdatedAt() < minRule) {
				hits++
			}
			if d.UpdatedAt() < now-10*time.Second {
				hits++
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/sweepN, "ns/flow")
	runtime.KeepAlive(hits)
}

func BenchmarkSweepV2Map(b *testing.B) {
	m := buildV2Map(benchTuples(sweepN))
	minRule := nowV2() - 5*16
	now := nowV2()
	b.ResetTimer()
	var hits int
	for range b.N {
		for _, d := range m {
			_ = d.Tuple.Proto
			if d.flags&fDirty != 0 && (d.flags&fReported != 0 || d.ruleUpdatedAt < minRule) {
				hits++
			}
			if d.updatedAt < now-10*16 {
				hits++
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/sweepN, "ns/flow")
	runtime.KeepAlive(hits)
}

func BenchmarkSweepSlabLinear(b *testing.B) {
	s := buildSlab(benchTuples(sweepN))
	minRule := nowV2() - 5*16
	now := nowV2()
	b.ResetTimer()
	var hits int
	for range b.N {
		for i := range s.data {
			d := &s.data[i]
			_ = d.Tuple.Proto
			if d.flags&fDirty != 0 && (d.flags&fReported != 0 || d.ruleUpdatedAt < minRule) {
				hits++
			}
			if d.updatedAt < now-10*16 {
				hits++
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/sweepN, "ns/flow")
	runtime.KeepAlive(hits)
}

func BenchmarkSweepHotHdr(b *testing.B) {
	s := buildSlab(benchTuples(sweepN))
	minRule := nowV2() - 5*16
	now := nowV2()
	b.ResetTimer()
	var hits int
	for range b.N {
		for i := range s.hdr {
			h := &s.hdr[i]
			_ = h.proto
			if h.flags&fDirty != 0 && (h.flags&fReported != 0 || h.ruleUpdatedAt < minRule) {
				hits++
			}
			if h.updatedAt < now-10*16 {
				hits++
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/sweepN, "ns/flow")
	runtime.KeepAlive(hits)
}

// ---------- conntrack update (handleCtInfo shape, minus endpoint lookups) ----------

const updN = 1_000_000

func randOrder(n int) []int {
	r := rand.New(rand.NewPCG(1, 2))
	return r.Perm(n)
}

func BenchmarkCTUpdateCurrent(b *testing.B) {
	ts := benchTuples(updN)
	m := buildCurrent(ts)
	order := randOrder(updN)
	b.ResetTimer()
	for i := range b.N {
		t := ts[order[i%updN]]
		d := m[t]
		if d.Reported && !d.UnreportedPacketInfo {
			// frozen endpoints: matches the common path in getDataAndUpdateEndpoints
		}
		if !d.IsDNAT {
			d.NatOutgoingPort = 0
		}
		d.SetConntrackCounters(10+i, 1000+i)
		d.SetConntrackCountersReverse(5+i, 500+i)
	}
}

func BenchmarkCTUpdateV2Map(b *testing.B) {
	ts := benchTuples(updN)
	m := buildV2Map(ts)
	order := randOrder(updN)
	b.ResetTimer()
	for i := range b.N {
		t := ts[order[i%updN]]
		d := m[t]
		if d.flags&(fReported|fUnreportedPktInfo) == fReported {
		}
		if d.flags&fDNAT == 0 {
			d.natOutgoingPort = 0
		}
		dirty := d.ct[0].Set(10+i) && d.ct[1].Set(1000+i)
		dirty = d.ct[2].Set(5+i) && d.ct[3].Set(500+i) || dirty
		if dirty {
			d.flags |= fDirty
		}
		d.flags |= fConnection
		d.updatedAt = nowV2()
	}
}

func BenchmarkCTUpdateSlab(b *testing.B) {
	ts := benchTuples(updN)
	s := buildSlab(ts)
	order := randOrder(updN)
	b.ResetTimer()
	for i := range b.N {
		t := ts[order[i%updN]]
		idx := s.idx[t]
		d := &s.data[idx]
		if d.flags&(fReported|fUnreportedPktInfo) == fReported {
		}
		if d.flags&fDNAT == 0 {
			d.natOutgoingPort = 0
		}
		dirty := d.ct[0].Set(10+i) && d.ct[1].Set(1000+i)
		dirty = d.ct[2].Set(5+i) && d.ct[3].Set(500+i) || dirty
		if dirty {
			d.flags |= fDirty
		}
		d.flags |= fConnection
		d.updatedAt = nowV2()
		h := &s.hdr[idx]
		h.flags, h.updatedAt = d.flags, d.updatedAt
	}
}

// Isolate the map-lookup cost: same lookups, no Data access.
func BenchmarkCTLookupOnly(b *testing.B) {
	ts := benchTuples(updN)
	m := buildCurrent(ts)
	order := randOrder(updN)
	b.ResetTimer()
	var p *Data
	for i := range b.N {
		p = m[ts[order[i%updN]]]
	}
	runtime.KeepAlive(p)
}

// ---------- round 2 ----------

// Slab reached via pointer map (no index indirection): update path should match V2Map while
// sweeps stay linear.
func BenchmarkCTUpdateSlabPtr(b *testing.B) {
	ts := benchTuples(updN)
	slab := make([]dataV2, updN)
	m := make(map[tuple.Tuple]*dataV2, updN)
	for i, t := range ts {
		slab[i] = *newDataV2(t)
		m[t] = &slab[i]
	}
	order := randOrder(updN)
	b.ResetTimer()
	for i := range b.N {
		d := m[ts[order[i%updN]]]
		if d.flags&fDNAT == 0 {
			d.natOutgoingPort = 0
		}
		dirty := d.ct[0].Set(10+i) && d.ct[1].Set(1000+i)
		dirty = d.ct[2].Set(5+i) && d.ct[3].Set(500+i) || dirty
		if dirty {
			d.flags |= fDirty
		}
		d.flags |= fConnection
		d.updatedAt = nowV2()
	}
}

// H8: packed 40 B tuple key.
type tuple40 struct {
	Src, Dst     [16]byte
	L4Src, L4Dst uint16
	Proto        uint8
	_            [3]uint8
}

func BenchmarkLookupTuple56(b *testing.B) {
	ts := benchTuples(updN)
	m := make(map[tuple.Tuple]*dataV2, updN)
	for _, t := range ts {
		m[t] = &dataV2{}
	}
	order := randOrder(updN)
	b.ResetTimer()
	var p *dataV2
	for i := range b.N {
		p = m[ts[order[i%updN]]]
	}
	runtime.KeepAlive(p)
}

func BenchmarkLookupTuple40(b *testing.B) {
	ts := benchTuples(updN)
	ks := make([]tuple40, updN)
	m := make(map[tuple40]*dataV2, updN)
	for i, t := range ts {
		ks[i] = tuple40{Src: t.Src, Dst: t.Dst, L4Src: uint16(t.L4Src), L4Dst: uint16(t.L4Dst), Proto: uint8(t.Proto)}
		m[ks[i]] = &dataV2{}
	}
	order := randOrder(updN)
	b.ResetTimer()
	var p *dataV2
	for i := range b.N {
		p = m[ks[order[i%updN]]]
	}
	runtime.KeepAlive(p)
}

// Fairer memory number: v2 with one direction's rule trace allocated (2-entry path), as a
// typical single-local-endpoint flow would have.
func TestZZBytesPerFlowWithTrace(t *testing.T) {
	const n = 1_000_000
	ts := benchTuples(n)
	b, keep := heapDelta(func() any {
		m := buildV2Map(ts)
		for _, d := range m {
			rt := &ruleTraceV2{verdictIdx: 1, path: make([]*calc.RuleID, 2)}
			rt.path[0], rt.path[1] = defTierPolicy1AllowIngressRuleID, defTierPolicy1AllowIngressRuleID
			d.traces[0] = rt
		}
		return m
	})
	t.Logf("v2-map+1trace %6.0f B/flow", float64(b)/n)
	runtime.KeepAlive(keep)
}

// ---------- new-flow allocation (10k-100k/s target) ----------

func BenchmarkAllocCurrent(b *testing.B) {
	ts := benchTuples(65536)
	b.ReportAllocs()
	var keep *Data
	for i := range b.N {
		keep = NewData(ts[i&0xffff], localEd1, remoteEd1)
	}
	runtime.KeepAlive(keep)
}

func BenchmarkAllocV2(b *testing.B) {
	ts := benchTuples(65536)
	b.ReportAllocs()
	var keep *dataV2
	for i := range b.N {
		keep = newDataV2(ts[i&0xffff])
	}
	runtime.KeepAlive(keep)
}

func BenchmarkAllocV2WithTrace(b *testing.B) {
	ts := benchTuples(65536)
	b.ReportAllocs()
	var keep *dataV2
	for i := range b.N {
		keep = newDataV2(ts[i&0xffff])
		keep.traces[0] = &ruleTraceV2{verdictIdx: -1, path: make([]*calc.RuleID, 3)}
	}
	runtime.KeepAlive(keep)
}
