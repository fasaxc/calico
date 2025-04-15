// Copyright (c) 2025 Tigera, Inc. All rights reserved.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internedlabels

import (
	"encoding/json"
	"iter"
	"maps"
	"sync"
	"unique"

	"github.com/projectcalico/calico/lib/std/interncache"
)

type stringHandle unique.Handle[string]

//goland:noinspection GoMixedReceiverTypes
func (s stringHandle) MarshalJSON() ([]byte, error) {
	return json.Marshal((unique.Handle[string])(s).Value())
}

//goland:noinspection GoMixedReceiverTypes
func (s *stringHandle) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	*s = stringHandle(unique.Make(str))
	return nil
}

//goland:noinspection GoMixedReceiverTypes
func (s stringHandle) Value() string {
	return unique.Handle[string](s).Value()
}

type handleMap = map[stringHandle]stringHandle

var (
	cacheLock sync.Mutex
	cache     = interncache.New[handleMap](
		interncache.MapHasher[stringHandle, stringHandle](),
		func(m *handleMap, m2 *handleMap) bool {
			return maps.Equal(*m, *m2)
		},
	)
)

type InternedLabels struct {
	// m is a pointer to an interned map of string handles.  The intern cache
	// relies on this being a pointer in order to keep its WeakPointer alive.
	m *handleMap
}

func Make(m map[string]string) InternedLabels {
	var hm handleMap
	if m == nil {
		return InternedLabels{}
	}
	hm = make(handleMap, len(m))
	for k, v := range m {
		hm[stringHandle(unique.Make(k))] = stringHandle(unique.Make(v))
	}

	cacheLock.Lock()
	interned := cache.Intern(&hm)
	cacheLock.Unlock()
	return InternedLabels{m: interned}
}

// MarshalJSON implements the json.Marshaler interface. Must be defined on the
// value receiver so that InternedLabels can be embedded in other structs.
//
//goland:noinspection GoMixedReceiverTypes
func (i InternedLabels) MarshalJSON() ([]byte, error) {
	if i.m == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(*i.m)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
//
//goland:noinspection GoMixedReceiverTypes
func (i *InternedLabels) UnmarshalJSON(data []byte) error {
	var temp handleMap
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	cacheLock.Lock()
	i.m = cache.Intern(&temp)
	cacheLock.Unlock()
	return nil
}

func (i *InternedLabels) AllHandles() iter.Seq2[unique.Handle[string], unique.Handle[string]] {
	return func(yield func(unique.Handle[string], unique.Handle[string]) bool) {
		if i.m == nil {
			return
		}
		for k, v := range *i.m {
			yield(unique.Handle[string](k), unique.Handle[string](v))
		}
	}
}

//goland:noinspection GoMixedReceiverTypes
func (i *InternedLabels) AllStrings() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		if i.m == nil {
			return
		}
		for k, v := range *i.m {
			yield(k.Value(), v.Value())
		}
	}
}

//goland:noinspection GoMixedReceiverTypes
func (i InternedLabels) RecomputeOriginalMap() map[string]string {
	if i.m == nil {
		return nil
	}
	m := make(map[string]string, len(*i.m))
	for k, v := range *i.m {
		m[k.Value()] = v.Value()
	}
	return m
}

//goland:noinspection GoMixedReceiverTypes
func (i *InternedLabels) GetString(k string) (string, bool) {
	if i.m == nil {
		return "", false
	}
	v, ok := (*i.m)[stringHandle(unique.Make(k))]
	if !ok {
		return "", false
	}
	return v.Value(), true
}

//goland:noinspection GoMixedReceiverTypes
func (i *InternedLabels) GetHandle(h unique.Handle[string]) (unique.Handle[string], bool) {
	if i.m == nil {
		return unique.Handle[string]{}, false
	}
	v, ok := (*i.m)[stringHandle(h)]
	if !ok {
		return unique.Handle[string]{}, false
	}
	return unique.Handle[string](v), true
}

//goland:noinspection GoMixedReceiverTypes
func (i *InternedLabels) Len() int {
	if i.m == nil {
		return 0
	}
	return len(*i.m)
}
