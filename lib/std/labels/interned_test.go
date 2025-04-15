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

package labels

import (
	"maps"
	"testing"
)

func TestInternCache(t *testing.T) {
	m1 := map[string]string{
		"key1": "value1",
	}
	in := MakeInterned(m1)
	m2 := map[string]string{
		"key1": "value1",
	}
	in2 := MakeInterned(m2)

	if in.m != in2.m {
		t.Errorf("Expected the same interned map, got different ones")
	}

	if !maps.Equal(m1, in.RecomputeOriginalMap()) {
		t.Errorf("Expected the inflated map to be equal to the original map")
	}

	m3 := map[string]string{
		"key1": "value2",
	}
	in3 := MakeInterned(m3)

	if in.m == in3.m {
		t.Errorf("Expected different interned maps, got the same one")
	}
}
