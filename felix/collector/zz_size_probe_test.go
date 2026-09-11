package collector

import (
	"fmt"
	"reflect"
	"testing"

	"k8s.io/kubernetes/pkg/proxy"

	"github.com/projectcalico/calico/felix/collector/types/tuple"
)

func TestZZSizes(t *testing.T) {
	for _, v := range []any{Data{}, RuleTrace{}, tuple.Tuple{}, proxy.ServicePortName{}} {
		ty := reflect.TypeOf(v)
		fmt.Printf("\n=== %s size=%d align=%d\n", ty.Name(), ty.Size(), ty.Align())
		for i := 0; i < ty.NumField(); i++ {
			f := ty.Field(i)
			fmt.Printf("  %-28s off=%4d size=%4d  cl=%d-%d  %s\n", f.Name, f.Offset, f.Type.Size(), f.Offset/64, (f.Offset+f.Type.Size()-1)/64, f.Type)
		}
	}
}
