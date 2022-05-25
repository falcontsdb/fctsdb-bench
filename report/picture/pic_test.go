package picture

import (
	"fmt"
	"testing"
)

func TestGenerateUniqueID(t *testing.T) {
	fmt.Println(generateUniqueID())
}

func BenchmarkGenerateUniqueID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		generateUniqueID()
	}
}
