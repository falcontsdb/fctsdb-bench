package keydriver

import (
	"fmt"
	"testing"
)

func Outer(args map[string]interface{}) {
	fmt.Println(args)
}

func TestKeyWord(t *testing.T) {
	driver := NewKeyDriver()
	driver.AddFunction("outer", Outer)
	driver.Call("outer", map[string]interface{}{
		"aa": "bb",
	})
}
