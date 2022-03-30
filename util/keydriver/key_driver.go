package keydriver

import (
	"errors"
	"reflect"
)

type KeyDriver struct {
	actions map[string]interface{}
}

func NewKeyDriver() *KeyDriver {
	return &KeyDriver{actions: make(map[string]interface{})}
}

func (d *KeyDriver) AddFunction(name string, function func(args map[string]interface{})) {

	// fmt.Println("name", runtime.FuncForPC(reflect.ValueOf(function).Pointer()).Name())
	d.actions[name] = function
}

func (d *KeyDriver) Call(name string, params ...interface{}) (result []reflect.Value, err error) {
	f := reflect.ValueOf(d.actions[name])
	if len(params) != f.Type().NumIn() {
		err = errors.New("the number of params is not adapted")
		return
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result = f.Call(in)
	return
}
