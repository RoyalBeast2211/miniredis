package protocol

import (
	"errors"
	"fmt"
)

type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

type Value struct {
	Type   Type
	Str    string
	Num    int64
	Array  []Value
	IsNull bool
}

func NewSimpleString(s string) Value {
	return Value{Type: TypeSimpleString, Str: s}
}

func NewError(msg string) Value {
	return Value{Type: TypeError, Str: msg}
}

func NewInteger(n int64) Value {
	return Value{Type: TypeInteger, Num: n}
}

func NewBulkString(s string) Value {
	return Value{Type: TypeBulkString, Str: s}
}

func NewNullBulkString() Value {
	return Value{Type: TypeBulkString, IsNull: true}
}

func NewArray(elems []Value) Value {
	return Value{Type: TypeArray, Array: elems}
}

func (v Value) ToCommand() ([]string, error) {
	if v.Type != TypeArray {
		return nil, errors.New("command must be a RESP array")
	}
	args := make([]string, len(v.Array))
	for i, elem := range v.Array {
		if elem.Type != TypeBulkString {
			return nil, fmt.Errorf("command argument at index %d must be a bulk string", i)
		}
		args[i] = elem.Str
	}
	return args, nil
}
