// Copyright 2021 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacodec

import (
	"reflect"
)

// A utility to inject elements in container types like slices, arrays, structs and maps, using a unified API.
type injector interface {

	// zeroElem returns a pointer to a zero value for the element at the given key.
	// Note that the returned value *must* be a pointer, even if the underlying element type is not, because the
	// returned value will be passed to Codec.Decode.
	zeroElem(index int, key interface{}) (value interface{}, err error)

	// setElem sets the element at the given key to the given decoded value.
	// Note that the decoded value will *always* be a pointer: if the underlying element type is not a pointer, the
	// value needs to be passed through reflect.Indirect before being set. keyWasNull and valueWasNull indicate that the
	// decoded key and value were CQL NULLs.
	setElem(index int, key, value interface{}, keyWasNull, valueWasNull bool) error
}

// A utility to inject keys and values in maps.
type keyValueInjector interface {
	injector

	// zeroKey returns a pointer to a zero value for the map key type.
	// Note that the returned value *must* be a pointer, even if the underlying key type is not, because the returned
	// value will be passed to Codec.Decode.
	zeroKey(index int) (value interface{}, err error)
}

type sliceInjector struct {
	dest reflect.Value
}

type structInjector struct {
	dest          reflect.Value
	fieldsByIndex map[int]reflect.Value
	fieldsByName  map[string]reflect.Value
}

type mapInjector struct {
	dest reflect.Value
}

func newSliceInjector(dest reflect.Value) (injector, error) {
	if !dest.IsValid() {
		return nil, ErrDestinationTypeNotSupported
	} else if dest.Kind() != reflect.Slice && dest.Kind() != reflect.Array {
		return nil, errWrongContainerType("slice or array", dest.Type())
	}
	return &sliceInjector{dest}, nil

}
func newStructInjector(dest reflect.Value) (keyValueInjector, error) {
	if !dest.IsValid() {
		return nil, ErrDestinationTypeNotSupported
	} else if dest.Kind() != reflect.Struct {
		return nil, errWrongContainerType("struct", dest.Type())
	} else if !dest.CanSet() {
		return nil, errDestinationUnaddressable(dest)
	}
	return &structInjector{dest: dest}, nil
}

func newMapInjector(dest reflect.Value) (keyValueInjector, error) {
	if !dest.IsValid() {
		return nil, ErrDestinationTypeNotSupported
	} else if dest.Kind() != reflect.Map {
		return nil, errWrongContainerType("map", dest.Type())
	}
	return &mapInjector{dest}, nil
}

func (i *sliceInjector) zeroElem(_ int, _ interface{}) (value interface{}, err error) {
	zero := ensurePointer(nilSafeZero(i.dest.Type().Elem()))
	return zero.Interface(), nil
}

func (i *sliceInjector) setElem(index int, _, value interface{}, _, valueWasNull bool) error {
	if index < 0 || index >= i.dest.Len() {
		return errSliceIndexOutOfRange(i.dest.Type().Kind() == reflect.Slice, index)
	}
	elementType := i.dest.Type().Elem()
	if valueWasNull {
		zero := reflect.Zero(elementType)
		i.dest.Index(index).Set(zero)
	} else {
		newValue := maybeIndirect(elementType, reflect.ValueOf(value))
		if !newValue.Type().AssignableTo(elementType) {
			if i.dest.Kind() == reflect.Slice {
				return errWrongElementType("slice element", elementType, newValue.Type())
			} else {
				return errWrongElementType("array element", elementType, newValue.Type())
			}
		}
		i.dest.Index(index).Set(newValue)
	}
	return nil
}

func (i *structInjector) zeroKey(_ int) (value interface{}, err error) {
	return new(string), nil
}

func (i *structInjector) zeroElem(_ int, key interface{}) (interface{}, error) {
	if field, err := i.locateAndStoreField(key); err != nil {
		return nil, err
	} else if field.IsValid() && field.CanSet() {
		zero := ensurePointer(nilSafeZero(field.Type()))
		return zero.Interface(), nil
	} else {
		return nil, errStructFieldInvalid(i.dest, key)
	}
}

func (i *structInjector) setElem(_ int, key, value interface{}, _, valueWasNull bool) error {
	field := i.retrieveStoredField(key)
	if field.IsValid() && field.CanSet() {
		fieldType := field.Type()
		if valueWasNull {
			zero := reflect.Zero(fieldType)
			field.Set(zero)
		} else {
			newValue := maybeIndirect(fieldType, reflect.ValueOf(value))
			if !newValue.Type().AssignableTo(fieldType) {
				return errWrongElementType("struct field value", fieldType, newValue.Type())
			}
			field.Set(newValue)
		}
		return nil
	} else {
		return errStructFieldInvalid(i.dest, key)
	}
}

func (i *structInjector) locateAndStoreField(key interface{}) (field reflect.Value, err error) {
	switch k := key.(type) {
	case string:
		field = locateFieldByName(i.dest, k)
		if i.fieldsByName == nil {
			i.fieldsByName = map[string]reflect.Value{}
		}
		i.fieldsByName[k] = field
	case *string:
		return i.locateAndStoreField(*k)
	case int:
		field = locateFieldByIndex(i.dest, k)
		if i.fieldsByIndex == nil {
			i.fieldsByIndex = map[int]reflect.Value{}
		}
		i.fieldsByIndex[k] = field
	default:
		err = errWrongElementTypes("struct field key", typeOfInt, typeOfString, reflect.TypeOf(key))
	}
	return
}

func (i *structInjector) retrieveStoredField(key interface{}) reflect.Value {
	var field reflect.Value
	switch k := key.(type) {
	case string:
		field = i.fieldsByName[k]
	case *string:
		field = i.fieldsByName[*k]
	case int:
		field = i.fieldsByIndex[k]
	}
	return field
}

func (i *mapInjector) zeroKey(_ int) (interface{}, error) {
	zero := ensurePointer(nilSafeZero(i.dest.Type().Key()))
	return zero.Interface(), nil
}

func (i *mapInjector) zeroElem(_ int, _ interface{}) (interface{}, error) {
	zero := ensurePointer(nilSafeZero(i.dest.Type().Elem()))
	return zero.Interface(), nil
}

func (i *mapInjector) setElem(_ int, key, value interface{}, keyWasNull, valueWasNull bool) error {
	keyType := i.dest.Type().Key()
	var newKey reflect.Value
	if keyWasNull {
		newKey = reflect.Zero(keyType)
	} else {
		newKey = maybeIndirect(keyType, reflect.ValueOf(key))
		if !newKey.Type().AssignableTo(keyType) {
			return errWrongElementType("map key", keyType, newKey.Type())
		}
	}
	valueType := i.dest.Type().Elem()
	var newValue reflect.Value
	if valueWasNull {
		newValue = reflect.Zero(valueType)
	} else {
		newValue = maybeIndirect(valueType, reflect.ValueOf(value))
		if !newValue.Type().AssignableTo(valueType) {
			return errWrongElementType("map value", valueType, newValue.Type())
		}
	}
	i.dest.SetMapIndex(newKey, newValue)
	return nil
}
