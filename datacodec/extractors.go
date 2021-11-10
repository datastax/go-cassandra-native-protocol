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
	"errors"
	"reflect"
	"strings"
)

// A utility to extract elements from container types like slices, arrays, structs and maps, using a unified API.
type extractor interface {

	// getElem returns the element at the given key.
	getElem(index int, key interface{}) (interface{}, error)
}

// A utility to extract keys from maps.
type keyValueExtractor interface {
	extractor

	// getKey returns the key at the given index.
	getKey(index int) interface{}
}

type sliceExtractor struct {
	source reflect.Value
}

type mapExtractor struct {
	source reflect.Value
	keys   []reflect.Value // to keep iteration order constant
}

type structExtractor struct {
	source reflect.Value
}

func newSliceExtractor(source reflect.Value) (extractor, error) {
	if source.Kind() != reflect.Slice && source.Kind() != reflect.Array {
		return nil, errors.New("expected slice or array, got: " + source.Type().String())
	} else if source.Kind() == reflect.Slice && source.IsNil() {
		return nil, errors.New("slice is nil")
	}
	return &sliceExtractor{source}, nil
}

func newStructExtractor(source reflect.Value) (keyValueExtractor, error) {
	if source.Kind() != reflect.Struct {
		return nil, errors.New("expected struct, got: " + source.Type().String())
	}
	return &structExtractor{source}, nil
}

func newMapExtractor(source reflect.Value) (keyValueExtractor, error) {
	if source.Kind() != reflect.Map {
		return nil, errors.New("expected map, got: " + source.Type().String())
	} else if source.IsNil() {
		return nil, errors.New("map is nil")
	}
	return &mapExtractor{source, source.MapKeys()}, nil
}

func (e *sliceExtractor) getElem(index int, _ interface{}) (interface{}, error) {
	if index < 0 || index >= e.source.Len() {
		return nil, errSliceIndexOutOfRange(e.source.Type().Kind() == reflect.Slice, index)
	}
	return e.source.Index(index).Interface(), nil
}

func (e *structExtractor) getKey(index int) interface{} {
	fieldName := e.source.Type().Field(index).Name
	field, _ := e.source.Type().FieldByName(fieldName)
	tag := field.Tag.Get("cassandra")
	if tag != "" {
		return tag
	}
	return strings.ToLower(fieldName)
}

func (e *structExtractor) getElem(_ int, key interface{}) (interface{}, error) {
	var field reflect.Value
	if name, ok := key.(string); ok {
		field = locateFieldByName(e.source, name)
	} else if index, ok := key.(int); ok {
		field = locateFieldByIndex(e.source, index)
	}
	if !field.IsValid() || !field.CanInterface() {
		return nil, errStructFieldInvalid(e.source, key)
	}
	return field.Interface(), nil
}

func (e *mapExtractor) getKey(index int) interface{} {
	return e.keys[index].Interface()
}

func (e *mapExtractor) getElem(_ int, key interface{}) (interface{}, error) {
	keyType := e.source.Type().Key()
	keyValue := reflect.ValueOf(key)
	if key == nil {
		keyValue = reflect.Zero(keyType)
	}
	if !keyValue.Type().AssignableTo(keyType) {
		return nil, errWrongElementType("map key", keyType, keyValue.Type())
	}
	value := e.source.MapIndex(keyValue)
	if !value.IsValid() || !value.CanInterface() {
		return nil, nil // key not found
	}
	return value.Interface(), nil
}
