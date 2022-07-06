// Copyright 2020 DataStax
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
	"bytes"
	"fmt"
	"reflect"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func NewMap(dataType *datatype.Map) (Codec, error) {
	if dataType == nil {
		return nil, ErrNilDataType
	}
	keyCodec, err := NewCodec(dataType.KeyType)
	if err != nil {
		return nil, fmt.Errorf("cannot create codec for map keys: %w", err)
	}
	valueCodec, err := NewCodec(dataType.ValueType)
	if err != nil {
		return nil, fmt.Errorf("cannot create codec for map values: %w", err)
	}
	return &mapCodec{dataType, keyCodec, valueCodec}, nil
}

type mapCodec struct {
	dataType   *datatype.Map
	keyCodec   Codec
	valueCodec Codec
}

func (c *mapCodec) DataType() datatype.DataType {
	return c.dataType
}

func (c *mapCodec) Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error) {
	ext, size, err := c.createExtractor(source)
	if err == nil && ext != nil {
		dest, err = writeMap(ext, size, c.keyCodec, c.valueCodec, version)
	}
	if err != nil {
		err = errCannotEncode(source, c.DataType(), version, err)
	}
	return
}

func (c *mapCodec) Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error) {
	wasNull = len(source) == 0
	var injectorFactory func(int) (keyValueInjector, error)
	if injectorFactory, err = c.createInjector(dest, wasNull); err == nil && injectorFactory != nil {
		err = readMap(source, injectorFactory, c.keyCodec, c.valueCodec, version)
	}
	if err != nil {
		err = errCannotDecode(dest, c.DataType(), version, err)
	}
	return
}

func (c *mapCodec) createExtractor(source interface{}) (ext keyValueExtractor, size int, err error) {
	sourceValue, sourceType, wasNil := reflectSource(source)
	if sourceType != nil {
		switch sourceType.Kind() {
		case reflect.Map:
			if !wasNil {
				size = sourceValue.Len()
				ext, err = newMapExtractor(sourceValue)
			}
		case reflect.Struct:
			if !wasNil {
				if c.keyCodec.DataType() != datatype.Varchar && c.keyCodec.DataType() != datatype.Ascii {
					err = errWrongDataType("map key", datatype.Varchar, datatype.Ascii, c.keyCodec.DataType())
				} else {
					size = sourceValue.NumField()
					ext, err = newStructExtractor(sourceValue)
				}
			}
		default:
			err = ErrSourceTypeNotSupported
		}
	}
	return
}

func (c *mapCodec) createInjector(dest interface{}, wasNull bool) (injectorFactory func(int) (keyValueInjector, error), err error) {
	destValue, err := reflectDest(dest, wasNull)
	if err == nil {
		switch destValue.Kind() {
		case reflect.Map:
			if !wasNull {
				injectorFactory = func(size int) (keyValueInjector, error) {
					adjustMapSize(destValue, size)
					return newMapInjector(destValue)
				}
			}
		case reflect.Struct:
			if !wasNull {
				if c.keyCodec.DataType() != datatype.Varchar && c.keyCodec.DataType() != datatype.Ascii {
					err = errWrongDataType("map key", datatype.Varchar, datatype.Ascii, c.keyCodec.DataType())
				} else {
					injectorFactory = func(size int) (keyValueInjector, error) {
						return newStructInjector(destValue)
					}
				}
			}
		case reflect.Interface:
			if !wasNull {
				var targetType reflect.Type
				if targetType, err = PreferredGoType(c.DataType()); err == nil {
					injectorFactory = func(size int) (keyValueInjector, error) {
						destValue.Set(reflect.MakeMapWithSize(targetType, size))
						return newMapInjector(destValue.Elem())
					}
				}
			}
		default:
			err = ErrDestinationTypeNotSupported
		}
	}
	return
}

func writeMap(ext keyValueExtractor, size int, keyCodec Codec, valueCodec Codec, version primitive.ProtocolVersion) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := writeCollectionSize(size, buf, version); err != nil {
		return nil, err
	}
	for i := 0; i < size; i++ {
		key := ext.getKey(i)
		if value, err := ext.getElem(i, key); err != nil {
			return nil, errCannotExtractMapValue(i, err)
		} else if encodedKey, err := keyCodec.Encode(key, version); err != nil {
			return nil, errCannotEncodeMapKey(i, err)
		} else if encodedValue, err := valueCodec.Encode(value, version); err != nil {
			return nil, errCannotEncodeMapValue(i, err)
		} else {
			_ = primitive.WriteBytes(encodedKey, buf)
			_ = primitive.WriteBytes(encodedValue, buf)
		}
	}
	return buf.Bytes(), nil
}

func readMap(source []byte, injectorFactory func(int) (keyValueInjector, error), keyCodec Codec, valueCodec Codec, version primitive.ProtocolVersion) error {
	reader := bytes.NewReader(source)
	total := len(source)
	if size, err := readCollectionSize(reader, version); err != nil {
		return err
	} else if inj, err := injectorFactory(size); err != nil {
		return err
	} else {
		for i := 0; i < size; i++ {
			if encodedKey, err := primitive.ReadBytes(reader); err != nil {
				return errCannotReadMapKey(i, err)
			} else if encodedValue, err := primitive.ReadBytes(reader); err != nil {
				return errCannotReadMapValue(i, err)
			} else if decodedKey, err := inj.zeroKey(i); err != nil {
				return errCannotCreateMapKey(i, err)
			} else if keyWasNull, err := keyCodec.Decode(encodedKey, decodedKey, version); err != nil {
				return errCannotDecodeMapKey(i, err)
			} else if decodedValue, err := inj.zeroElem(i, decodedKey); err != nil {
				return errCannotCreateMapValue(i, err)
			} else if valueWasNull, err := valueCodec.Decode(encodedValue, decodedValue, version); err != nil {
				return errCannotDecodeMapValue(i, err)
			} else if err = inj.setElem(i, decodedKey, decodedValue, keyWasNull, valueWasNull); err != nil {
				return errCannotInjectMapEntry(i, err)
			}
		}
		if remaining := reader.Len(); remaining != 0 {
			return errBytesRemaining(total, remaining)
		}
	}
	return nil
}
