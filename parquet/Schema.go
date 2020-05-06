package parquet

import (
	"encoding/binary"
	"fmt"
	sh "github.com/houkx/parquet-go/parquet/schema"
	"github.com/json-iterator/go"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type Schema struct {
	Fields           []SchemaField
	PFields          []Field
	CompressionCodec sh.CompressionCodec
	jsonMapPool      sync.Pool
}
type SchemaField struct {
	name         string
	fieldType    sh.Type
	defaultValue interface{}
	RequiredField
	makeValues func(max int) *Values
	append     func(values *Values, record *map[string]interface{})
	write       func(w io.Writer, meta *Metadata, values *Values) error
	reset       func(values *Values)
	intSizePool sync.Pool
}
type Values struct {
	strs []string
	i32s []int32
	f32s []float32
	f64s []float64
	i64s []int64
	boos []bool
}

func (p *Schema) GetJsonMap() *map[string]interface{} {
	return p.jsonMapPool.Get().(*map[string]interface{})
}
func (p *Schema) ReturnJsonMap(m *map[string]interface{}) {
	mp := *m
	for _, f := range p.Fields {
		mp[f.name] = nil
	}
	p.jsonMapPool.Put(m)
}
func NewSchema(avroSchema string, compression sh.CompressionCodec) (schema *Schema, err error) {
	return getSchemaFromAvroSchema(avroSchema, compression)
}

func getSchemaFromAvroSchema(avroSchema string, compression sh.CompressionCodec) (sc *Schema, err error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	var fieldsAny = json.Get([]byte(avroSchema), "fields")
	if fieldsAny.LastError() != nil {
		return nil, fieldsAny.LastError()
	}

	var fieldsO = fieldsAny.GetInterface()
	if fields, ok := fieldsO.([]interface{}); ok {
		fs := make([]SchemaField, 0, len(fields))
		pfs := make([]Field, 0, len(fields))
		for _, m := range fields {
			if m, ok := m.(map[string]interface{}); ok {
				var fieldTypeStr = m["type"].(string)
				t, e := avroTypeToParquetType(strings.ToLower(fieldTypeStr))
				if e == nil {
					var fieldName = m["name"].(string)
					var defV = m["default"]
					if defV != nil {
						defV = convertDataByType(t, defV, defVal(t))
					} else {
						defV = defVal(t)
					}
					f := SchemaField{
						name:          fieldName,
						fieldType:     t,
						defaultValue:  defV,
						RequiredField: RequiredField{Paths: []string{fieldName}, Codec: compression},
					}
					pf := Field{
						Name:           f.Name(),
						Path:           f.Path(),
						RepetitionType: RepetitionRequired,
						Types:          []int{0},
					}
					switch t {
					case sh.Type_BYTE_ARRAY:
						pf.Type = StringType
						f.reset = func(values *Values) {
							values.strs = values.strs[:0]
						}
						f.makeValues = func(max int) *Values {
							return &Values{strs: make([]string, 0, max)}
						}
						f.append = func(values *Values, record *map[string]interface{}) {
							m := *record
							fv, _ := m[f.name]
							val := convertDataByType(f.fieldType, fv, f.defaultValue)
							values.strs = append(values.strs, val.(string))
						}
						f.intSizePool = sync.Pool{
							New: func() interface{} { return make([]byte, 4) },
						}
						f.write = func(w io.Writer, meta *Metadata, values *Values) error {
							var buf = GetBuffer()
							sizeBuf := f.intSizePool.Get().([]byte)
							defer func() { PutBuffer(buf); f.intSizePool.Put(sizeBuf); f.reset(values) }()
							size := len(values.strs)
							order := binary.LittleEndian
							for _, str := range values.strs {
								order.PutUint32(sizeBuf, uint32(len(str)))
								buf.Write(sizeBuf)
								buf.WriteString(str)
							}
							return f.DoWrite(w, meta, buf.Bytes(), size, nil)
						}
					case sh.Type_INT32:
						pf.Type = Int32Type
						f.reset = func(values *Values) {
							values.i32s = values.i32s[:0]
						}
						f.makeValues = func(max int) *Values {
							return &Values{i32s: make([]int32, 0, max)}
						}
						f.append = func(values *Values, record *map[string]interface{}) {
							m := *record
							fv, _ := m[f.name]
							val := convertDataByType(f.fieldType, fv, f.defaultValue)
							values.i32s = append(values.i32s, val.(int32))
						}
						f.write = func(w io.Writer, meta *Metadata, values *Values) error {
							var buf = GetBuffer()
							defer func() { PutBuffer(buf); f.reset(values) }()
							size := len(values.i32s)
							WriteI32s(buf, binary.LittleEndian, values.i32s)
							return f.DoWrite(w, meta, buf.Bytes(), size, nil)
						}
					case sh.Type_FLOAT:
						pf.Type = Float32Type
						f.reset = func(values *Values) {
							values.f32s = values.f32s[:0]
						}
						f.makeValues = func(max int) *Values {
							return &Values{f32s: make([]float32, 0, max)}
						}
						f.append = func(values *Values, record *map[string]interface{}) {
							m := *record
							fv, _ := m[f.name]
							val := convertDataByType(f.fieldType, fv, f.defaultValue)
							values.f32s = append(values.f32s, val.(float32))
						}
						f.write = func(w io.Writer, meta *Metadata, values *Values) error {
							var buf = GetBuffer()
							defer func() { PutBuffer(buf); f.reset(values) }()
							size := len(values.f32s)
							WriteF32s(buf, binary.LittleEndian, values.f32s)
							return f.DoWrite(w, meta, buf.Bytes(), size, nil)
						}
					case sh.Type_DOUBLE:
						pf.Type = Float64Type
						f.reset = func(values *Values) {
							values.f64s = values.f64s[:0]
						}
						f.makeValues = func(max int) *Values {
							return &Values{f64s: make([]float64, 0, max)}
						}
						f.append = func(values *Values, record *map[string]interface{}) {
							m := *record
							fv, _ := m[f.name]
							val := convertDataByType(f.fieldType, fv, f.defaultValue)
							values.f64s = append(values.f64s, val.(float64))
						}
						f.write = func(w io.Writer, meta *Metadata, values *Values) error {
							var buf = GetBuffer()
							defer func() { PutBuffer(buf); f.reset(values) }()
							size := len(values.f64s)
							WriteF64s(buf, binary.LittleEndian, values.f64s)
							return f.DoWrite(w, meta, buf.Bytes(), size, nil)
						}
					case sh.Type_INT64:
						pf.Type = Int64Type
						f.reset = func(values *Values) {
							values.i64s = values.i64s[:0]
						}
						f.makeValues = func(max int) *Values {
							return &Values{i64s: make([]int64, 0, max)}
						}
						f.append = func(values *Values, record *map[string]interface{}) {
							m := *record
							fv, _ := m[f.name]
							val := convertDataByType(f.fieldType, fv, f.defaultValue)
							values.i64s = append(values.i64s, val.(int64))
						}
						f.write = func(w io.Writer, meta *Metadata, values *Values) error {
							var buf = GetBuffer()
							defer func() { PutBuffer(buf); f.reset(values) }()
							size := len(values.i64s)
							WriteI64s(buf, binary.LittleEndian, values.i64s)
							return f.DoWrite(w, meta, buf.Bytes(), size, nil)
						}
					case sh.Type_BOOLEAN:
						pf.Type = BoolType
						f.reset = func(values *Values) {
							values.boos = values.boos[:0]
						}
						f.makeValues = func(max int) *Values {
							return &Values{boos: make([]bool, 0, max)}
						}
						f.append = func(values *Values, record *map[string]interface{}) {
							m := *record
							fv, _ := m[f.name]
							val := convertDataByType(f.fieldType, fv, f.defaultValue)
							values.boos = append(values.boos, val.(bool))
						}
						f.write = func(w io.Writer, meta *Metadata, values *Values) error {
							ln := len(values.boos)
							n := (ln + 7) / 8
							rawBuf := make([]byte, n)

							for i := 0; i < ln; i++ {
								if values.boos[i] {
									rawBuf[i/8] = rawBuf[i/8] | (1 << uint32(i%8))
								}
							}
							f.reset(values)
							return f.DoWrite(w, meta, rawBuf, ln, nil)
						}
					}
					fs = append(fs, f)
					pfs = append(pfs, pf)
				}
			}
		}
		sc = &Schema{Fields: fs, PFields: pfs, CompressionCodec: compression,
			jsonMapPool: sync.Pool{
				New: func() interface{} {
					return new(map[string]interface{})
				},
			},
		}
	}
	return sc, err
}
func avroTypeToParquetType(avroType string) (t sh.Type, err error) {
	switch avroType {
	case "string":
		return sh.Type_BYTE_ARRAY, nil
	case "int":
		return sh.Type_INT32, nil
	case "float":
		return sh.Type_FLOAT, nil
	case "double":
		return sh.Type_DOUBLE, nil
	case "long":
		return sh.Type_INT64, nil
	case "boolean":
		return sh.Type_BOOLEAN, nil
	}
	return sh.Type(0), fmt.Errorf("not a valid Type string")
}
func convertDataByType(dataType sh.Type, val, defV interface{}) interface{} {
	var vt = reflect.Invalid
	if val != nil {
		vt = reflect.TypeOf(val).Kind()
	}
	//
	switch dataType {
	case sh.Type_BYTE_ARRAY:
		if vt == reflect.Invalid {
			val = defV
		} else if vt != reflect.String {
			val = fmt.Sprint(val)
		} else if val == nil {
			val = ""
		}
		val = val.(string)
	case sh.Type_INT32:
		if vt == reflect.Float64 {
			val = int32(val.(float64))
		} else if vt == reflect.String {
			num, e := strconv.Atoi(val.(string))
			if e != nil {
				val = defV
			} else {
				val = int32(num)
			}
		} else {
			val = defV
		}
	case sh.Type_FLOAT:
		if vt == reflect.Float64 {
			val = float32(val.(float64))
		} else if vt == reflect.String {
			num, e := strconv.ParseFloat(val.(string), 32)
			if e != nil {
				val = defV
			} else {
				val = float32(num)
			}
		} else {
			val = defV
		}
	case sh.Type_DOUBLE:
		if vt == reflect.Float64 {
			val = val.(float64)
		} else if vt == reflect.String {
			num, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				val = defV
			} else {
				val = num
			}
		} else {
			val = defV
		}
	case sh.Type_INT64:
		if vt == reflect.Int64 {
			val = val.(int64)
		} else if vt == reflect.Float64 {
			val = int64(val.(float64))
		} else if vt == reflect.String {
			num, e := strconv.ParseInt(val.(string), 10, 64)
			if e != nil {
				val = defV
			} else {
				val = num
			}
		} else {
			val = defV
		}
	case sh.Type_BOOLEAN:
		if vt == reflect.Invalid {
			val = defV
		} else {
			var e error
			val, e = strconv.ParseBool(fmt.Sprint(val))
			if e != nil {
				val = defV
			}
		}
	}
	return val
}
func defVal(fieldType sh.Type) interface{} {
	switch fieldType {
	case sh.Type_BYTE_ARRAY:
		return ""
	case sh.Type_INT32:
		return int32(DefaultNumber)
	case sh.Type_INT64:
		return int64(DefaultNumber)
	case sh.Type_FLOAT:
		return float32(DefaultNumber)
	case sh.Type_DOUBLE:
		return float64(DefaultNumber)
	case sh.Type_BOOLEAN:
		return false
	}
	return nil
}

var DefaultNumber int

func init() {
	var numStr = "-1"
	if v, exists := os.LookupEnv("schema.def.num"); exists {
		numStr = v
	}
	defNum, e := strconv.Atoi(numStr)
	if e != nil {
		defNum = -1
	}
	DefaultNumber = defNum
}
