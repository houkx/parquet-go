package parquet

import (
	"encoding/binary"
	"io"
	"math"
	"sync"
)

var bytesMap = make(map[int]sync.Pool)

func WriteI32(w io.Writer, order binary.ByteOrder, data int32) error {
	n := 4
	p, ok := bytesMap[n]
	if !ok {
		p = sync.Pool{
			New: func() interface{} {
				return make([]byte, n)
			},
		}
		bytesMap[n] = p
	}
	bs := p.Get().([]byte)
	order.PutUint32(bs, uint32(data))
	_, err := w.Write(bs)
	p.Put(bs)
	return err
}
func WriteI32s(w io.Writer, order binary.ByteOrder, data []int32) error {
	n := 4 * len(data)
	p, ok := bytesMap[n]
	if !ok {
		p = sync.Pool{
			New: func() interface{} {
				return make([]byte, n)
			},
		}
		bytesMap[n] = p
	}
	bs := p.Get().([]byte)
	for i, x := range data {
		order.PutUint32(bs[4*i:], uint32(x))
	}
	_, err := w.Write(bs)
	p.Put(bs)
	return err
}
func WriteI64s(w io.Writer, order binary.ByteOrder, data []int64) error {
	n := 8 * len(data)
	p, ok := bytesMap[n]
	if !ok {
		p = sync.Pool{
			New: func() interface{} {
				return make([]byte, n)
			},
		}
		bytesMap[n] = p
	}
	bs := p.Get().([]byte)
	for i, x := range data {
		order.PutUint64(bs[8*i:], uint64(x))
	}
	_, err := w.Write(bs)
	p.Put(bs)
	return err
}
func WriteF32s(w io.Writer, order binary.ByteOrder, data []float32) error {
	n := 4 * len(data)
	p, ok := bytesMap[n]
	if !ok {
		p = sync.Pool{
			New: func() interface{} {
				return make([]byte, n)
			},
		}
		bytesMap[n] = p
	}
	bs := p.Get().([]byte)
	for i, x := range data {
		order.PutUint32(bs[4*i:], math.Float32bits(x))
	}
	_, err := w.Write(bs)
	p.Put(bs)
	return err
}
func WriteF64s(w io.Writer, order binary.ByteOrder, data []float64) error {
	n := 8 * len(data)
	p, ok := bytesMap[n]
	if !ok {
		p = sync.Pool{
			New: func() interface{} {
				return make([]byte, n)
			},
		}
		bytesMap[n] = p
	}
	bs := p.Get().([]byte)
	for i, x := range data {
		order.PutUint64(bs[8*i:], math.Float64bits(x))
	}
	_, err := w.Write(bs)
	p.Put(bs)
	return err
}
/*func Write(w io.Writer, order binary.ByteOrder, data interface{}) error {
	n := intDataSize(data)
	bs := make([]byte, n)
	switch v := data.(type) {
	case *bool:
		if *v {
			bs[0] = 1
		} else {
			bs[0] = 0
		}
	case bool:
		if v {
			bs[0] = 1
		} else {
			bs[0] = 0
		}
	case []bool:
		for i, x := range v {
			if x {
				bs[i] = 1
			} else {
				bs[i] = 0
			}
		}
	case *int8:
		bs[0] = byte(*v)
	case int8:
		bs[0] = byte(v)
	case []int8:
		for i, x := range v {
			bs[i] = byte(x)
		}
	case *uint8:
		bs[0] = *v
	case uint8:
		bs[0] = v
	case []uint8:
		bs = v // TODO(josharian): avoid allocating bs in this case?
	case *int16:
		order.PutUint16(bs, uint16(*v))
	case int16:
		order.PutUint16(bs, uint16(v))
	case []int16:
		for i, x := range v {
			order.PutUint16(bs[2*i:], uint16(x))
		}
	case *uint16:
		order.PutUint16(bs, *v)
	case uint16:
		order.PutUint16(bs, v)
	case []uint16:
		for i, x := range v {
			order.PutUint16(bs[2*i:], x)
		}
	case *int32:
		order.PutUint32(bs, uint32(*v))
	case int32:
		order.PutUint32(bs, uint32(v))
	case []int32:
		for i, x := range v {
			order.PutUint32(bs[4*i:], uint32(x))
		}
	case *uint32:
		order.PutUint32(bs, *v)
	case uint32:
		order.PutUint32(bs, v)
	case []uint32:
		for i, x := range v {
			order.PutUint32(bs[4*i:], x)
		}
	case *int64:
		order.PutUint64(bs, uint64(*v))
	case int64:
		order.PutUint64(bs, uint64(v))
	case []int64:
		for i, x := range v {
			order.PutUint64(bs[8*i:], uint64(x))
		}
	case *uint64:
		order.PutUint64(bs, *v)
	case uint64:
		order.PutUint64(bs, v)
	case []uint64:
		for i, x := range v {
			order.PutUint64(bs[8*i:], x)
		}
	case *float32:
		order.PutUint32(bs, math.Float32bits(*v))
	case float32:
		order.PutUint32(bs, math.Float32bits(v))
	case []float32:
		for i, x := range v {
			order.PutUint32(bs[4*i:], math.Float32bits(x))
		}
	case *float64:
		order.PutUint64(bs, math.Float64bits(*v))
	case float64:
		order.PutUint64(bs, math.Float64bits(v))
	case []float64:
		for i, x := range v {
			order.PutUint64(bs[8*i:], math.Float64bits(x))
		}
	}
	_, err := w.Write(bs)
	return err
}

// intDataSize returns the size of the data required to represent the data when encoded.
// It returns zero if the type cannot be implemented by the fast path in Read or Write.
func intDataSize(data interface{}) int {
	switch data := data.(type) {
	case bool, int8, uint8, *bool, *int8, *uint8:
		return 1
	case []bool:
		return len(data)
	case []int8:
		return len(data)
	case []uint8:
		return len(data)
	case int16, uint16, *int16, *uint16:
		return 2
	case []int16:
		return 2 * len(data)
	case []uint16:
		return 2 * len(data)
	case int32, uint32, *int32, *uint32:
		return 4
	case []int32:
		return 4 * len(data)
	case []uint32:
		return 4 * len(data)
	case int64, uint64, *int64, *uint64:
		return 8
	case []int64:
		return 8 * len(data)
	case []uint64:
		return 8 * len(data)
	case float32, *float32:
		return 4
	case float64, *float64:
		return 8
	case []float32:
		return 4 * len(data)
	case []float64:
		return 8 * len(data)
	}
	return 0
}
*/