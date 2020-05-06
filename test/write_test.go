package test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	park "github.com/houkx/parquet-go/parquet"
	"github.com/houkx/parquet-go/parquet/schema"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestBinaryWrite(t *testing.T) {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, []int32{1, 2, 3})
}

type Values struct {
	strs []string
	i32s []int32
	f32s []float32
	f64s []float64
	i64s []int64
	boos []bool
}

func Test_values(t *testing.T) {
	vs := &Values{}
	t.Log("", vs.strs == nil)
}
func Test_schema(t *testing.T) {
	sc, e := park.NewSchema(avroSchema, schema.CompressionCodec_SNAPPY)
	if e != nil {
		t.Fatal(e)
	}
	if sc.PFields == nil || len(sc.PFields) != 5 {
		t.Fatal("元数据字段解析出错!")
	}
	if sc.Fields == nil || len(sc.Fields) != 5 {
		t.Fatal("元数据字段解析出错!")
	}
	t.Log("Pass!")
}
func Test_writeParquetFile(t *testing.T) {
	sc, e := park.NewSchema(avroSchema, schema.CompressionCodec_GZIP)
	if e != nil {
		t.Fatal(e)
	}
	file, err := os.Create("./p20200429." + sc.CompressionCodec.String() + ".parquet")
	if err != nil {
		t.Fatal(e)
	}
	pw := park.NewParquetWriter(sc, file, 11)
	var format = `{"uid":"%s", "did":"%s", "type":%d, "code":%d,"time":%d}`
	for i := 0; i < 50; i++ {
		s := fmt.Sprintf(format, "us-"+strconv.Itoa(i),
			"c3p"+strconv.Itoa(i), i%8, (i+1)*4+100, time.Now().Unix())
		//var data = make(map[string]interface{})
		//jsoniter.Unmarshal([]byte(s), &data)
		//pw.Write(&data)
		pw.WriteJson([]byte(s))
	}
	pw.Close()
	file.Close()
	t.Log("Write Finished: ", pw.Rows())
}

var avroSchema = `
   {
  "name": "ali_hkx_test",
  "type": "record",
  "fields": [
    {
      "name": "uid",
      "type": "string"
    },
    {
      "name": "did",
      "type": "string"
    },
    {
      "name": "code",
      "type": "int",
       "default": "500"
    },
    {
      "name": "type",
      "type": "int",
       "default": -2
    }, 
    {
      "name": "time",
      "type": "long",
      "default": 0
    }
  ]
}
  `
