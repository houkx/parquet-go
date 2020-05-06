package test

import (
	"fmt"
	park "github.com/houkx/parquet-go/parquet"
	"github.com/houkx/parquet-go/parquet/schema"
	"github.com/json-iterator/go"
	"os"
	"strconv"
	"testing"
	"time"
)

func Benchmark_parquetWrite(b *testing.B) {
	//Benchmark_parquetWrite-4   	  374072	      3625 ns/op	     726 B/op	      47 allocs/op
	//Benchmark_parquetWrite-4   	  382755	      3201 ns/op	     726 B/op	      47 allocs/op
	// 反射优化后:
	//Benchmark_parquetWrite-4   	  391532	      3373 ns/op	     711 B/op	      41 allocs/op
	// gzip:Benchmark_parquetWrite-4  312222	      3904 ns/op	     771 B/op	      41 allocs/op
	// Benchmark_parquetWrite-4   	  363636	      3658 ns/op	     830 B/op	      41 allocs/op
	// Benchmark_parquetWrite-4   	  324324	      3364 ns/op	     798 B/op	      41 allocs/op
	// 加buff_pool 优化后:
	// Benchmark_parquetWrite-4   	  342856	      3208 ns/op	     736 B/op	      37 allocs/op
	// Benchmark_parquetWrite-4   	  399999	      3363 ns/op	     797 B/op	      37 allocs/op
	b.ReportAllocs()
	b.ResetTimer()
	sc, e := park.NewSchema(avroSchema, schema.CompressionCodec_GZIP)//CompressionCodec_SNAPPY,CompressionCodec_GZIP
	if e != nil {
		b.Fatal(e)
	}
	file, err := os.Create("./p20200412.parquet")
	if err != nil {
		b.Fatal(e)
	}
	pw := park.NewParquetWriter(sc, file, 1000)
	var format = `{"uid":"%s", "did":"%s", "type":%d, "code":%d,"time":%d}`
	//var data = make(map[string]interface{})
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf(format, "us-"+strconv.Itoa(i),
			"c3p"+strconv.Itoa(i), i%8, (i+1)*4+100, time.Now().Unix())
		//jsoniter.Unmarshal([]byte(s), &data)
		nop(s)
		//data["uid"] = "us-"+strconv.Itoa(i)
        //json.NewDecoder(bytes.NewReader([]byte(s))).Decode(&data)
		pw.WriteJson([]byte(s))
		//pw.Write(&data)
	}
	pw.Close()
	file.Close()
	//GZIP-noJson:   Benchmark_parquetWrite-4   	 1233136	  958 ns/op	      99 B/op	 7 allocs/op
	//SNAPPY-noJson: Benchmark_parquetWrite-4   	 2299634	  555 ns/op	      85 B/op	 7 allocs/op
}
func Benchmark_jsonDecode(b *testing.B) {
	// Benchmark_jsonDecode-4   	  571428	      2098 ns/op	     550 B/op	      32 allocs/op
	b.ReportAllocs()
	b.ResetTimer()
	var data = make(map[string]interface{})
	var format = `{"uid":"%s", "did":"%s", "type":%d, "code":%d,"time":%d}`
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf(format, "us-"+strconv.Itoa(i),
				"c3p"+strconv.Itoa(i), i%8, (i+1)*4+100, time.Now().Unix())
		jsoniter.Unmarshal([]byte(s), &data)
	}
}
func nop(o interface{})  {

}
