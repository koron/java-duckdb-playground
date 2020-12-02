package main

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Item struct {
	I int32 `parquet:"name=i, type=INT32"`
}

func gen(name string) error {
	fw, err := local.NewLocalFileWriter(name)
	if err != nil {
		return err
	}
	defer fw.Close()
	pw, err := writer.NewParquetWriter(fw, &Item{}, 4)
	if err != nil {
		return err
	}
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	for i := 0; i < 10000000; i++ {
		err := pw.Write(&Item{I: int32(i % 5)})
		if err != nil {
			return err
		}
	}
	err = pw.WriteStop()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	err := gen("simple.parquet")
	if err != nil {
		log.Fatal(err)
	}
}
