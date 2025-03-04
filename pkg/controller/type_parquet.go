package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/electric-saw/pg2parquet/pkg/pgoutputv2"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/xitongsys/parquet-go/types"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// PARQUET MAP
// bool             `parquet:"name=bool, type=BOOLEAN"`
// int32            `parquet:"name=int32, type=INT32"`
// int64            `parquet:"name=int64, type=INT64"`
// string           `parquet:"name=int96, type=INT96"`
// float32          `parquet:"name=float, type=FLOAT"`
// float64          `parquet:"name=double, type=DOUBLE"`
// string           `parquet:"name=bytearray, type=BYTE_ARRAY"`
// string           `parquet:"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10"`
// string           `parquet:"name=utf8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// int32            `parquet:"name=int_8, type=INT32, convertedtype=INT32, convertedtype=INT_8"`
// int32            `parquet:"name=int_16, type=INT32, convertedtype=INT_16"`
// int32            `parquet:"name=int_32, type=INT32, convertedtype=INT_32"`
// int64            `parquet:"name=int_64, type=INT64, convertedtype=INT_64"`
// int32            `parquet:"name=uint_8, type=INT32, convertedtype=UINT_8"`
// int32            `parquet:"name=uint_16, type=INT32, convertedtype=UINT_16"`
// int32            `parquet:"name=uint_32, type=INT32, convertedtype=UINT_32"`
// int64            `parquet:"name=uint_64, type=INT64, convertedtype=UINT_64"`
// int32            `parquet:"name=date, type=INT32, convertedtype=DATE"`
// int32            `parquet:"name=date2, type=INT32, convertedtype=DATE, logicaltype=DATE"`
// int32            `parquet:"name=timemillis, type=INT32, convertedtype=TIME_MILLIS"`
// int32            `parquet:"name=timemillis2, type=INT32, logicaltype=TIME, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
// int64            `parquet:"name=timemicros, type=INT64, convertedtype=TIME_MICROS"`
// int64            `parquet:"name=timemicros2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
// int64            `parquet:"name=timestampmillis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
// int64            `parquet:"name=timestampmillis2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
// int64            `parquet:"name=timestampmicros, type=INT64, convertedtype=TIMESTAMP_MICROS"`
// int64            `parquet:"name=timestampmicros2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
// string           `parquet:"name=interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"`
// int32            `parquet:"name=decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
// int64            `parquet:"name=decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18"`
// string           `parquet:"name=decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12"`
// string           `parquet:"name=decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20"`
// int32            `parquet:"name=decimal5, type=INT32, scale=2, precision=9, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2"`
// map[string]int32 `parquet:"name=map, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
// []string         `parquet:"name=list, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
// []int32          `parquet:"name=repeated, type=INT32, repetitiontype=REPEATED"`

func fieldToParquet(name string, kind pgoutputv2.Kind) reflect.StructField {
	typ := reflect.StructField{
		Name: cases.Title(language.Und).String(name),
	}

	var parquetTag string

	switch kind {
	case pgoutputv2.String:
		typ.Type = reflect.TypeOf("")
		parquetTag = "type=BYTE_ARRAY, convertedType=UTF8"
	case pgoutputv2.StringArray:
		typ.Type = reflect.TypeOf([]string{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"
	case pgoutputv2.Bool:
		typ.Type = reflect.TypeOf(true)
		parquetTag = "type=BOOLEAN"
	case pgoutputv2.BoolArray:
		typ.Type = reflect.TypeOf([]bool{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=BOOLEAN"
	case pgoutputv2.Byte:
		typ.Type = reflect.TypeOf(byte(0))
		parquetTag = "type=INT32, convertedtype=INT32, convertedtype=INT_8"
	case pgoutputv2.ByteArray:
		typ.Type = reflect.TypeOf([]byte{})
		parquetTag = "type=BYTE_ARRAY"
	case pgoutputv2.ByteArrayArray:
		typ.Type = reflect.TypeOf([][]byte{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY"
	case pgoutputv2.Float32:
		typ.Type = reflect.TypeOf(float32(0))
		parquetTag = "type=FLOAT"
	case pgoutputv2.Float32Array:
		typ.Type = reflect.TypeOf([]float32{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=FLOAT"
	case pgoutputv2.Float64:
		typ.Type = reflect.TypeOf(float64(0))
		parquetTag = "type=DOUBLE"
	case pgoutputv2.Float64Array:
		typ.Type = reflect.TypeOf([]float64{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=DOUBLE"
	case pgoutputv2.Int32:
		typ.Type = reflect.TypeOf(int32(0))
		parquetTag = "type=INT32, convertedtype=INT_32"
	case pgoutputv2.Int32Array:
		typ.Type = reflect.TypeOf([]int32{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=INT32, convertedtype=INT_32"
	case pgoutputv2.Int64:
		typ.Type = reflect.TypeOf(int64(0))
		parquetTag = "type=INT64, convertedtype=INT_64"
	case pgoutputv2.Int64Array:
		typ.Type = reflect.TypeOf([]int64{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=INT64, convertedtype=INT_64"
	case pgoutputv2.UInt32:
		typ.Type = reflect.TypeOf(uint32(0))
		parquetTag = "type=INT32, convertedtype=UINT_32"
	case pgoutputv2.Time:
		typ.Type = reflect.TypeOf(time.Now())
		parquetTag = "type=INT64, convertedtype=TIMESTAMP_MICROS, logicaltype.isadjustedtoutc=true"
	case pgoutputv2.TimeArray:
		typ.Type = reflect.TypeOf([]time.Time{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=INT64, convertedtype=TIME_MICROS, logicaltype.isadjustedtoutc=false"
	case pgoutputv2.Interval:
		typ.Type = reflect.TypeOf(time.Duration(0))
		parquetTag = "type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"
	case pgoutputv2.Date:
		typ.Type = reflect.TypeOf(time.Now())
		parquetTag = "type=INT32, convertedtype=DATE, logicaltype=DATE"
	case pgoutputv2.DateArray:
		typ.Type = reflect.TypeOf([]time.Time{})
		parquetTag = "type=MAP, convertedtype=LIST, valuetype=INT32, convertedtype=DATE, logicaltype=DATE"
	default:
		typ.Type = reflect.TypeOf("")
		parquetTag = "type=BYTE_ARRAY, convertedType=UTF8"
	}

	typ.Tag = reflect.StructTag(fmt.Sprintf(`parquet:"name=%s, %s"`, name, parquetTag))
	return typ
}

func dummyType(columns []*pgoutputv2.ValueType) reflect.Type {
	st := []reflect.StructField{}
	for i := range columns {
		st = append(st, fieldToParquet(columns[i].Name, columns[i].GoKind))
	}

	typ := reflect.StructOf(st)
	return typ
}

func valueToParquet(value *pgoutputv2.Value, ptr reflect.Value) error {
	switch value.GoKind {
	case pgoutputv2.StringArray:
		return parseStringArray(value, ptr)
	case pgoutputv2.Time:
		ptr.Set(reflect.ValueOf(types.TimeToTIMESTAMP_MICROS(value.Get().(time.Time), false)))
		return nil
	case pgoutputv2.TimeArray:
		var data []any
		raw := value.Get().([]time.Time)
		for range raw {
			data = append(data, types.TimeToTIMESTAMP_MICROS(value.Get().(time.Time), false))
		}
		ptr.Set(reflect.ValueOf(data))
		return nil
	case pgoutputv2.Interval:
		var data time.Duration
		if err := value.AssignTo(&data); err != nil {
			return fmt.Errorf("failed on parse interval to duration of column %s: %e", value.Name, err)
		}
		ptr.Set(reflect.ValueOf(data.Milliseconds()))
		return nil
	default:
		ptr.Set(reflect.ValueOf(value.Get()))
		return nil
	}
}

func parseStringArray(value *pgoutputv2.Value, ptr reflect.Value) error {
	if ptr.Type().AssignableTo(reflect.TypeOf([]string{})) {
		switch value.PgValue.(type) {
		case *pgtype.UUID:
			var data []pgtype.UUID
			var uuids []string
			if err := value.AssignTo(&data); err != nil {
				return fmt.Errorf("can't assign field %s: %e", value.Name, err)
			}
			for _, uuid := range data {
				var s string
				if err := uuid.Scan(s); err != nil {
					return fmt.Errorf("failed on parse uuid to string: %e", err)
				}
				uuids = append(uuids, s)
			}

			ptr.Set(reflect.ValueOf(uuids))
			return nil
		default:
			var data []string
			if err := value.AssignTo(&data); err != nil {
				return err
			}
			ptr.Set(reflect.ValueOf(data))
			return nil
		}
	} else {
		return fmt.Errorf("can't parse string array on type: %T", ptr)
	}
}
