package pgoutputv2

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Kind int

var decMap = pgtype.NewMap()

const (
	String Kind = iota
	StringArray
	Bool
	BoolArray
	Byte
	ByteArray
	ByteArrayArray
	Time
	TimeArray
	Date
	DateArray
	Interval
	Float32
	Float32Array
	Float64
	Float64Array
	Int32
	Int32Array
	Int64
	Int64Array
	UInt32
)

type RelationManager struct {
	mu        sync.RWMutex
	relations map[uint32]*pglogrepl.RelationMessage
	typeNames map[uint32]string
}

type ValueType struct {
	Name       string
	GoKind     Kind
	PgTypeName string
}
type Value struct {
	PgValue any
	Data    *[]byte
	ValueType
}

type Values []*Value

func NewRelationManager() *RelationManager {
	return &RelationManager{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		typeNames: make(map[uint32]string),
	}
}

func (rs *RelationManager) RefreshTypes(ctx context.Context, conn *pgx.Conn) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	sql := "SELECT oid, typname from pg_type;"
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return err
	}

	for rows.Next() {
		var id uint32
		var name string
		if err = rows.Scan(&id, &name); err != nil {
			return err
		}
		rs.typeNames[id] = name
	}
	return nil
}

func (rs *RelationManager) Set(r *pglogrepl.RelationMessage) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.relations[r.RelationID] = r
}

func (rs *RelationManager) Get(id uint32) (*pglogrepl.RelationMessage, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	r, ok := rs.relations[id]
	return r, ok
}

func (rs *RelationManager) pgTypeName(id uint32) string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.typeNames[id]
}

func (rs *RelationManager) ParseValues(id uint32, row *pglogrepl.TupleData) (Values, error) {
	result := Values{}
	r, ok := rs.Get(id)
	if !ok {
		return result, fmt.Errorf("can't found relation %d", id)
	}

	for i := range row.Columns {
		col := r.Columns[i]
		dec, kind := Decoder(col)

		val, err := dec.DecodeValue(decMap, col.DataType, pgtype.TextFormatCode, row.Columns[i].Data)
		if err != nil {
			return nil, fmt.Errorf("can't decode tuple %s, %s", col.Name, err)
		}

		result = append(result, &Value{
			PgValue: val,
			Data:    &row.Columns[i].Data,
			ValueType: ValueType{
				Name:       col.Name,
				GoKind:     kind,
				PgTypeName: rs.pgTypeName(col.DataType),
			},
		})
	}
	return result, nil
}

func (rs *RelationManager) Types(id uint32) ([]*ValueType, error) {
	result := []*ValueType{}
	r, ok := rs.Get(id)
	if !ok {
		return result, fmt.Errorf("can't found relation %d", id)
	}
	for _, col := range r.Columns {
		_, kind := Decoder(col)

		result = append(result, &ValueType{
			Name:       col.Name,
			PgTypeName: rs.pgTypeName(col.DataType),
			GoKind:     kind,
		})
	}
	return result, nil
}

func Decoder(col *pglogrepl.RelationMessageColumn) (pgtype.Codec, Kind) {
	var decoder pgtype.Codec
	var kind Kind = String

	if t, ok := decMap.TypeForOID(col.DataType); ok {

		decoder = t.Codec
	} else {
		decoder = &pgtype.TextCodec{}
	}

	switch col.DataType {
	case pgtype.ACLItemArrayOID:
		kind = StringArray
	case pgtype.BoolArrayOID:
		kind = BoolArray
	case pgtype.BPCharArrayOID:
		kind = StringArray
	case pgtype.ByteaArrayOID:
		kind = ByteArrayArray
	case pgtype.CIDRArrayOID:
		kind = ByteArrayArray
	case pgtype.DateArrayOID:
		kind = DateArray
	case pgtype.Float4ArrayOID:
		kind = Float32Array
	case pgtype.Float8ArrayOID:
		kind = Float64Array
	case pgtype.InetArrayOID:
		kind = ByteArrayArray
	case pgtype.Int2ArrayOID:
		kind = Int32Array
	case pgtype.Int4ArrayOID:
		kind = Int32Array
	case pgtype.Int8ArrayOID:
		kind = Int64Array
	case pgtype.NumericArrayOID:
		kind = Int64Array
	case pgtype.TextArrayOID:
		kind = StringArray
	case pgtype.TimestampArrayOID:
		kind = TimeArray
	case pgtype.TimestamptzArrayOID:
		kind = TimeArray
	case pgtype.UUIDArrayOID:
		kind = StringArray
	case pgtype.VarcharArrayOID:
		kind = StringArray
	case pgtype.ACLItemOID:
		kind = String
	case pgtype.BitOID:
		kind = String
	case pgtype.BoolOID:
		kind = Bool
	case pgtype.BoxOID:
		// TODO: parse BoxOID
	case pgtype.BPCharOID:
		kind = String
	case pgtype.ByteaOID:
		kind = ByteArray
	case pgtype.QCharOID:
		kind = String
	case pgtype.CIDOID:
		kind = UInt32
	case pgtype.CIDROID:
		// TODO: parse CIDROID
	case pgtype.CircleOID:
		// TODO: parse CircleOID
	case pgtype.DateOID:
		kind = Date
	case pgtype.DaterangeOID:
		// TODO: parse DaterangeOID
	case pgtype.Float4OID:
		kind = Float32
	case pgtype.Float8OID:
		kind = Float64
	case pgtype.InetOID:
		// TODO: parse InetOID
	case pgtype.Int2OID:
		kind = Int32
	case pgtype.Int4OID:
		kind = Int32
	case pgtype.Int4rangeOID:
		// TODO: parse Int4rangeOID
	case pgtype.Int8OID:
		kind = Int64
	case pgtype.Int8rangeOID:
		// TODO: parse Int8rangeOID
	case pgtype.IntervalOID:
		kind = Interval
		// TODO: parse IntervalOID ===
	case pgtype.JSONOID:
		kind = String
	case pgtype.JSONBOID:
		kind = String
	case pgtype.LineOID:
		// TODO: parse LineOID
	case pgtype.LsegOID:
		// TODO: parse LsegOID
	case pgtype.MacaddrOID:
		kind = String
	case pgtype.NameOID:
		kind = String
	case pgtype.NumericOID:
		kind = Int64
	case pgtype.NumrangeOID:
		// TODO: parse NumrangeOID ===
	case pgtype.OIDOID:
		kind = UInt32
	case pgtype.PathOID:
		// TODO: parse PathOID
	case pgtype.PointOID:
		// TODO: parse PointOID
	case pgtype.PolygonOID:
		// TODO: parse PolygonOID
	case pgtype.TextOID:
		kind = String
	case pgtype.TIDOID:
		kind = String
	case pgtype.TimestampOID:
		kind = Time
	case pgtype.TimestamptzOID:
		kind = Time
	case pgtype.TsrangeOID:
		// TODO: parse TsrangeOID
	case pgtype.TstzrangeOID:
		// TODO: parse TstzrangeOID
	case pgtype.UnknownOID:
		kind = String
	case pgtype.UUIDOID:
		kind = String
	case pgtype.VarbitOID:
		// TODO: parse VarbitOID
	case pgtype.VarcharOID:
		kind = String
	case pgtype.XIDOID:
		kind = UInt32
	default:
		kind = String
	}

	return decoder, kind
}

func (vt ValueType) String() string {
	return fmt.Sprintf("PgType: %s, goKind: %d", vt.PgTypeName, vt.GoKind)
}

func (v *Value) Get() any {
	return v.PgValue
	// val, err := v.PgValue.(driver.Valuer).Value()
	// if err != nil {
	// 	return nil
	// }

	// return val
}

func (v *Value) AssignTo(dst any) error {
	t, ok := decMap.TypeForValue(dst)
	if !ok {
		return fmt.Errorf("can't assign to %T", dst)
	}

	return decMap.Scan(t.OID, pgtype.TextFormatCode, *v.Data, dst)
}

func (v *Value) String() string {
	return fmt.Sprint(v.Get())
}

func (v Values) GetValues() []any {
	var values []any

	for _, value := range v {
		values = append(values, value.Get())
	}

	return values
}

func (v Values) GetNamedValues() map[string]any {
	values := make(map[string]any)

	for _, value := range v {
		values[value.Name] = value.Get()
	}

	return values
}
