package pgoutputv2

import "github.com/jackc/pglogrepl"

func ParseWalMessage(raw []byte, inStream bool) (pglogrepl.Message, error) {
	msg, err := pglogrepl.ParseV2(raw, inStream)
	if err != nil {
		return nil, err
	}

	return msg, nil
}
