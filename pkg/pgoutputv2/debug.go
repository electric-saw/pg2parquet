package pgoutputv2

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

func DebugMsg(m pglogrepl.Message) string {
	switch msg := m.(type) {
	case *pglogrepl.BeginMessage:
		return fmt.Sprintf("Begin message LSN: %s XID: %d", msg.FinalLSN.String(), msg.Xid)
	case *pglogrepl.RelationMessage:
		return fmt.Sprintf("Relation message ID: %d Replica: %d", msg.RelationID, msg.ReplicaIdentity)
	case *pglogrepl.RelationMessageV2:
		return fmt.Sprintf("Relation message ID: %d Replica: %d", msg.RelationID, msg.ReplicaIdentity)
	case *pglogrepl.UpdateMessage:
		return fmt.Sprintf("Update on relation ID: %d", msg.RelationID)
	case *pglogrepl.InsertMessage:
		return fmt.Sprintf("Insert on relation ID: %d", msg.RelationID)
	case *pglogrepl.InsertMessageV2:
		return fmt.Sprintf("Insert on relation ID: %d", msg.RelationID)
	case *pglogrepl.DeleteMessage:
		return fmt.Sprintf("Delete on relation ID: %d", msg.RelationID)
	case *pglogrepl.CommitMessage:
		return fmt.Sprintf("Commit message LSN: %s Transaction: %s", msg.CommitLSN.String(), msg.TransactionEndLSN.String())
	case *pglogrepl.OriginMessage:
		return fmt.Sprintf("Origin message LSN: %s Origin: %s", msg.CommitLSN.String(), msg.Name)
	case *pglogrepl.TypeMessage:
		return fmt.Sprintf("Type message ID: %d Name: %s", msg.DataType, msg.Name)
	case *pglogrepl.TruncateMessage:
		return fmt.Sprintf("Truncate relation: %v, total relations: %d", msg.RelationIDs, msg.RelationNum)
	default:
		return fmt.Sprintf("Unknown message type: %T", msg)
	}

}
