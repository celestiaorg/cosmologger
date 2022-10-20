package tx

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/celestiaorg/cosmologger/configs"
	"github.com/celestiaorg/cosmologger/database"
	"github.com/celestiaorg/cosmologger/validators"

	// sdkClient "github.com/cosmos/cosmos-sdk/client"
	// authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	tmClient "github.com/tendermint/tendermint/rpc/client/http"
	// coretypes "github.com/tendermint/tendermint/rpc/core/types"
	"github.com/tendermint/tendermint/rpc/coretypes"
	tmTypes "github.com/tendermint/tendermint/types"
	"google.golang.org/grpc"
)

func ProcessEvents(grpcCnn *grpc.ClientConn, evr coretypes.ResultEvent, db *database.Database, insertQueue *database.InsertQueue) error {

	rec := getTxRecordFromEvent(evr)
	rec.LogTime = time.Now()

	dbRow := rec.getDBRow()

	qRes, _ := db.Load(database.TABLE_TX_EVENTS, database.RowType{database.FIELD_TX_EVENTS_TX_HASH: rec.TxHash})
	if len(qRes) > 0 && rec.Module != "" {
		// This tx is already in the DB, let's update it
		go func() {
			_, err := db.Update(
				database.TABLE_TX_EVENTS,
				dbRow,
				database.RowType{database.FIELD_TX_EVENTS_TX_HASH: rec.TxHash},
			)
			if err != nil {
				log.Printf("Err in `Update TX`: %v", err)
			}
		}()

	} else {

		insertQueue.AddToInsertQueue(database.TABLE_TX_EVENTS, dbRow)
	}

	// Let's add validator's info
	if rec.Validator != "" ||
		rec.Action == ACTION_UNJAIL {
		// Just to make things non-blocking
		go func() {

			// When `unjail` actions is invoked, the validator address is in the `sender` filed (well mostly :D)
			if rec.Action == ACTION_UNJAIL &&
				strings.HasPrefix(rec.Sender, configs.Configs.Bech32Prefix.Validator.Address) {

				rec.Validator = rec.Sender
			}

			if rec.Validator != "" {

				err := validators.AddNewValidator(db, grpcCnn, rec.Validator)
				if err != nil {
					log.Printf("Err in `AddNewValidator`: %v", err)
					// return err
				}
			}
		}()
	}

	return nil
}

func getTxRecordFromEvent(evr coretypes.ResultEvent) TxRecord {
	var txRecord TxRecord

	// Let's make it simpler to process and compatible with the old code
	events := map[string]string{}
	for _, e := range evr.Events {
		keyPrefix := e.Type
		for _, a := range e.Attributes {
			events[keyPrefix+"."+a.Key] = a.Value
		}
	}

	if events["tx.height"] != "" {
		txRecord.Height, _ = strconv.ParseUint(events["tx.height"], 10, 64)
	}

	if events["tx.hash"] != "" {
		txRecord.TxHash = events["tx.hash"]
	}

	if events["message.module"] != "" {
		txRecord.Module = events["message.module"]
	}

	if events["message.sender"] != "" {
		txRecord.Sender = events["message.sender"]

	} else if events["transfer.sender"] != "" {

		txRecord.Sender = events["transfer.sender"]
	}

	if events["transfer.recipient"] != "" {
		txRecord.Receiver = events["transfer.recipient"]
	}

	if events["delegate.validator"] != "" {
		txRecord.Validator = events["delegate.validator"]

	} else if events["create_validator.validator"] != "" {

		txRecord.Validator = events["create_validator.validator"]
	}

	if events["message.action"] != "" {
		txRecord.Action = events["message.action"]
	}

	if events["delegate.amount"] != "" {
		txRecord.Amount = events["delegate.amount"]

	} else if events["transfer.amount"] != "" {

		txRecord.Amount = events["transfer.amount"]
	}

	if events["tx.acc_seq"] != "" {
		txRecord.TxAccSeq = events["tx.acc_seq"]
	}

	if events["tx.signature"] != "" {
		txRecord.TxSignature = events["tx.signature"]
	}

	if events["proposal_vote.proposal_id"] != "" {
		txRecord.ProposalId, _ = strconv.ParseUint(events["proposal_vote.proposal_id"], 10, 64)

	} else if events["proposal_deposit.proposal_id"] != "" {

		txRecord.ProposalId, _ = strconv.ParseUint(events["proposal_deposit.proposal_id"], 10, 64)
	}

	// Memo cannot be retrieved through tx events, we may fill it up with another way later
	// txRecord.TxMemo =

	jsonBytes, err := json.Marshal(events)
	if err == nil {
		txRecord.Json = string(jsonBytes)
	}

	// LogTime: is recorded by the DBMS itself

	return txRecord
}

func (t TxRecord) getDBRow() database.RowType {
	return database.RowType{

		database.FIELD_TX_EVENTS_TX_HASH:      t.TxHash,
		database.FIELD_TX_EVENTS_HEIGHT:       t.Height,
		database.FIELD_TX_EVENTS_MODULE:       t.Module,
		database.FIELD_TX_EVENTS_SENDER:       t.Sender,
		database.FIELD_TX_EVENTS_RECEIVER:     t.Receiver,
		database.FIELD_TX_EVENTS_VALIDATOR:    t.Validator,
		database.FIELD_TX_EVENTS_ACTION:       t.Action,
		database.FIELD_TX_EVENTS_AMOUNT:       t.Amount,
		database.FIELD_TX_EVENTS_TX_ACCSEQ:    t.TxAccSeq,
		database.FIELD_TX_EVENTS_TX_SIGNATURE: t.TxSignature,
		database.FIELD_TX_EVENTS_PROPOSAL_ID:  t.ProposalId,
		database.FIELD_TX_EVENTS_TX_MEMO:      t.TxMemo,
		database.FIELD_TX_EVENTS_JSON:         t.Json,
		database.FIELD_TX_EVENTS_LOG_TIME:     t.LogTime,
	}
}

func Start(cli *tmClient.HTTP, grpcCnn *grpc.ClientConn, db *database.Database, insertQueue *database.InsertQueue) {

	go func() {

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(configs.Configs.GRPC.CallTimeout))
		defer cancel()

		eventChan, err := cli.Subscribe(ctx,
			configs.Configs.TendermintClient.SubscriberName,
			tmTypes.QueryForEvent(tmTypes.EventTxValue).String(),
		)
		if err != nil {
			panic(err)
		}

		for {
			evRes := <-eventChan
			if err := ProcessEvents(grpcCnn, evRes, db, insertQueue); err != nil {
				log.Printf("Error in processing TX event: %v", err)
			}
		}
	}()

	// fixEmptyEvents(cli, db)
}

// Since some TX events are delayed and we catch them empty, we need to query them later to get them fixed
func fixEmptyEvents(cli *tmClient.HTTP, db *database.Database) {

	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
		os.Kill, //nolint
		os.Interrupt)

	go func() {

		for {
			select {
			case <-quitChannel:
				return
			default:

				// Get all TX events that are empty
				rows, err := db.Load(database.TABLE_TX_EVENTS, database.RowType{database.FIELD_TX_EVENTS_MODULE: ""})
				if err != nil {
					log.Printf("Error in loading empty TX events: %v", err)
				}

				for _, row := range rows {
					txHash := string(row[database.FIELD_TX_EVENTS_TX_HASH].([]uint8))
					// Quering the TX from the Node...
					rec, err := queryTx(cli, txHash)
					if err != nil {
						log.Printf("Error in querying TX: %s\t %v", txHash, err)
						continue
					}
					rec.LogTime = time.Now()
					dbRow := rec.getDBRow()

					_, err = db.Update(database.TABLE_TX_EVENTS, dbRow, database.RowType{database.FIELD_TX_EVENTS_TX_HASH: rec.TxHash})
					if err != nil {
						log.Printf("[FixEmptyEvents] Err in `Update TX`: %s\t %v", txHash, err)
					}
				}

				time.Sleep(time.Second)
			}
		}
	}()

}

func queryTx(cli *tmClient.HTTP, txHash string) (TxRecord, error) {

	wsURI := os.Getenv("RPC_ADDRESS")

	// A dirty hack to get the things done
	cmd := exec.Command("archwayd", "query", "tx", txHash, "--node", wsURI, "--output", "json")
	stdout, err := cmd.Output()

	if err != nil {
		return TxRecord{}, err
	}

	rec := getTxRecordFromJson(stdout)

	return rec, nil

}

func getTxRecordFromJson(jsonByte []byte) TxRecord {
	var txRecord TxRecord
	// jsonStr = strings.Trim(jsonStr, " \r\n\t")

	jsonVar := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonByte), &jsonVar)
	if err != nil {
		fmt.Printf("Unmarshaling JSON Err: %v\n", err.Error())
		return txRecord
	}

	if jsonVar["height"] != nil && len(jsonVar["height"].(string)) > 0 {
		txRecord.Height, _ = strconv.ParseUint(jsonVar["height"].(string), 10, 64)
	}

	if jsonVar["txhash"] != nil && len(jsonVar["txhash"].(string)) > 0 {
		txRecord.TxHash = jsonVar["txhash"].(string)
	}

	if jsonVar["codespace"] != nil && len(jsonVar["codespace"].(string)) > 0 {
		txRecord.Module = jsonVar["codespace"].(string)
	}

	messages := []interface{}{}
	if txJson, ok := jsonVar["tx"].(map[string]interface{}); ok {
		if body, ok := txJson["body"].(map[string]interface{}); ok {
			if msgs, ok := body["messages"].([]interface{}); ok {
				messages = msgs
			}
		}

		if val, ok := txJson["signatures"].([]interface{}); ok {
			txRecord.TxSignature = val[0].(string)
		}
	}

	for i := range messages {
		msg := messages[i].(map[string]interface{})
		if val, ok := msg["@type"].(string); ok {
			if val == "" {
				val = "NA"
			}
			txRecord.Action = val
		}

		if val, ok := msg["sender"].(string); ok {
			txRecord.Sender = val
		} else if val, ok := msg["delegator_address"].(string); ok {
			txRecord.Sender = val
		} else if val, ok := msg["inputs"].([]interface{}); ok {

			if addr, ok := val[0].(map[string]interface{})["address"].(string); ok {
				txRecord.Sender = addr
			}
		}

		if val, ok := msg["validator_address"].(string); ok {
			txRecord.Receiver = val
			txRecord.Validator = val
		} else if val, ok := msg["recipient"].(string); ok {
			txRecord.Receiver = val
		} else if val, ok := msg["outputs"].([]interface{}); ok {
			if addr, ok := val[0].(map[string]interface{})["address"].(string); ok {
				txRecord.Receiver = addr
			}
		}

		if val, ok := msg["value"].(map[string]interface{}); ok {
			txRecord.Amount = val["amount"].(string) + val["denom"].(string)
		} else if val, ok := msg["amount"].(map[string]interface{}); ok {
			txRecord.Amount = val["amount"].(string) + val["denom"].(string)
		}

	}

	// if jsonVar["proposal_vote.proposal_id"] != nil && len(jsonVar["proposal_vote.proposal_id"]) > 0 {
	// 	txRecord.ProposalId, _ = strconv.ParseUint(jsonVar["proposal_vote.proposal_id"], 10, 64)

	// } else if jsonVar["proposal_deposit.proposal_id"] != nil && len(jsonVar["proposal_deposit.proposal_id"]) > 0 {

	// 	txRecord.ProposalId, _ = strconv.ParseUint(jsonVar["proposal_deposit.proposal_id"], 10, 64)
	// }

	if txRecord.Module == "" {
		txRecord.Module = "NA"
	}

	txRecord.Json = string(jsonByte)

	// LogTime: is recorded by the DBMS itself

	return txRecord
}
