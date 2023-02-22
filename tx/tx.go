package tx

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/celestiaorg/cosmologger/configs"
	"github.com/celestiaorg/cosmologger/database"
	"github.com/celestiaorg/cosmologger/validators"

	// sdkClient "github.com/cosmos/cosmos-sdk/client"
	// authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"

	tmClient "github.com/tendermint/tendermint/rpc/client/http"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"

	// "github.com/tendermint/tendermint/rpc/coretypes"
	tmTypes "github.com/tendermint/tendermint/types"
	"google.golang.org/grpc"
)

func ProcessEvents(grpcCnn *grpc.ClientConn, rec TxRecord, db *database.Database, insertQueue *database.InsertQueue) error {

	rec.LogTime = time.Now()
	dbRow := rec.getDBRow()

	qRes, _ := db.Load(database.TABLE_TX_EVENTS, database.RowType{database.FIELD_TX_EVENTS_TX_HASH: rec.TxHash})
	if len(qRes) > 0 && rec.Action != "" {
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

		insertQueue.Add(database.TABLE_TX_EVENTS, dbRow)
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

	// Simplify the nested map by flattening it
	events := map[string]string{}
	for k, v := range evr.Events {
		if len(v) > 0 {
			events[k] = v[0]
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

	} else if events["payfordata.signer"] != "" {

		txRecord.Sender = events["payfordata.signer"]

	} else if events["payforblob.signer"] != "" {

		txRecord.Sender = events["payforblob.signer"]
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

func Start(cli *tmClient.HTTP, grpcCnn *grpc.ClientConn, db *database.Database, insertQueue *database.InsertQueue, mode DataCollectionMode) {

	if mode == PullMode {

		go func() {

			height, err := getLatestProcessedBlockHeight(db)
			if err != nil {
				log.Fatalf("getting the latest processed block height in tx_events: %v", err)
			}
			// height++ // let's comment this out in case the program was aborted in the middle of the tx process

			for {
				// ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(configs.Configs.GRPC.CallTimeout))
				ctx := context.Background()

				txs, err := queryTxsByHeight(ctx, cli, height)
				if err != nil {
					log.Printf("getting the tx_events data: %v", err)
					time.Sleep(time.Microsecond * 50)
					continue // Try again
				}

				for _, t := range txs {
					if err := ProcessEvents(grpcCnn, *t, db, insertQueue); err != nil {
						log.Printf("processing tx_event: %v", err)
					}
				}

				// cancel()
				height++
			}

		}()

		return
	}

	if mode == EventMode {

		go func() {

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(configs.Configs.GRPC.CallTimeout))
			defer cancel()

			eventChan, err := cli.Subscribe(ctx,
				configs.Configs.TendermintClient.SubscriberName,
				tmTypes.QueryForEvent(tmTypes.EventTx).String(),
			)
			if err != nil {
				panic(err)
			}

			for {
				evRes := <-eventChan
				rec := getTxRecordFromEvent(evRes)
				if err := ProcessEvents(grpcCnn, rec, db, insertQueue); err != nil {
					log.Printf("processing TX event: %v", err)
				}
			}
		}()

		return
	}

	// fixEmptyEvents(cli, db)
}

func getLatestProcessedBlockHeight(db *database.Database) (uint64, error) {

	SQL := fmt.Sprintf(
		`SELECT MAX("%s") AS "result" FROM "%s"`,

		database.FIELD_TX_EVENTS_HEIGHT,
		database.TABLE_TX_EVENTS,
	)

	rows, err := db.Query(SQL, database.QueryParams{})
	if err != nil {
		return 0, err
	}

	if len(rows) == 0 ||
		rows[0] == nil ||
		rows[0]["result"] == nil {
		return 0, nil
	}

	return uint64(rows[0]["result"].(int64)), nil
}

func queryTxsByHeight(ctx context.Context, cli *tmClient.HTTP, height uint64) ([]*TxRecord, error) {

	recs := []*TxRecord{}

	pg := 1
	ppg := 100

	for {

		res, err := cli.TxSearch(ctx, fmt.Sprintf("tx.height=%d", height), false, &pg, &ppg, "")
		if err != nil {
			return nil, err
		}

		if res.TotalCount == 0 {
			break
		}

		for _, t := range res.Txs {
			rec := getTxRecordFromTxResult(t)
			recs = append(recs, &rec)
		}

		if res.TotalCount <= ppg*pg {
			break
		}

		pg++
	}

	return recs, nil

}

func getTxRecordFromTxResult(t *coretypes.ResultTx) TxRecord {

	evr := coretypes.ResultEvent{}
	evr.Events = make(map[string][]string)

	for _, e := range t.TxResult.Events {
		keyPrefix := e.Type
		for _, a := range e.Attributes {
			k := keyPrefix + "." + string(a.Key)
			if evr.Events[k] == nil {
				evr.Events[k] = []string{string(a.Value)}
			} else {
				evr.Events[k] = append(evr.Events[k], string(a.Value))
			}
		}
	}

	evr.Events["tx.hash"] = []string{t.Hash.String()}
	evr.Events["tx.height"] = []string{fmt.Sprintf("%d", t.Height)}

	return getTxRecordFromEvent(evr)
}
