package block

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/celestiaorg/cosmologger/configs"
	"github.com/celestiaorg/cosmologger/database"
	"github.com/celestiaorg/cosmologger/validators"
	sdk "github.com/cosmos/cosmos-sdk/types"

	// coretypes "github.com/tendermint/tendermint/rpc/core/types"
	tmClient "github.com/tendermint/tendermint/rpc/client/http"
	"github.com/tendermint/tendermint/rpc/coretypes"
	tmTypes "github.com/tendermint/tendermint/types"
	"google.golang.org/grpc"
)

var genesisValidatorsDone bool

func ProcessEvents(grpcCnn *grpc.ClientConn, evr *coretypes.ResultEvent, db *database.Database, insertQueue *database.InsertQueue) error {
	rec := getBlockRecordFromEvent(evr)
	fmt.Printf("Block: %s\tH: %d\tTxs: %d\n", rec.BlockHash, rec.Height, rec.NumOfTxs)

	dbRow := rec.getBlockDBRow()
	insertQueue.Add(database.TABLE_BLOCKS, dbRow)

	for i := range rec.LastBlockSigners {
		dbRow := rec.LastBlockSigners[i].getBlockSignerDBRow()
		insertQueue.Add(database.TABLE_BLOCK_SIGNERS, dbRow)
	}

	// Let's add genesis validator's info
	if !genesisValidatorsDone && rec.Height > 50 {
		// Let's do it only once
		genesisValidatorsDone = true

		// Just to make things non-blocking
		go func() {

			valList, err := validators.QueryValidatorsList(grpcCnn)
			if err != nil {
				log.Printf("Err in `validators.QueryValidatorsList`: %v", err)
				// return err
			}

			for i := range valList {
				err := validators.AddNewValidator(db, grpcCnn, valList[i])
				if err != nil {
					log.Printf("Err in `AddNewValidator`: %v", err)
					// return err
				}
			}

		}()
	}

	return ProcessContractEvents(grpcCnn, evr, db, insertQueue)
}

func getBlockRecordFromEvent(evr *coretypes.ResultEvent) *BlockRecord {

	b := evr.Data.(tmTypes.EventDataNewBlock)
	return getBlockRecordFromTmBlock(b.Block)
}

func getBlockRecordFromTmBlock(b *tmTypes.Block) *BlockRecord {
	var br BlockRecord

	br.BlockHash = b.Hash().String()

	br.Height = uint64(b.Height)
	br.NumOfTxs = uint64(len(b.Txs))
	br.Time = b.Time

	for i := range b.LastCommit.Signatures {

		consAddr, err := sdk.ConsAddressFromHex(b.LastCommit.Signatures[i].ValidatorAddress.String())
		if err != nil {
			continue // just ignore this signer as it might not be running and we face some strange error
		}

		br.LastBlockSigners = append(br.LastBlockSigners, BlockSignersRecord{
			BlockHeight: br.Height - 1, // Because the signers are for the previous block
			ValConsAddr: consAddr.String(),
			Time:        b.LastCommit.Signatures[i].Timestamp,
			Signature:   base64.StdEncoding.EncodeToString(b.LastCommit.Signatures[i].Signature),
		})
	}

	return &br
}

func (b *BlockRecord) getBlockDBRow() database.RowType {
	return database.RowType{
		database.FIELD_BLOCKS_BLOCK_HASH: b.BlockHash,
		database.FIELD_BLOCKS_HEIGHT:     b.Height,
		database.FIELD_BLOCKS_NUM_OF_TXS: b.NumOfTxs,
		database.FIELD_BLOCKS_TIME:       b.Time,
	}
}

func (s *BlockSignersRecord) getBlockSignerDBRow() database.RowType {
	return database.RowType{
		database.FIELD_BLOCK_SIGNERS_BLOCK_HEIGHT:  s.BlockHeight,
		database.FIELD_BLOCK_SIGNERS_VAL_CONS_ADDR: s.ValConsAddr,
		database.FIELD_BLOCK_SIGNERS_TIME:          s.Time,
		database.FIELD_BLOCK_SIGNERS_SIGNATURE:     s.Signature,
	}
}

func Start(cli *tmClient.HTTP, grpcCnn *grpc.ClientConn, db *database.Database, insertQueue *database.InsertQueue) {

	go func() {

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(configs.Configs.GRPC.CallTimeout))
		defer cancel()

		eventChan, err := cli.Subscribe(
			ctx,
			configs.Configs.TendermintClient.SubscriberName,
			tmTypes.QueryForEvent(tmTypes.EventNewBlockValue).String(),
		)
		if err != nil {
			panic(err)
		}

		for {
			evRes := <-eventChan
			if err := ProcessEvents(grpcCnn, &evRes, db, insertQueue); err != nil {
				//TODO: We need some customizable log level
				log.Printf("Error in processing block event: %v", err)
			}
		}
	}()

	// fixMissingBlocks(cli, db, insertQueue)
}

func findMissingBlocks(start, end uint64, db *database.Database) ([]uint64, error) {
	var missingBlocks []uint64

	totalBlocks, err := GetTotalBlocksByRange(start, end, db)
	if err != nil {
		return missingBlocks, err
	}
	expectedBlocks := end - start + 1

	if totalBlocks != expectedBlocks {
		if start == end {
			missingBlocks = append(missingBlocks, start)
		} else {
			middle := (start + end) / 2
			mb1, err := findMissingBlocks(start, middle, db)
			if err != nil {
				return missingBlocks, err
			}
			missingBlocks = append(missingBlocks, mb1...)

			mb2, err := findMissingBlocks(middle+1, end, db)
			if err != nil {
				return missingBlocks, err
			}
			missingBlocks = append(missingBlocks, mb2...)
		}
	}

	return missingBlocks, nil
}

func GetTotalBlocksByRange(start, end uint64, db *database.Database) (uint64, error) {

	SQL := fmt.Sprintf(`
		SELECT 
			COUNT(*) AS "total"
		FROM "%s"
		WHERE 
			"height" >= $1 AND 
			"height" <= $2`,
		database.TABLE_BLOCKS,
	)

	rows, err := db.Query(SQL, database.QueryParams{start, end})
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 ||
		rows[0] == nil ||
		rows[0]["total"] == nil {
		return 0, nil
	}

	return uint64(rows[0]["total"].(int64)), nil
}

func GetLatestBlockHeight(db *database.Database) (uint64, error) {

	SQL := fmt.Sprintf(
		`SELECT MAX("%s") AS "result" FROM "%s"`,

		database.FIELD_BLOCKS_HEIGHT,
		database.TABLE_BLOCKS,
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
