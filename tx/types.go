package tx

import "time"

type TxRecord struct {
	TxHash      string
	Height      uint64
	Module      string
	Sender      string
	Receiver    string
	Validator   string
	Action      string
	Amount      string
	TxAccSeq    string
	TxSignature string
	ProposalId  uint64
	TxMemo      string
	Json        string
	LogTime     time.Time
}

type DataCollectionMode int

const (
	PullMode DataCollectionMode = iota + 1
	EventMode
)
