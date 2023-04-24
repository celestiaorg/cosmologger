package database

import (
	"fmt"
	"log"
	"time"

	fifo "github.com/foize/go.fifo"
)

type InsertQueue struct {
	insert *fifo.Queue
	db     *Database
	closed bool
}

func NewInsertQueue(db *Database) *InsertQueue {
	return &InsertQueue{
		insert: fifo.NewQueue(),
		closed: false,
		db:     db,
	}
}

func (i *InsertQueue) Add(table string, row ...RowType) {
	i.insert.Add(&InsertQueueItem{
		Table: table,
		Rows:  row,
		DB:    i.db,
	})
}

func (i *InsertQueue) Start() error {
	if i.closed {
		return fmt.Errorf("queue is already closed")
	}
	go func() {
		for !i.closed {

			item := i.insert.Next()
			if item == nil {
				// Queue is empty, keep waiting
				time.Sleep(50 * time.Millisecond)
				continue
			}

			iq := item.(*InsertQueueItem)
			if len(iq.Rows) == 0 {
				continue
			}

			for _, row := range iq.Rows {
				_, err := i.db.Insert(iq.Table, row)
				if err != nil {
					log.Printf("async insert: %v\ntable: `%v`\t row: %#v\n", err, iq.Table, row)
				}
			}
		}
	}()
	return nil
}

func (i *InsertQueue) Stop() {
	i.closed = true
}

func (i *InsertQueue) GetQueueLen() int {
	return i.insert.Len()
}
