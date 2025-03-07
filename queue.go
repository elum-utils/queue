package queue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Item represents a queue item with an ID, data, and a creation timestamp.
type Item struct {
	ID   int    // Unique identifier for the item.
	Data []byte // Data of the item, stored as a byte slice.
}

// Queue provides a FIFO queue backed by a SQLite database.
type Queue struct {
	db         *sql.DB            // The SQL database connection used by the queue.
	ctx        context.Context    // Context for managing request-scoped values and cancellation signals.
	cancelFunc context.CancelFunc // Cancellation function for the context
	clb        func(item Item, delay func(sec time.Duration))

	mx sync.Mutex // Mutex to ensure thread-safe operations on the queue.
}

// New initializes a new Queue instance and sets up the database connection.
// It optionally resets the database if specified in the configuration.
func New(config ...Config) (*Queue, error) {
	cfg := configDefault(config...) // Retrieve the configuration with defaults.

	inMemory := strings.HasPrefix(cfg.LocalFile, "file::memory_")
	if cfg.Reset && !inMemory {
		// Remove the database file if reset is requested and it's not an in-memory database.
		err := os.Remove(cfg.LocalFile)
		if err != nil && !os.IsNotExist(err) {
			return nil, err // Return an error if it's not "file does not exist" error.
		}
	}

	// Initialize SQLite database connection.
	db, err := sql.Open("sqlite3", cfg.LocalFile)
	if err != nil {
		return nil, err
	}

	// Create the queue table if it does not exist.
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data BLOB NOT NULL
        );
    `)
	if err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	c := &Queue{
		db:         db,
		ctx:        ctx,
		cancelFunc: cancelFunc,
		clb:        func(item Item, delay func(sec time.Duration)) {},
	}

	go c.process()

	return c, nil
}

// Add inserts a new item with the specified data into the queue.
func (c *Queue) Add(data []byte) error {
	c.mx.Lock() // Lock for exclusive access to the queue.
	defer c.mx.Unlock()

	_, err := c.db.ExecContext(
		c.ctx,
		"INSERT INTO queue(`data`) VALUES (?)",
		data,
	)
	return err
}

// Get retrieves up to 'limit' items from the queue.
// It returns the items along with any error encountered.
func (c *Queue) Get(limit int) ([]Item, error) {
	c.mx.Lock() // Lock for exclusive access to the queue.
	defer c.mx.Unlock()

	rows, err := c.db.Query(
		"SELECT `id`, `data` FROM queue LIMIT ?",
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // Ensure rows are closed after processing.

	var items []Item
	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.ID, &item.Data); err != nil {
			return nil, err
		}
		items = append(items, item) // Collect items into a slice.
	}
	return items, nil
}

// Delete removes an item with the specified ID from the queue.
func (c *Queue) Delete(id int) error {
	c.mx.Lock() // Lock for exclusive access to the queue.
	defer c.mx.Unlock()

	_, err := c.db.Exec("DELETE FROM queue WHERE id = ?", id)
	return err
}

func (c *Queue) Listener(clb func(item Item, delay func(sec time.Duration))) {
	c.clb = clb
}

func (c *Queue) Close() error {
	c.cancelFunc()
	return c.db.Close()
}

func (c *Queue) process() {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
			c.process() // Restart subscription on panic
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			fmt.Println("Shutting down process loop")
			return
		default:
			items, err := c.Get(1) // Try to get one item
			if err != nil {
				fmt.Println("Error retrieving item:", err)
				continue
			}

			if len(items) > 0 {
				for _, item := range items {
					var delay time.Duration
					broken := func(sec time.Duration) {
						delay = sec
					}

					c.clb(item, broken)

					if delay > 0 {
						fmt.Println("Processing broke, sleeping for 30 seconds")
						time.Sleep(delay)
					}
				}
			} else {
				time.Sleep(2 * time.Second)
			}
		}
	}
}
