package db

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/ozgen/raven-mq/internal/log"
)

// Storage handles persistent storage in SQLite.
type Storage struct {
	db *sql.DB
}

// NewStorage initializes SQLite database.
func NewStorage(dbPath string) (*Storage, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Create necessary tables
	createTablesSQL := `
	CREATE TABLE IF NOT EXISTS exchanges (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		type TEXT NOT NULL
	);
	CREATE TABLE IF NOT EXISTS queues (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE
	);
	CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		queue_name TEXT NOT NULL,
		message TEXT NOT NULL,
		routing_key TEXT NOT NULL,
		status TEXT NOT NULL 
	);
	`
	_, err = db.Exec(createTablesSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	log.LogInfo("SQLite storage initialized successfully.")
	return &Storage{db: db}, nil
}

// SaveExchange stores an exchange persistently.
func (s *Storage) SaveExchange(name, exType string) error {
	_, err := s.db.Exec("INSERT OR IGNORE INTO exchanges (name, type) VALUES (?, ?)", name, exType)
	return err
}

// LoadExchanges retrieves all stored exchanges.
func (s *Storage) LoadExchanges() (map[string]string, error) {
	rows, err := s.db.Query("SELECT name, type FROM exchanges")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	exchanges := make(map[string]string)
	for rows.Next() {
		var name, exType string
		if err := rows.Scan(&name, &exType); err != nil {
			return nil, err
		}
		exchanges[name] = exType
	}
	return exchanges, nil
}

// SaveQueue persists a queue.
func (s *Storage) SaveQueue(name string) error {
	_, err := s.db.Exec("INSERT OR IGNORE INTO queues (name) VALUES (?)", name)
	return err
}

// LoadQueues retrieves all stored queues.
func (s *Storage) LoadQueues() ([]string, error) {
	rows, err := s.db.Query("SELECT name FROM queues")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		queues = append(queues, name)
	}
	return queues, nil
}

func (s *Storage) SaveMessage(queue, message, routingKey string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	_, err = tx.Exec("INSERT INTO messages (queue_name, message, routing_key, status) VALUES (?, ?, ?, 'pending')",
		queue, message, routingKey)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to save message: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit message: %w", err)
	}

	log.LogInfo("Stored message '%s' in queue '%s' as 'pending'", message, queue)
	return nil
}

// LoadMessages retrieves messages for a queue.
func (s *Storage) LoadMessages(queue string) ([]string, error) {
	rows, err := s.db.Query("SELECT message FROM messages WHERE queue_name = ?", queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []string
	for rows.Next() {
		var message string
		if err := rows.Scan(&message); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (s *Storage) DeleteMessage(queue, message string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	// Use `ROWID` to delete only one instance of the message
	result, err := tx.Exec(`
		DELETE FROM messages 
		WHERE ROWID IN (
			SELECT ROWID FROM messages 
			WHERE queue_name = ? AND message = ? 
			LIMIT 1
		)`, queue, message)

	if err != nil {
		tx.Rollback() // Rollback on failure
		return fmt.Errorf("failed to delete message: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		log.LogWarn("No message deleted for queue '%s' with message '%s'", queue, message)
	} else {
		log.LogInfo("Message '%s' deleted from queue '%s' in SQLite", message, queue)
	}

	return tx.Commit() // Commit transaction
}

func (s *Storage) MarkMessageUnacknowledged(queue, message string) error {
	_, err := s.db.Exec("UPDATE messages SET status = 'pending' WHERE queue_name = ? AND message = ?", queue, message)
	if err != nil {
		return fmt.Errorf("failed to mark message as unacknowledged: %w", err)
	}
	log.LogInfo("Marked message '%s' as unacknowledged in queue '%s'", message, queue)
	return nil
}

// UnacknowledgedMessage represents a message that was not acknowledged
type UnacknowledgedMessage struct {
	QueueName  string
	Body       string
	RoutingKey string
}

// LoadUnacknowledgedMessages fetches all messages with status 'pending' for redelivery
func (s *Storage) LoadUnacknowledgedMessages() ([]UnacknowledgedMessage, error) {
	rows, err := s.db.Query("SELECT queue_name, message, routing_key FROM messages WHERE status = 'pending'")
	if err != nil {
		return nil, fmt.Errorf("failed to load unacknowledged messages: %w", err)
	}
	defer rows.Close()

	var messages []UnacknowledgedMessage
	for rows.Next() {
		var msg UnacknowledgedMessage
		if err := rows.Scan(&msg.QueueName, &msg.Body, &msg.RoutingKey); err != nil {
			return nil, fmt.Errorf("failed to scan unacknowledged message: %w", err)
		}
		messages = append(messages, msg)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating unacknowledged messages: %w", err)
	}

	return messages, nil
}

// Close closes the database.
func (s *Storage) Close() error {
	return s.db.Close()
}
