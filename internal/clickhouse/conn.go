package clickhouse

import (
	// "database/sql"
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouse syntax reference
// "Non-quoted identifiers must match the regex"
var clickHouseIdentifier = regexp.MustCompile(`^[a-zA-Z_][0-9a-zA-Z_.]*$`)

type ClickHouseAdapter struct {
	// NOTE: We switched to sql.DB, but clickhouse.Conn appears to handle
	// PrepareBatch and Query correctly with multiple goroutines, despite
	// technically being a "driver.Conn"
	// db              *sql.DB
	// conn clickhouse.Conn
	db              clickhouse.Conn
	databse_name    string
	table           string
	readIgnoreLabel string
	readIgnoreHints bool
}

type Config struct {
	Address  string
	Database string
	Username string
	Password string
	Table    string

	ReadIgnoreLabel string
	ReadIgnoreHints bool

	Debug bool
}

// func NewClickHouseAdapter(config *Config) (*ClickHouseAdapter, error) {
// 	if !clickHouseIdentifier.MatchString(config.Table) {
// 		return nil, fmt.Errorf("invalid table name: use non-quoted identifier")
// 	}
// 	db, err := clickhouse.Open(&clickhouse.Options{
// 		Addr: []string{config.Address},
// 		Auth: clickhouse.Auth{
// 			Database: config.Database,
// 			Username: config.Username,
// 			Password: config.Password,
// 		},
// 		Debug:       config.Debug,
// 		DialTimeout: 5 * time.Second,
// 		Protocol:    clickhouse.Native,
// 				MaxOpenConns:    16,
// 		MaxIdleConns:    1,
// 		ConnMaxLifetime: time.Hour,
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("error connecting to ClickHouse: %w", err)
// 	}

// 	// db := clickhouse.OpenDB(&clickhouse.Options{
// 	// 	Addr: []string{config.Address},
// 	// 	Auth: clickhouse.Auth{
// 	// 		Database: config.Database,
// 	// 		Username: config.Username,
// 	// 		Password: config.Password,
// 	// 	},
// 	// 	Debug:       config.Debug,
// 	// 	DialTimeout: 5 * time.Second,
// 	// 	//MaxOpenConns:    16,
// 	// 	//MaxIdleConns:    1,
// 	// 	//ConnMaxLifetime: time.Hour,
// 	// })
// 	// db.SetMaxOpenConns(16)
// 	// db.SetMaxIdleConns(1)
// 	// db.SetConnMaxLifetime(time.Hour)

// 	// Immediately try to connect with the provided credentials, fail fast.
// 	if err := db.Ping(); err != nil {
// 		return nil, fmt.Errorf("unable to connect to clickhouse server: %w", err)
// 	}
// 	return &ClickHouseAdapter{
// 		db:              db,
// 		databse_name:    config.Database,
// 		table:           config.Table,
// 		readIgnoreLabel: config.ReadIgnoreLabel,
// 		readIgnoreHints: config.ReadIgnoreHints}, nil
// 	// return &ClickHouseAdapter{
// 	// 	db:              db,
// 	// 	databse_name:    config.Database,
// 	// 	table:           config.Table,
// 	// 	readIgnoreLabel: config.ReadIgnoreLabel,
// 	// 	readIgnoreHints: config.ReadIgnoreHints,
// 	// }, nil
// }

func NewClickHouseAdapter(config *Config) (*ClickHouseAdapter, error) {
	if !clickHouseIdentifier.MatchString(config.Table) {
		return nil, fmt.Errorf("invalid table name: use non-quoted identifier")
	}
	db, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Address},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Debug:           config.Debug,
		DialTimeout:     10 * time.Second,
		Protocol:        clickhouse.Native,
		MaxOpenConns:    64,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("error connecting to ClickHouse: %w", err)
	}

	if err := db.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to connect to clickhouse server: %w", err)
	}

	return &ClickHouseAdapter{
		db:              db,
		databse_name:    config.Database,
		table:           config.Table,
		readIgnoreLabel: config.ReadIgnoreLabel,
		readIgnoreHints: config.ReadIgnoreHints,
	}, nil
}
