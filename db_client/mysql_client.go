package db_client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	_ "github.com/go-sql-driver/mysql"
)

// MysqlWrite is a Writer that writes to a mysql server.
type MysqlClient struct {
	DB *sql.DB
	c  common.ClientConfig
}

// NewMysqlClient returns a new DBClient of Mysql .
func NewMysqlClient(c common.ClientConfig) (*MysqlClient, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s?multiStatements=true&charset=utf8", c.User, c.Password, "tcp", c.Host, c.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(0) //最大连接周期，超过时间的连接就close
	db.SetMaxOpenConns(100)  //设置最大连接数
	db.SetMaxIdleConns(100)  //设置闲置连接数
	return &MysqlClient{
		DB: db,
		c:  c,
	}, nil
}

func (c *MysqlClient) Close() {
	c.DB.Close()
}

func (c *MysqlClient) Write(body []byte) (int64, error) {
	conn, err := c.DB.Conn(context.Background())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	sql := string(body)
	startTime := time.Now()
	_, err = conn.ExecContext(context.Background(), sql)
	executeTime := time.Since(startTime).Nanoseconds()
	return executeTime, err
}

func (c *MysqlClient) Query(lines []byte) (int64, error) {
	conn, err := c.DB.Conn(context.Background())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	sql := string(lines)
	startTime := time.Now()
	rows, err := conn.QueryContext(context.Background(), sql)
	rows.Close()
	executeTime := time.Since(startTime).Nanoseconds()
	return executeTime, err
}

func (c *MysqlClient) CreateDb(withEncryption bool) error {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/", c.c.User, c.c.Password, "tcp", c.c.Host)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if withEncryption {
		return errors.New("mysql version do not support the encryption option")
	}
	createDbSql := fmt.Sprintf("create database %s;", c.c.Database)
	_, err = db.Exec(createDbSql)
	return err
}

func (c *MysqlClient) ListDatabases() ([]string, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/", c.c.User, c.c.Password, "tcp", c.c.Host)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return []string{}, err
	}
	defer db.Close()
	showDatabaseSql := "show databases;"
	rows, err := db.Query(showDatabaseSql)
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()
	databases := make([]string, 0)
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			return nil, err
		}
		databases = append(databases, database)
	}
	return databases, nil
}

func (c *MysqlClient) Ping() error {
	_, err := net.DialTimeout("tcp", c.c.Host, 5*time.Second)
	return err
}
