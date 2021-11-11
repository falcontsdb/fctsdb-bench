package db_client

import (
	"context"
	"database/sql"
	"fmt"
	"git.querycap.com/falcontsdb/fctsdb-bench/common"
	_ "github.com/go-sql-driver/mysql"
	"net"
	"time"
)

// MysqlWrite is a Writer that writes to a mysql server.
type MysqlClient struct {
	DB *sql.DB
	c  common.ClientConfig
}

// NewMysqlClient returns a new DBClient of Mysql .
func NewMysqlClient(c common.ClientConfig) (*MysqlClient, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s?multiStatements=true", c.User, c.Password, "tcp", c.Host, c.Database)
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
	defer conn.Close()
	if err != nil {
		return 0, err
	}
	sql := string(body)
	startTime := time.Now()
	_, err = conn.ExecContext(context.Background(), sql)
	executeTime := time.Since(startTime).Nanoseconds()
	return executeTime, err
}

func (c *MysqlClient) Query(lines []byte) (int64, error) {
	conn, err := c.DB.Conn(context.Background())
	defer conn.Close()
	if err != nil {
		return 0, err
	}
	sql := string(lines)
	startTime := time.Now()
	rows, err := conn.QueryContext(context.Background(), sql)
	rows.Close()
	executeTime := time.Since(startTime).Nanoseconds()
	return executeTime, err
}

func (c *MysqlClient) CreateDb() error {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/", c.c.User, c.c.Password, "tcp", c.c.Host)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
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
	defer rows.Close()
	if err != nil {
		return []string{}, err
	}
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
