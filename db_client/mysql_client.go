package db_client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"git.querycap.com/falcontsdb/fctsdb-bench/data_generator/common"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// MysqlWrite is a Writer that writes to a mysql server.
type MysqlClient struct {
	DB *sql.DB
	c  ClientConfig
}

// NewMysqlClient returns a new DBClient of Mysql .
func NewMysqlClient(c ClientConfig) (*MysqlClient, error) {
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

func (m *MysqlClient) Close() {
	m.DB.Close()
}

func (m *MysqlClient) Write(body []byte) (int64, error) {
	conn, err := m.DB.Conn(context.Background())
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

func (m *MysqlClient) Query(lines []byte) (int64, error) {
	conn, err := m.DB.Conn(context.Background())
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

func (m *MysqlClient) InitUser() error {
	return nil
}

func (m *MysqlClient) LoginUser() error {
	return nil
}

func (m *MysqlClient) CreateDatabase(name string, withEncryption bool) error {

	log.Infof("create database %s", name)
	existingDatabases, err := m.listDatabases()
	if err != nil {
		return err
	}

	for _, existingDatabase := range existingDatabases {
		if name == existingDatabase {
			log.Warn("The following database \"%s\" already exist in the data store, do'not need create.", name)
			return nil
		}
	}

	if name == "" {
		name = m.c.Database
	}
	dsn := fmt.Sprintf("%s:%s@%s(%s)/", m.c.User, m.c.Password, "tcp", m.c.Host)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if withEncryption {
		return errors.New("mysql version do not support the encryption option")
	}
	createDbSql := fmt.Sprintf("create database %s;", name)
	_, err = db.Exec(createDbSql)
	return err
}

func (m *MysqlClient) listDatabases() ([]string, error) {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/", m.c.User, m.c.Password, "tcp", m.c.Host)
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

func (m *MysqlClient) CheckConnection(timeout time.Duration) bool {
	endTime := time.Now().Add(timeout)
	log.Info("checking connection ")
	fmt.Print("checking .")
	defer fmt.Println()
	for time.Now().Before(endTime) {
		_, err := net.DialTimeout("tcp", m.c.Host, 5*time.Second)
		if err == nil {
			return true
		}
		time.Sleep(2 * time.Second)
		fmt.Print(".")
	}
	return false
}

func (m *MysqlClient) CreateMeasurement(p *common.Point) error {
	// buf := scratchBufPool.Get().([]byte)
	buf := make([]byte, 0, 4*1024)
	buf = append(buf, "create table IF NOT EXISTS "...)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, " ("...)

	// add the timestamp
	buf = append(buf, "time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP"...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, " char(64) NOT NULL DEFAULT ''"...)
	}
	buf = append(buf, ',')

	var i int
	for i = 0; i < len(p.FieldKeys); i++ {
		k := p.FieldKeys[i]
		v := p.FieldValues[i]
		buf = append(buf, k...)
		switch v.(type) {
		case int, int64:
			buf = append(buf, " bigint"...)
		case float64, float32:
			buf = append(buf, " double"...)
		case []byte:
			buf = append(buf, " char(64)"...)
		case string:
			buf = append(buf, " char(64)"...)
		case bool:
			//mysql不支持bool，一般使用tinyint(1)来存储，这里使用char是因为这样就不需要修改fastFormatAppend函数
			buf = append(buf, " char(64)"...)
		default:
			panic(fmt.Sprintf("unknown field type for %#v", v))
		}
		buf = append(buf, ',')
	}

	for i = 0; i < len(p.Int64FiledKeys); i++ {
		buf = append(buf, p.Int64FiledKeys[i]...)
		buf = append(buf, " bigint"...)
		buf = append(buf, ',')
	}
	buf = append(buf, "PRIMARY KEY pk_name_gender_ctime(time,"...)
	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, p.TagKeys[i]...)
		if i+1 < len(p.TagKeys) {
			buf = append(buf, ',')
		}
	}

	buf = append(buf, ")"...)
	buf = append(buf, ");"...)

	// 写入表
	_, err := m.Write(buf)
	return err

}

func (m *MysqlClient) BeforeSerializePoints(buf []byte, p *common.Point) []byte {
	buf = append(buf, "insert into "...)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, " values"...)
	return buf
}

// insert into table values ( "xxx","xxx")
func (s *MysqlClient) SerializeAndAppendPoint(buf []byte, p *common.Point) []byte {
	// buf := scratchBufPool.Get().([]byte)
	// buf := make([]byte, 0, 4*1024)
	//buf = append(buf, "insert into "...)
	//buf = append(buf, p.MeasurementName...)
	buf = append(buf, "("...)

	// add the timestamp
	buf = append(buf, '"')
	buf = append(buf, p.Timestamp.Format("2006-01-02 15:04:05.000")...)
	buf = append(buf, '"')

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ',')
		buf = append(buf, '"')
		buf = append(buf, p.TagValues[i]...)
		buf = append(buf, '"')
	}
	buf = append(buf, ',')

	var i int
	for i = 0; i < len(p.FieldKeys); i++ {
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, false)
		if i+1 < len(p.FieldKeys) || len(p.Int64FiledKeys) != 0 {
			buf = append(buf, ',')
		}
	}

	for i = 0; i < len(p.Int64FiledKeys); i++ {
		v := p.Int64FiledValues[i]
		buf = strconv.AppendInt(buf, v, 10)
		if i+1 < len(p.Int64FiledKeys) {
			buf = append(buf, ',')
		}
	}
	buf = append(buf, ")"...)
	buf = append(buf, ',')

	return buf
}

func (m *MysqlClient) AfterSerializePoints(buf []byte, p *common.Point) []byte {
	buf = buf[:len(buf)-1]
	return append(buf, ';')
}
