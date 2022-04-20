package db_client

type ClientConfig struct {
	Host     string
	Database string
	Gzip     bool
	Debug    bool
	User     string
	Password string
	// Debug label for more informative errors.
	DebugInfo string
}

type DBClient interface {
	Write([]byte) (int64, error)
	Query([]byte) (int64, error)
	ListDatabases() ([]string, error)
	CreateDb(string, bool) error
	Ping() error
}
