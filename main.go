package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	uuid "github.com/satori/go.uuid"
)

var (
	t       = time.Unix(1000000, 0)
	entropy = ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)

	events = []string{
		"purchase",
		"pageview",
		"comment",
	}

	eventProperties = map[string][]string{
		"item":   []string{"t-shirt", "leash", "dog collar", "cat collar", "dog toy", "cat toy"},
		"color":  []string{"red", "blue", "green", "black", "white"},
		"region": []string{"north america", "south america", "asia", "europe", "africa"},
	}
)

type Singlestore struct {
	*sqlx.DB
}

type SinglestoreConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

func randomEvent(source *rand.Rand) (string, map[string]string) {
	evt := events[source.Intn(len(events))]
	props := make(map[string]string)
	for k, v := range eventProperties {
		if source.Float32() < 0.75 {
			props[k] = v[source.Intn(len(v))]
		}
	}
	return evt, props
}

func NewSinglestore(config SinglestoreConfig) (*Singlestore, error) {
	// We use NewConfig here to set default values. Then we override what we need to.
	mysqlConf := mysql.NewConfig()
	mysqlConf.User = config.Username
	mysqlConf.Passwd = config.Password
	mysqlConf.DBName = config.Database
	mysqlConf.Addr = fmt.Sprintf("%s:%d", config.Host, config.Port)
	mysqlConf.ParseTime = true
	mysqlConf.Timeout = 10 * time.Second
	mysqlConf.InterpolateParams = true
	mysqlConf.AllowNativePasswords = true
	mysqlConf.MultiStatements = false

	mysqlConf.Params = map[string]string{
		"collation_server":    "utf8_general_ci",
		"sql_select_limit":    "18446744073709551615",
		"compile_only":        "false",
		"enable_auto_profile": "false",
		"sql_mode":            "'STRICT_ALL_TABLES'",
	}

	connector, err := mysql.NewConnector(mysqlConf)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	db.SetConnMaxLifetime(time.Hour)
	db.SetMaxIdleConns(20)

	return &Singlestore{DB: sqlx.NewDb(db, "mysql")}, nil
}

type ZeroReader struct{}

func (z *ZeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

type Clock struct {
	t time.Time
	r *rand.Rand
}

func (c *Clock) Now() time.Time {
	c.t = c.t.Add(time.Duration(c.r.Intn(200)) * time.Millisecond)
	return c.t
}

func tsToULID(t time.Time) ulid.ULID {
	return ulid.MustNew(ulid.Timestamp(t), &ZeroReader{})
}

func TrackULID(db *Singlestore, clk *Clock, source *rand.Rand) {
	ts := clk.Now()
	ulid := ulid.MustNew(ulid.Timestamp(ts.UTC()), entropy)
	evt, properties := randomEvent(source)
	tx := db.MustBegin()

	_, err := tx.Exec("insert into analytics_ulid.events values (?, ?, ?)", ts, ulid, evt)
	if err != nil {
		panic(err)
	}

	for k, v := range properties {
		_, err = tx.Exec("insert into analytics_ulid.properties values (?, ?, ?)", ulid.String(), k, v)
		if err != nil {
			panic(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}

func Track(db *Singlestore, clk *Clock, source *rand.Rand) {
	ts := clk.Now()
	uuid := uuid.NewV4()
	evt, properties := randomEvent(source)
	tx := db.MustBegin()

	_, err := tx.Exec("insert into analytics_twocol.events values (?, ?, ?)", ts, uuid, evt)
	if err != nil {
		panic(err)
	}

	for k, v := range properties {
		_, err = tx.Exec("insert into analytics_twocol.properties values (?, ?, ?, ?)", ts, uuid, k, v)
		if err != nil {
			panic(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}

func main() {
	s2, err := NewSinglestore(SinglestoreConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "root",
		Database: "analytics_ulid",
	})
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			source := rand.New(rand.NewSource(t.UnixNano()))
			clock := &Clock{t: time.Now(), r: source}
			for i := 0; i < 2000000; i++ {
				Track(s2, clock, source)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
