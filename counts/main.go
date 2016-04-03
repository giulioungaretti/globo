// Package counts handles a multipolygon requests
// and returns the event counts inside the polygon
// in the current time range.
package counts

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/giulioungaretti/globo/geoJSON"
	// use postgres driver
	_ "github.com/lib/pq"
)

const (
	dbUser     = "giulio"
	dbPassword = "postgres"
	dbName     = "postgres"
	// Endpoint is  the name of the counts endpoint
	Endpoint = "/v1/counts/"
)

var db *sql.DB
var err error

type row struct {
	value  uint64
	cellid uint64
}

func checker(c, t *uint64, done chan struct{}) {
	for {
		time.Sleep(10 * time.Microsecond)
		log.Debugf("⚡️  %v, %v", *c, *t)
		if *c == *t {
			log.Debugf("⚡️  done")
			close(done)
			return
		}
	}
}

func init() {
	db, err = connect()
	if err != nil {
		log.Fatal(err)
	}
}

func connect() (db *sql.DB, err error) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		dbUser, dbPassword, dbName)
	db, err = sql.Open("postgres", dbinfo)
	if err != nil {
		return
	}
	err = db.Ping()
	if err != nil {
		return
	}
	return
}

func query(db *sql.DB, minID, maxID uint64, start string, end string) (row *sql.Row) {
	row = db.QueryRow(`
	SELECT sum(count)
	FROM data
	WHERE
		daterange($1::date, $2::date, '[]')
		@> day
	AND
		numrange($3, $4)
		@>s2cellid
	`, start, end, minID, maxID)
	return
}

type res struct {
	loopid int
	res    uint64
}

//Handler takes care of the request
func Handler(w http.ResponseWriter, r *http.Request) {

	// parsq query
	values := r.URL.Query()
	var start string
	var end string
	var precision int
	var err error
	if p, ok := values["precision"]; ok {
		precision, err = strconv.Atoi(p[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	} else {
		// set max precision
		precision = 30
	}
	if starts, ok := values["start"]; ok {
		start = starts[0]
	} else {
		http.Error(w, "Missing start date", http.StatusBadRequest)
	}
	if ends, ok := values["end"]; ok {
		end = ends[0]
	} else {
		http.Error(w, "Missing end date", http.StatusBadRequest)
	}

	// match geoJSON type
	resp, err := geoJSON.Matcher(r, Endpoint)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// get data from geojson
	cellUnions, _, err := resp.ToS2(precision)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results := make(map[string]uint64)
	t := time.Now()
	for i, ids := range cellUnions {
		var count uint64
		min := ids[0]
		max := ids[len(ids)-1]
		row := query(db, min, max, start, end)
		err := row.Scan(&count)
		if err != nil {
			log.Error(err)
			log.Debug(min, max, nil, start, end)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			continue
		}
		results[fmt.Sprint(i)] = count
	}
	log.Debug(time.Since(t))
	encoder := json.NewEncoder(w)
	w.Header().Set("Content-Type", "application/json")
	err = encoder.Encode(results)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
