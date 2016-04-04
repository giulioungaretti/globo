// Package counts handles a multipolygon requests
// and returns the event counts inside the polygon
// in the current time range.
package counts

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/giulioungaretti/geo/s2"
	"github.com/giulioungaretti/globo/geoJSON"
	// use postgres driver
	_ "github.com/giulioungaretti/pq"
)

const (
	dbUser     = "giulio"
	dbPassword = "postgres"
	dbName     = "postgres"
	// Endpoint is  the name of the counts endpoint
	Endpoint = "/v1/counts/"
)

type dbHelper struct {
	db       *sql.DB
	queryAll *sql.Stmt
	queryID  *sql.Stmt
}

var err error
var db dbHelper
var privateConunter uint64

type row struct {
	value  uint64
	cellid uint64
}

func checker(c, t *uint64, done chan struct{}) {
	for {
		time.Sleep(10 * time.Microsecond)
		if atomic.LoadUint64(c) == atomic.LoadUint64(t) {
			log.Debugf("⚡️  done")
			close(done)
			return
		}
	}
}

func processor(ch chan row, count, processed *uint64, loop s2.Loop, bound s2.Rect) {
	for row := range ch {
		if containsCell(loop, bound, row.cellid) {
			atomic.AddUint64(count, row.value)
		}
		atomic.AddUint64(processed, 1)
	}
}

func init() {
	db, err = connect()
	if err != nil {
		log.Fatal(err)
	}
}

func connect() (db dbHelper, err error) {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		dbUser, dbPassword, dbName)
	dbo, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return
	}
	err = dbo.Ping()
	if err != nil {
		return
	}
	db.db = dbo
	stmt, err := dbo.Prepare(`
			SELECT sum(count), s2cellid
			FROM data
			WHERE
				daterange($1::date, $2::date, '[]')
				@> day
			AND
				numrange($3, $4)
				@>s2cellid
			GROUP BY
				s2cellid
	`)
	if err != nil {
		return
	}
	db.queryAll = stmt
	stmt, err = dbo.Prepare(`
	SELECT sum(count), s2cellid
	FROM data
	WHERE ID = $1
	AND
		daterange($2::date, $3::date, '[]')
		@> day
	AND
		numrange($4, $5)
		@>s2cellid
	GROUP BY
		s2cellid
	`)
	if err != nil {
		return
	}
	db.queryID = stmt
	return
}

func query(db dbHelper, ID int, minID, maxID uint64, start string, end string) (rows *sql.Rows, err error) {
	if ID == 0 {
		rows, err = db.queryAll.Query(start, end, minID, maxID)
		return rows, err
	}
	rows, err = db.queryID.Query(ID, start, end, minID, maxID)
	return
}

type res struct {
	loopid int
	res    uint64
}

//HandleSerial takes care of the request
func HandleSerial(w http.ResponseWriter, r *http.Request) {

	// parsq query
	values := r.URL.Query()
	var start string
	var ID int
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
	cellUnions, loops, err := resp.ToS2(precision)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results := make(map[string]uint64)
	t := time.Now()

	for i, ids := range cellUnions {
		min := ids[0]
		max := ids[len(ids)-1]
		rows, err := query(db, ID, min, max, start, end)
		if err != nil {
			log.Error(err)
			log.Debug(ID, min, max, nil, start, end)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			continue
		}
		var rowsnumber uint64
		// get loop
		loop := loops[i]
		// get bound
		bound := loop.RectBound()
		// channels
		var processed uint64
		var count uint64
		for rows.Next() {
			var row row
			if err := rows.Scan(&row.value, &row.cellid); err != nil {
				log.Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				break
			} else {
				if containsCell(loop, bound, row.cellid) {
					atomic.AddUint64(&count, row.value)
				}
				atomic.AddUint64(&processed, 1)
			}
		}
		results[fmt.Sprint(i)] = count
		log.Debugf("total row recieved:%v", rowsnumber)
		log.Debugf("total  recieved:%v", results)
		log.Debugf("total  recieved:%v", atomic.LoadUint64(&privateConunter))
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

//Handler takes care of the request
func Handler(w http.ResponseWriter, r *http.Request) {

	workers := runtime.NumCPU()
	// parsq query
	values := r.URL.Query()
	var start string
	var ID int
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
	cellUnions, loops, err := resp.ToS2(precision)
	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results := make(map[string]uint64)
	t := time.Now()
loop:

	for i, ids := range cellUnions {
		min := ids[0]
		max := ids[len(ids)-1]
		rows, err := query(db, ID, min, max, start, end)
		if err != nil {
			log.Error(err)
			log.Debug(ID, min, max, nil, start, end)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			continue
		}
		var rowsnumber uint64
		// get loop
		loop := loops[i]
		// get bound
		bound := loop.RectBound()
		// channels
		var processed uint64
		var count uint64
		ch := make(chan row, 1000)
		done := make(chan struct{})

		for i := 0; i < workers; i++ {
			go processor(ch, &count, &processed, loop, bound)
		}
		for rows.Next() {
			var row row
			if err := rows.Scan(&row.value, &row.cellid); err != nil {
				log.Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				break loop
			} else {
				ch <- row
				atomic.AddUint64(&rowsnumber, 1)
			}
		}
		go checker(&processed, &rowsnumber, done)
		log.Debugf("🚥")
		<-done
		results[fmt.Sprint(i)] = count
		log.Debugf("total row recieved:%v", rowsnumber)
		log.Debugf("total  recieved:%v", results)
		log.Debugf("total  recieved:%v", atomic.LoadUint64(&privateConunter))
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

func containsCell(l s2.Loop, bound s2.Rect, cellid uint64) (res bool) {
	// fast check: if cell not in bounding rect
	// return false
	id := s2.CellID(cellid)
	cell := s2.CellFromCellID(id)
	cellBound := cell.RectBound()
	if !bound.Contains(cellBound) {
		return false
	}
	ll := id.LatLng()
	if l.ContainsPoint(s2.PointFromLatLng(ll)) {
		atomic.AddUint64(&privateConunter, 1)
		return true
	}
	return res
}
