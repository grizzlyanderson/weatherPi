package main

import (
	//"database/sql"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	//"syscall"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

const stationID = "165461071a9b4f40bb8325924aa45cbb"
const timeFormat = "2006-01-02 15:04:05"

// set up rollup table name enumeration & String to get proper name
type rollUpTable int

const (
	undef  = iota
	minute = iota
	hour   = iota
	day    = iota
)

func (r rollUpTable) String() string {
	switch r {
	case minute:
		return "envDataMinute"
	case hour:
		return "envDataHours"
	case day:
		return "envDataDays"
	case undef:
		return ""
	default:
		return ""
	}
}

// Temp provides temperature reading values.
// Period of roll-up (minute, hour, day) specified in parent Measurement
// Values given in degrees Fahrenheit
type Temp struct {
	maxF     float64 `json:"max,omitempty"`
	minF     float64 `json:"min,omitempty"`
	averageF float64 `json:"average,omitempty"`
	weight   int     `json:"weight,omitempty"`
}

// Press provides air pressure values.
// Period of roll-up (minute, hour, day) speficied in parent Measurement
// Values are given in hPa
type Press struct {
	maxhPa     float64 `json:"max,omitempty"`
	minhPa     float64 `json:"min,omitempty"`
	averagehPa float64 `json:"average,omitempty"`
	weight     int     `json:"weight,omitempty"`
}

// Humid provides humidity reading values.
// Period of roll-up (minute, hour, day) speficied in parent Measurement
// Values are given in as percentage
type Humid struct {
	max     float64 `json:"max,omitempty"`
	min     float64 `json:"min,omitempty"`
	average float64 `json:"average,omitempty"`
	weight  int     `json:"weight,omitempty"`
}

// Wind - not supported - present for API contract/future
type Wind struct {
	degree int     `json:"degree,omitempty"`
	speed  float64 `json:"speed,omitempty"`
}

// Precip - not supported - present for API contract/future use
type Precip struct {
	rain1h  float64 `json:"rain_1h,omitempty"`
	rain6h  float64 `json:"rain_6h,omitempty"`
	rain24h float64 `json:"rain_24h,omitempty"`
}

// Measurement provides weather information for a particular station durring a particular time period (minute, hour, day)
// date is the Unix epoch value for the START of the minute/hour/day.
// the values are for that m/h/d, not the previous one
// wind and precip are not yet supported
type Measurement struct {
	rollup    string `json:"type"`
	timeOf    int64  `json:"date"`
	stationID string `json:"station_id"`
	temp      Temp   `json:"temp"`
	humidity  Humid  `json:"humidity"`
	wind      Wind   `json:"wind"`
	pressure  Press  `json:"pressure"`
	precip    Precip `json:"precipittion"`
}

// Person - from example code
type Person struct {
	ID        string   `json:"id,omitempty"`
	Firstname string   `json:"firstname,omitempty"`
	Lastname  string   `json:"lastname,omitempty"`
	Address   *Address `json:"address,omitempty"`
}

// Address - from example code
type Address struct {
	City  string `json:"city,omitempty"`
	State string `json:"state,omitempty"`
}

var people []Person

// GetPersonEndpoint - from example code
func GetPersonEndpoint(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	for _, item := range people {
		if item.ID == params["id"] {
			json.NewEncoder(w).Encode(item)
			return
		}
	}
	json.NewEncoder(w).Encode(&Person{})
}

// GetPeopleEndpoint - from example code blah blah
func GetPeopleEndpoint(w http.ResponseWriter, req *http.Request) {
	json.NewEncoder(w).Encode(people)
}

// CreatePersonEndpoint - from example code
func CreatePersonEndpoint(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	var person Person
	_ = json.NewDecoder(req.Body).Decode(&person)
	person.ID = params["id"]
	people = append(people, person)
	json.NewEncoder(w).Encode(people)
}

// DeletePersonEndpoint - from example code
func DeletePersonEndpoint(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	for index, item := range people {
		if item.ID == params["id"] {
			people = append(people[:index], people[index+1:]...)
			break
		}
	}
	json.NewEncoder(w).Encode(people)
}

func getMeasurementsEndpoint(w http.ResponseWriter, req *http.Request) {
	qs := req.URL.Query()

	var rt rollUpTable = undef
	// if for any reason there is more than one QS param named "type" we're blowing off all subsequent values
	// TODO should probably make an error response if more than one...
	switch len(qs["type"]) {
	case 0:
		http.Error(w, "Request parameter 'type' is required!", http.StatusBadRequest)
		return
	case 1:
		switch qs["type"][0] {
		case "m":
			rt = minute
		case "h":
			rt = hour
		case "d":
			rt = day
		case "":
			rt = undef
		default:
			http.Error(w, fmt.Sprint("Invalid request parameter type=%s!", qs.Get("type")), http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "Only 1 '?type' allowed!", http.StatusBadRequest)
		return
	}

	print(qs["limit"])
	limit, err := strconv.ParseInt(qs["limit"][0], 10, 0)
	if err != nil || limit <= 0 {
		http.Error(w, "Request parameter limit required and must be > 0!", http.StatusBadRequest)
		return
	}

	rows, err := getRollUpRecords(rt, limit)
	defer rows.Close()
	if err != nil {
		http.Error(w, "A problem occurred reading weather information", http.StatusInternalServerError)
		return
	}
	var measurements []Measurement
	var ta, tmin, tmax, ha, hmin, hmax, pa, pmin, pmax float64
	var timestamp string
	for rows.Next() {
		err := rows.Scan(&timestamp, &ta, &tmin, &tmax, &ha, &hmin, &hmax, &pa, &pmin, &pmax)
		if err != nil {
			log.Fatal(err)
			http.Error(w, "A problem occurred reading weather information", http.StatusInternalServerError)
			return
		}
		t, _ := time.Parse(timeFormat, timestamp)
		measurements = append(measurements, Measurement{
			rollup:    rt.String(),
			timeOf:    t.Unix(), // TODO needs to be converted to epoch
			stationID: stationID,
			temp: Temp{
				maxF:     tmax,
				minF:     tmin,
				averageF: ta},
			humidity: Humid{
				max:     hmax,
				min:     hmin,
				average: ha},
			wind: Wind{},
			pressure: Press{
				maxhPa:     pmax,
				minhPa:     pmin,
				averagehPa: pa},
			precip: Precip{}})
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
		http.Error(w, "A problem occurred reading weather information", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(measurements)
}

func getRollUpRecords(table rollUpTable, maxRows int64) (*sql.Rows, error) {
	db, err := sql.Open("sqlite3", "./foo.db")
	if err != nil {
		log.Fatal(err)
	}

	return db.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY timestamp DESC TOP %s", table.String(), maxRows))
}

func main() {
	router := mux.NewRouter()
	people = append(people, Person{ID: "1", Firstname: "Nic", Lastname: "Raboy", Address: &Address{City: "Dublin", State: "CA"}})
	people = append(people, Person{ID: "2", Firstname: "Maria", Lastname: "Raboy"})
	router.HandleFunc("/people", GetPeopleEndpoint).Methods("GET")
	router.HandleFunc("/people/{id}", GetPersonEndpoint).Methods("GET")
	router.HandleFunc("/people/{id}", CreatePersonEndpoint).Methods("POST")
	router.HandleFunc("/people/{id}", DeletePersonEndpoint).Methods("DELETE")
	router.HandleFunc("/measurements", getMeasurementsEndpoint).Methods("GET")
	log.Fatal(http.ListenAndServe(":12345", router))
}
