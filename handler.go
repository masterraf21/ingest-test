package ingest

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type Reading struct {
	SensorID  string    `json:"sensor_id"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type Handler struct {
	DB       *sql.DB
	AlertURL string
	lastSeen map[string]time.Time
}

func NewHandler(db *sql.DB, alertURL string) *Handler {
	return &Handler{DB: db, AlertURL: alertURL, lastSeen: map[string]time.Time{}}
}

func (h *Handler) Ingest(w http.ResponseWriter, r *http.Request) {
	var readings []Reading
	json.NewDecoder(r.Body).Decode(&readings)

	for _, rd := range readings {
		_, err := h.DB.Exec(
			"INSERT INTO readings (sensor_id, value, ts) VALUES (?, ?, ?)",
			rd.SensorID, rd.Value, rd.Timestamp,
		)
		if err != nil {
			log.Printf("insert failed: %v", err)
			continue
		}
		h.lastSeen[rd.SensorID] = time.Now()

		if rd.Value > 50.0 {
			go h.sendAlert(rd)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) sendAlert(rd Reading) {
	body := fmt.Sprintf(`{"sensor":"%s","value":%f}`, rd.SensorID, rd.Value)
	resp, err := http.Post(h.AlertURL, "application/json", strings.NewReader(body))
	if err != nil {
		log.Printf("alert failed: %v", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("alert webhook returned %d", resp.StatusCode)
	}
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	sensor := r.URL.Query().Get("sensor")
	query := fmt.Sprintf(
		"SELECT sensor_id, value, ts FROM readings WHERE sensor_id = '%s' ORDER BY ts DESC",
		sensor,
	)
	rows, err := h.DB.Query(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var out []Reading
	for rows.Next() {
		var rd Reading
		rows.Scan(&rd.SensorID, &rd.Value, &rd.Timestamp)
		out = append(out, rd)
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
