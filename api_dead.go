package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/friesencr/go-workers2/storage"
)

const maxDeadPageSize = 1000

func (s *apiServer) Dead(w http.ResponseWriter, req *http.Request) {
	page, pageSize, err := parseDeadURLQuery(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	allDead := []Dead{}
	for _, m := range s.registeredManagers() {
		d, err := m.GetDead(page, pageSize)
		if err != nil {
			s.logger.Error("couldn't retrieve dead jobs for manager", "error", err)
		} else {
			allDead = append(allDead, d)
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(allDead)
}

func (s *apiServer) RequeueDead(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jid := req.URL.Query().Get("jid")
	if jid == "" {
		http.Error(w, "jid is required", http.StatusBadRequest)
		return
	}

	for _, m := range s.registeredManagers() {
		err := m.RequeueDead(jid)
		if err == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err != storage.NoMessage {
			s.logger.Error("couldn't requeue dead job", "jid", jid, "error", err)
			http.Error(w, "couldn't requeue dead job", http.StatusInternalServerError)
			return
		}
	}

	http.Error(w, "dead job not found", http.StatusNotFound)
}

// Dead stores a page of dead jobs
type Dead struct {
	TotalDeadCount int64  `json:"total_dead_count"`
	DeadJobs       []*Msg `json:"dead_jobs"`
}

func parseDeadURLQuery(req *http.Request) (uint64, int64, error) {
	var page uint64
	if p := req.URL.Query().Get("page"); len(p) > 0 {
		pageVal, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid page %q", p)
		}
		page = pageVal
	}

	pageSize := int64(10)
	if ps := req.URL.Query().Get("page_size"); len(ps) > 0 {
		pageSizeVal, err := strconv.ParseInt(ps, 10, 64)
		if err != nil || pageSizeVal <= 0 || pageSizeVal > maxDeadPageSize {
			return 0, 0, fmt.Errorf("page_size must be between 1 and %d", maxDeadPageSize)
		}
		pageSize = pageSizeVal
	}

	return page, pageSize, nil
}
