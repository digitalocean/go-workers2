package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func (s *apiServer) Retries(w http.ResponseWriter, req *http.Request) {
	page, pageSizeVal, query, err := parseURLQuery(req)
	if err != nil {
		Logger.Println("couldn't retrieve retries filtering query:", err)
	}

	allRetries := []Retries{}
	for _, m := range s.managers {
		r, err := m.GetRetries(page, pageSizeVal, query)
		if err != nil {
			Logger.Println("couldn't retrieve retries for manager:", err)
		} else {
			allRetries = append(allRetries, r)
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(allRetries)
}

// Retries stores retry information
type Retries struct {
	TotalRetryCount int64           `json:"total_retry_count"`
	RetryJobs       []RetryJobStats `json:"retry_jobs"`
}

// RetryJobStats stores information about a single retry job
type RetryJobStats struct {
	Class        string `json:"class"`
	ErrorMessage string `json:"error_message"`
	FailedAt     string `json:"failed_at"`
	JobID        string `json:"jid"`
	Queue        string `json:"queue"`
	RetryCount   int64  `json:"retry_count"`
}

func parseURLQuery(req *http.Request) (uint64, int64, string, error) {
	query := req.URL.Query().Get("q")
	if len(query) > 0 {
		query = fmt.Sprintf("*" + query + "*")
	} else {
		return 0, 10, query, nil
	}

	var pageVal uint64
	page := req.URL.Query().Get("page")
	if len(page) > 0 {
		pageVal, err := strconv.ParseUint(page, 10, 64)
		if err != nil {
			return pageVal, 10, query, nil
		}
	} else {
		return 0, 10, query, nil
	}

	var pageSizeVal int64
	pageSize := req.URL.Query().Get("page_size")
	if len(pageSize) > 0 {
		pageSizeVal, err := strconv.ParseInt(pageSize, 10, 64)
		if err != nil {
			return pageVal, pageSizeVal, query, nil
		}
	} else {
		return pageVal, 10, query, nil
	}

	return pageVal, pageSizeVal, query, nil
}
