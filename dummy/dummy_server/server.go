package main

import (
	"log/slog"
	"net/http"
)

type NotifyRequest struct {
	Name string
}

func main() {
	addr := ":9000"
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		if name == "" {
			http.Error(w, "empty params parameters", 400)
			return
		}

		//var req NotifyRequest

		//err := json.Unmarshal([]byte(params), &req)
		//if err != nil {
		//	http.Error(w, err.Error(), 400)
		//	return
		//}

		slog.Info("notifying", slog.String("name", name))
	})

	http.ListenAndServe(addr, nil)
}
