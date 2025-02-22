package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type NotifyRequest struct {
    Name string 
}

func main() {
    addr := ":9000" 
    http.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
        params := r.URL.Query().Get("params")    
        if params == "" {
            http.Error(w, "empty params parameters", 400)
            return
        }
        
        var req NotifyRequest

        err := json.Unmarshal([]byte(params), &req)
        if err != nil {
            http.Error(w, err.Error(), 400)
            return
        }
        
        fmt.Printf("notifying %s\n", req.Name)
    })

    http.ListenAndServe(addr, nil)
}
