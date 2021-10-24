package main

import (
	"log"

	"github.com/dodafin/kafkaesque/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
