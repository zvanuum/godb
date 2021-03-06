package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	arguments := os.Args
	var port int
	if len(arguments) == 1 {
		port = 8888
	} else {
		converted, err := strconv.Atoi(arguments[1])
		if err != nil {
			fmt.Printf("%s is not a number\n", arguments[1])
			return
		}

		port = converted
	}

	dbServer, err := NewDatabaseServer(port)
	if err != nil {
		log.Printf("There was an error creating the database: %s", err.Error())
		return
	}

	done := make(chan bool, 1)
	go listenForExit(dbServer, done)

	go func() {
		err = dbServer.Listen()
		if err != nil {
			log.Printf("Failed to start server: %s", err.Error())
			return
		}
	}()

	<-done
	log.Printf("Closing application")
}

func listenForExit(server DatabaseServer, done chan bool) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		server.Close()
		done <- true
	}()
}
