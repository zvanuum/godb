package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
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

	dbServer := NewDBServer(port)
	err := dbServer.Listen()
	if err != nil {
		log.Printf("Failed to start server: %s", err.Error())
		return
	}
}
