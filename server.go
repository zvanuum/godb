package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

const (
	HELP = "Commands: \n" +
		"\tGET: String key -> takes a string as key, returns value at given key if it exists otherwise an empty string\n" +
		"\tSET: String key, String value -> Maps key to value (delimitted by spaces). Returns OK or an error\n" +
		"\tDEL: String key -> Deletes value at key and return OK or an error\n" +
		"\tBEGIN: Starts transaction, all following commands will be specific to that transaction\n" +
		"\tCOMMIT: End transaction, applies all commands in transaction\n" +
		"\tQUIT: Closes connection\n"
	TIMEOUT = 60 * time.Second
	CLOSING = "Closing\n"

	GET    = "GET"
	SET    = "SET"
	DEL    = "DEL"
	BEGIN  = "BEGIN"
	COMMIT = "COMMIT"
	QUIT   = "QUIT"
)

type DBServer interface {
	Listen() error
	Close() error
}

type db struct {
	port   int
	repo   Repository
	server net.Listener
}

func NewDBServer(port int) DBServer {
	log.Printf("Initializing DB")

	// load from fisk here?
	repo := NewRepository()

	return &db{
		port:   port,
		repo:   repo,
		server: nil,
	}
}

func (db *db) Listen() error {
	server, err := net.Listen("tcp4", fmt.Sprintf(":%d", db.port))
	if err != nil {
		return err
	}

	db.server = server

	defer db.server.Close()
	log.Printf("Listening on port %s\n", db.port)

	// go db.acceptConnections()
	db.acceptConnections()

	return nil
}

func (db *db) Close() error {
	if err := db.server.Close(); err != nil {
		return err
	}

	return nil
}

func (db *db) acceptConnections() {
	for {
		c, err := db.server.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s", err.Error())
			return
		}
		c.SetReadDeadline(time.Now().Add(TIMEOUT))

		go db.handleConnection(c)
	}
}

func (db *db) handleConnection(c net.Conn) {
	writeMessage(c, HELP)

	log.Printf("Serving %s\n", c.RemoteAddr().String())
	scanner := bufio.NewScanner(c)
	scanner.Split(ScanCRLF)

	for scanner.Scan() {
		data := scanner.Text()

		strData := string(data)
		if strData == QUIT {
			c.Write([]byte(string(CLOSING)))
			break
		} else if len(strData) == 0 {
			continue
		}

		command, rest, err := parseCommand(strData)
		var response string
		if err != nil {
			response = err.Error()
		} else {
			response = rest
		}

		log.Println(command)
		writeMessage(c, response)
	}

	c.Close()
}

func writeMessage(c net.Conn, message string) {
	if _, err := c.Write([]byte(message)); err != nil {
		log.Printf("Failed to write to connection: %s", err.Error())
	}
}

func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
		if data[i] == '\n' {
			return i + 1, data[0:i], nil
		}

		advance = i + 1

		if len(data) > i+1 && data[i+1] == '\n' {
			advance += 1
		}

		return advance, data[0:i], nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return 0, nil, nil
}

func parseCommand(input string) (string, string, error) {
	firstSpace := strings.Index(input, " ")
	if firstSpace == -1 {
		return "", "", errors.New("Invalid input. Please specify a command.\n")
	}

	return input[:firstSpace], input[firstSpace+1:], nil
}
