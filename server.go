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
	OK      = "OK\n"

	GET    = "GET"
	SET    = "SET"
	DEL    = "DEL"
	BEGIN  = "BEGIN"
	COMMIT = "COMMIT"
	QUIT   = "QUIT"
)

type DatabaseServer interface {
	Listen() error
	Close() error
}

type databaseServer struct {
	port     int
	database Database
	listener net.Listener
}

type databaseServerInstruction struct {
	operation string
	key       string
	value     string
}

func NewDatabaseServer(port int) (DatabaseServer, error) {
	log.Printf("Initializing DB")

	// TODO make some kinda config struct soon
	filename := "./db.db"
	repo, err := NewDatabase(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %s", err.Error())
	}

	return &databaseServer{
		port:     port,
		database: repo,
		listener: nil,
	}, nil
}

func (serv *databaseServer) Listen() error {
	server, err := net.Listen("tcp4", fmt.Sprintf(":%d", serv.port))
	if err != nil {
		return err
	}

	serv.listener = server

	defer serv.listener.Close()
	log.Printf("Listening on port %d\n", serv.port)

	serv.acceptConnections()

	return nil
}

func (serv *databaseServer) Close() error {
	if err := serv.listener.Close(); err != nil {
		return err
	}

	return nil
}

func (serv *databaseServer) acceptConnections() {
	for {
		c, err := serv.listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s", err.Error())
			return
		}
		c.SetReadDeadline(time.Now().Add(TIMEOUT))

		go serv.handleConnection(c)
	}
}

func (serv *databaseServer) handleConnection(c net.Conn) {
	writeMessage(c, HELP)

	log.Printf("Serving %s\n", c.RemoteAddr().String())
	scanner := bufio.NewScanner(c)
	scanner.Split(scanCRLF)

	for scanner.Scan() {
		data := scanner.Text()

		strData := strings.TrimSpace(string(data))
		if strData == QUIT {
			c.Write([]byte(string(CLOSING)))
			break
		} else if len(strData) == 0 {
			continue
		}

		instruction, err := parseInstruction(strData)
		if err != nil {
			writeMessage(c, err.Error())
			continue
		}

		result, err := serv.executeInstruction(instruction)
		if err != nil {
			writeMessage(c, err.Error())
		}

		writeMessage(c, result+"\n")
	}

	c.Close()
}

func writeMessage(c net.Conn, message string) {
	if _, err := c.Write([]byte(message)); err != nil {
		log.Printf("Failed to write to connection: %s", err.Error())
	}
}

func (serv *databaseServer) executeInstruction(instruction *databaseServerInstruction) (string, error) {
	var result string
	var err error

	log.Printf("operation: %s, key: %s, value: %s\n", instruction.operation, instruction.key, instruction.value)

	switch strings.ToUpper(instruction.operation) {
	case GET:
		result, err = serv.database.Get(instruction.key)
	case SET:
		err = serv.database.Set(instruction.key, instruction.value)
		if err == nil {
			result = OK
		}
	case DEL:
		err = serv.database.Delete(instruction.key)
		if err == nil {
			result = OK
		}
	default:
		err = fmt.Errorf("unrecognized operation: %s", instruction.operation)
	}

	return result, err
}

func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
		if data[i] == '\n' {
			return i + 1, data[0:i], nil
		}

		advance = i + 1

		if len(data) > i+1 && data[i+1] == '\n' {
			advance++
		}

		return advance, data[0:i], nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return 0, nil, nil
}

// Need to edit this so that a key can be wrapped in quotes to include a space
func parseInstruction(input string) (*databaseServerInstruction, error) {
	operation, rest := splitOnFirstSpace(input)
	if len(operation) == 0 {
		return nil, errors.New("invalid input, no operation specified.\n")
	}

	key, value := splitOnFirstSpace(rest)
	if len(key) == 0 {
		return nil, errors.New("invalid input, no key was specified.\n")
	}

	return &databaseServerInstruction{
		operation: operation,
		key:       key,
		value:     value,
	}, nil
}

func splitOnFirstSpace(input string) (string, string) {
	firstSpace := strings.Index(input, " ")
	if firstSpace == -1 {
		return input, ""
	}

	var rest string
	if firstSpace == len(input)-1 {
		rest = ""
	} else {
		rest = input[firstSpace+1:]
	}

	return input[:firstSpace], rest
}
