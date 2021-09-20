package repl

import (
	"bufio"
	"errors"
	// "fmt"
	"io"
	"net"
	"os"
	// "strings"

	uuid "github.com/google/uuid"
)

// REPL struct.
type REPL struct {
	commands map[string]func(string, *REPLConfig) error
	help     map[string]string
}

// REPL Config struct.
type REPLConfig struct {
	writer   io.Writer
	clientId uuid.UUID
}

// Get writer.
func (replConfig *REPLConfig) GetWriter() io.Writer {
	return replConfig.writer
}

// Get address.
func (replConfig *REPLConfig) GetAddr() uuid.UUID {
	return replConfig.clientId
}

// Construct an empty REPL.
func NewRepl() *REPL {
	r := new(REPL)
	r.commands = nil
	r.help = nil
	return r
}

// Combines a slice of REPLs.
func CombineRepls(repls []*REPL) (*REPL, error) {
	if repls == nil{
		r := NewRepl()
		return r, errors.New("empty repls")
	}
	cur := repls[0]
	for index, element := range repls{
		if index == 0{
		}else{
			for key, _ := range element.commands {
				_, ok := cur.commands[key]
				if ok {
					return nil, errors.New("duplicated key in repls")

				}else{
					cur.commands[key] = element.commands[key]
					cur.help[key] = element.help[key]
				}
			}
		}
	}
	return cur, nil
}

// Get commands.
func (r *REPL) GetCommands() map[string]func(string, *REPLConfig) error {
	return r.commands
}

// Get help.
func (r *REPL) GetHelp() map[string]string {
	return r.help
}

// Add a command, along with its help string, to the set of commands.
func (r *REPL) AddCommand(trigger string, action func(string, *REPLConfig) error, help string) {
	r.commands[trigger] = action
	r.help[trigger] = help
}

// Return all REPL usage information as a string.
func (r *REPL) HelpString() string {
	res := ""
	for _, ele := range r.help{
		res += ele
	}
	return res
}

// Run the REPL.
func (r *REPL) Run(c net.Conn, clientId uuid.UUID, prompt string) {
	// Get reader and writer; stdin and stdout if no conn.
	var reader io.Reader
	var writer io.Writer
	if c == nil {
		reader = os.Stdin
		writer = os.Stdout
	} else {
		reader = c
		writer = c
	}
	scanner := bufio.NewScanner((reader))
	replConfig := &REPLConfig{writer: writer, clientId: clientId}
	// Begin the repl loop!
	for scanner.Scan() {
		// clean user input here scanner.Text()
		io.WriteString(writer, prompt)
		cur := scanner.Text()
		handler := r.commands[cur]
		handler(cur, replConfig)
		// after get command, go find action in map
	}
}

// cleanInput preprocesses input to the db repl.
func cleanInput(text string) string {
	panic("function not yet implemented");
}
