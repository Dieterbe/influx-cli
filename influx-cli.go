package main

import (
	"github.com/BurntSushi/toml"
	"github.com/andrew-d/go-termutil"
	"github.com/davecgh/go-spew/spew"
	"github.com/gobs/readline"
	"github.com/influxdb/influxdb/client"
	"github.com/rcrowley/go-metrics"
	//	"log"
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	usr "os/user"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// the following client methods are not implemented yet.
// CreateDatabaseUser
// ChangeDatabaseUser
// UpdateDatabaseUser
// UpdateDatabaseUserPermissions
// DeleteDatabaseUser
// GetDatabaseUserList
// AlterDatabasePrivilege
// AuthenticateClusterAdmin
// GetShards // this returns LongTermShortTermShards which i think is not useful for >0.8

// DropShardSpace
// CreateShardSpace
// DropShard
// UpdateShardSpace

// upto how many points to commit in 1 go?
var AsyncCapacity = 1000

// how long to wait max before flushing a commit payload
var AsyncMaxWait = 500 * time.Millisecond

var host, user, pass, db string
var port int
var cl *client.Client
var cfg *client.ClientConfig
var handlers []HandlerSpec
var timing bool
var dateTime bool
var recordsOnly bool
var async bool
var asyncInserts chan *client.Series
var asyncInsertsCommitted chan int
var forceInsertsFlush chan bool
var sync_inserts_timer metrics.Timer

var path_rc, path_hist string

type Handler func(cmd []string, out io.Writer) *Timing

type HandlerSpec struct {
	Match string
	Handler
}

type Timing struct {
	Pre      time.Time
	Executed time.Time
	Printed  time.Time
}

func makeTiming() *Timing {
	return &Timing{Pre: time.Now()}
}

func (t *Timing) StringQuery() string {
	if t.Executed.IsZero() {
		return "unknown"
	}
	return t.Executed.Sub(t.Pre).String()
}

func (t *Timing) StringPrint() string {
	if t.Executed.IsZero() || t.Printed.IsZero() {
		return "unknown"
	}
	return t.Printed.Sub(t.Executed).String()
}

func (t *Timing) String() string {
	return "query+network: " + t.StringQuery() + "\ndisplaying   : " + t.StringPrint()
}

var regexBind = "^bind"
var regexConn = "^conn$"
var regexCreateAdmin = "^create admin ([a-zA-Z0-9_-]+) (.+)"
var regexCreateDb = "^create db ([a-zA-Z0-9_-]+)"
var regexDeleteAdmin = "^delete admin ([a-zA-Z0-9_-]+)"
var regexDeleteDb = "^delete db ([a-zA-Z0-9_-]+)"
var regexDeleteServer = "^delete server (.+)"
var regexDropSeries = "^drop series .+"
var regexEcho = "^echo (.+)"
var regexInsert = "^insert into ([a-zA-Z0-9_-]+) ?(\\(.+\\))? values \\((.*)\\)$"
var regexInsertQuoted = "^insert into \"(.+)\" ?(\\(.+\\))? values \\((.*)\\)$"
var regexListAdmin = "^list admin"
var regexListDb = "^list db"
var regexListSeries = "^list series.*"
var regexListServers = "^list servers$"
var regexListShardspaces = "^list shardspaces$"
var regexOption = "^\\\\([a-z]+) ?([a-zA-Z0-9_-]+)?"
var regexPing = "^ping$"
var regexRaw = "^raw (.+)"
var regexSelect = "^select .*"
var regexUpdateAdmin = "^update admin ([a-zA-Z0-9_-]+) (.+)"
var regexWriteRc = "^writerc"

type Config struct {
	Host          string
	Port          int
	User          string
	Pass          string
	Db            string
	AsyncCapacity int
	AsyncMaxWait  int
}

func init() {
	path_rc = Expand("~/.influxrc")
	path_hist = Expand("~/.influx_history")

	flag.StringVar(&host, "host", "localhost", "host to connect to")
	flag.IntVar(&port, "port", 8086, "port to connect to")
	flag.StringVar(&user, "user", "root", "influxdb username")
	flag.StringVar(&pass, "pass", "root", "influxdb password")
	flag.StringVar(&db, "db", "", "database to use")
	flag.BoolVar(&recordsOnly, "recordsOnly", false, "when enabled, doesn't display header")
	flag.BoolVar(&async, "async", false, "when enabled, asynchronously flushes inserts")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: influx-cli [flags] [query to execute on start]")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nNote: you can also pipe queries into stdin, one line per query\n")
	}

	handlers = []HandlerSpec{
		HandlerSpec{regexBind, bindHandler},
		HandlerSpec{regexConn, connHandler},
		HandlerSpec{regexCreateAdmin, createAdminHandler},
		HandlerSpec{regexCreateDb, createDbHandler},
		HandlerSpec{regexDeleteAdmin, deleteAdminHandler},
		HandlerSpec{regexDeleteDb, deleteDbHandler},
		HandlerSpec{regexDeleteServer, deleteServerHandler},
		HandlerSpec{regexDropSeries, dropSeriesHandler},
		HandlerSpec{regexEcho, echoHandler},
		HandlerSpec{regexInsert, insertHandler},
		HandlerSpec{regexInsertQuoted, insertHandler},
		HandlerSpec{regexListAdmin, listAdminHandler},
		HandlerSpec{regexListDb, listDbHandler},
		HandlerSpec{regexListSeries, listSeriesHandler},
		HandlerSpec{regexListServers, listServersHandler},
		HandlerSpec{regexListShardspaces, listShardspacesHandler},
		HandlerSpec{regexOption, optionHandler},
		HandlerSpec{regexPing, pingHandler},
		HandlerSpec{regexRaw, rawHandler},
		HandlerSpec{regexSelect, selectHandler},
		HandlerSpec{regexUpdateAdmin, updateAdminPassHandler},
		HandlerSpec{regexWriteRc, writeRcHandler},
	}

	asyncInserts = make(chan *client.Series)
	asyncInsertsCommitted = make(chan int)
	forceInsertsFlush = make(chan bool)

	sync_inserts_timer = metrics.NewTimer()
	metrics.Register("insert_sync", sync_inserts_timer)
}

func printHelp() {
	out := `Help:

options & current session
-------------------------

\dt              : print timestamps as datetime strings
\r               : show records only, no headers
\t               : toggle timing, which displays timing of
                   query execution + network and output displaying
                   (default: false)
\async           : asynchronously flush inserts
\comp            : disable compression (client lib doesn't support enabling)
\db <db>         : switch to databasename (requires a bind call to be effective)
\user <username> : switch to different user (requires a bind call to be effective)
\pass <password> : update password (requires a bind call to be effective)

bind             : bind again, possibly after updating db, user or pass
ping             : ping the server


admin
-----

create admin <user> <pass>      : add given admin user
delete admin <user>             : delete admin user
update admin <user> <pass>      : update the password for given admin user
list admin                      : list admins

create db <name>                : create database
delete db <name>                : drop database
list db                         : list databases

list series [/regex/[i]]        : list series, optionally filtered by regex
drop series <name>              : drop series by given name

delete server <id>              : delete server by id
list servers                    : list servers

list shardspaces                : list shardspaces


data i/o
--------

insert into <name> [(col1[,col2[...]])] values (val1[,val2[,val3[...]]])
                           : insert values into the given columns for given series name.
                             columns is optional and defaults to (time, sequence_number, value)
                             (timestamp is assumed to be in ms. ms/u/s prefixes don't work yet)
select ...                 : select statement for data retrieval


misc
----

conn             : display info about current connection
raw <str>        : execute query raw (fallback for unsupported queries)
echo <str>       : echo string + newline.
                   this is useful when the input is not visible, i.e. from scripts
writerc          : write current parameters to ~/.influxrc file
commands         : this menu
help             : this menu
exit / ctrl-D    : exit the program

modifiers
---------

ANY command above can be subject to piping to another command or writing output to a file, like so:

command; | <command>     : pipe the output into an external command (example: list series; | sort)
                           note: currently you can only pipe into one external command at a time
command; > <filename>    : redirect the output into a file

`
	fmt.Println(out)
}

func getClient() error {
	cfg = &client.ClientConfig{
		Host:     fmt.Sprintf("%s:%d", host, port),
		Username: user,
		Password: pass,
		Database: db,
	}
	var err error
	cl, err = client.NewClient(cfg)
	if err != nil {
		return err
	}
	err = cl.Ping()
	if err != nil {
		return err
	}
	//fmt.Printf("connected to %s:%s@%s:%d/%s\n", user, pass, host, port, db)
	return nil
}

func Expand(in string) (out string) {
	if in[:1] == "~" {
		cur_usr, err := usr.Current()
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error()+"\n")
			os.Exit(2)
		}
		out := strings.Replace(in, "~", cur_usr.HomeDir, 1)
		return out
	}
	return in
}

func main() {
	var conf Config
	if _, err := os.Stat(path_rc); err == nil {
		if _, err := toml.DecodeFile(path_rc, &conf); err != nil {
			fmt.Fprintf(os.Stderr, err.Error()+"\n")
			os.Exit(2)
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		os.Exit(2)
	}
	// else, rc doesn't exist, which is fine.

	if conf.Host != "" {
		host = conf.Host
	}
	if conf.Port != 0 {
		port = conf.Port
	}
	if conf.User != "" {
		user = conf.User
	}
	if conf.Pass != "" {
		pass = conf.Pass
	}
	if conf.Db != "" {
		db = conf.Db
	}
	if conf.AsyncCapacity > 0 {
		AsyncCapacity = conf.AsyncCapacity
	}
	if conf.AsyncMaxWait > 0 {
		AsyncMaxWait = time.Duration(conf.AsyncMaxWait) * time.Millisecond
	}

	flag.Parse()
	query := strings.Join(flag.Args(), " ")

	err := getClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}

	//go metrics.Log(metrics.DefaultRegistry, 10e9, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))
	go committer()

	if query != "" {
		// execute query passed from cmd arg and stop
		cmd := strings.TrimSuffix(strings.TrimSpace(query), ";")
		handle(cmd)
	} else if !termutil.Isatty(os.Stdin.Fd()) {
		// execute all input from stdin and stop
		readStdin()
	} else {
		// if stdin is a tty, provide readline prompt with history.
		err = readline.ReadHistoryFile(path_hist)
		if err != nil && err.Error() != "no such file or directory" {
			fmt.Fprintf(os.Stderr, "Cannot read '%s': %s\n", path_hist, err.Error())
			os.Exit(1)
		}
		ui()
		err = readline.WriteHistoryFile(path_hist)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot write to '%s': %s\n", path_hist, err.Error())
			Exit(1)
		}
	}
	Exit(0)
}
func Exit(code int) {
	close(asyncInserts)
	select {
	case <-time.After(time.Second * 5):
		fmt.Fprintf(os.Stderr, "Could not flush all inserts.  Closing anyway")
	case num := <-asyncInsertsCommitted:
		if num > 0 {
			fmt.Printf("Final %d async inserts committed\n", num)
		}
	}
	os.Exit(code)
}

func readStdin() {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			Exit(2)
		}
		cmd := strings.TrimSpace(line)
		handle(cmd)
	}
}

func ui() {
	prompt := "influx> "
L:
	for {
		switch result := readline.ReadLine(&prompt); true {
		case result == nil:
			fmt.Println("")
			break L
		case *result == "exit":
			readline.AddHistory(*result)
			break L
		case *result == "commands":
			readline.AddHistory(*result)
			printHelp()
		case *result == "help":
			readline.AddHistory(*result)
			printHelp()
		case *result != "": //ignore blank lines
			readline.AddHistory(*result)
			cmd := strings.TrimSpace(*result)
			handle(cmd)
		}
	}
}

func handle(cmd string) {
	handled := false
	var writeTo io.WriteCloser
	var pipeTo *exec.Cmd
	writeTo = os.Stdout
	mode := 0 // 1 -> pipe to cmd, 2 -> write to file
	cmd = strings.Replace(cmd, "; |", ";|", 1)
	cmd = strings.Replace(cmd, "; >", ";>", 1)

	if strings.Contains(cmd, ";|") {
		mode = 1
		cmdArr := strings.Split(cmd, ";|")
		cmd = strings.TrimSpace(cmdArr[0])
		cmdAndArgs := strings.Fields(strings.TrimSpace(cmdArr[1]))
		if len(cmdAndArgs) == 0 {
			fmt.Fprintln(os.Stderr, "error: no command specified to pipe to")
			Exit(2)
		}

		pipeTo = exec.Command(cmdAndArgs[0], cmdAndArgs[1:]...)
		var err error
		writeTo, err = pipeTo.StdinPipe()
		if err != nil {
			fmt.Fprintln(os.Stderr, "internal error: cannot open pipe", err.Error())
			Exit(2)
		}
		pipeTo.Stdout = os.Stdout
		pipeTo.Stderr = os.Stderr
	} else if strings.Contains(cmd, ";>") {
		mode = 2
		cmdArr := strings.Split(cmd, ";>")
		cmd = cmdArr[0]
		file := strings.TrimSpace(cmdArr[1])
		fd, err := os.Create(file)
		if err != nil {
			fmt.Fprintln(os.Stderr, "internal error: cannot open file", file, "for writing", err.Error())
			Exit(2)
		}
		defer func() { fd.Close() }()
		writeTo = fd
	} else {
		// it may or may not have this ending delimiter
		cmd = strings.TrimSuffix(cmd, ";")
	}

	for _, spec := range handlers {
		re := regexp.MustCompile(spec.Match)
		if matches := re.FindStringSubmatch(cmd); len(matches) > 0 {
			if mode == 1 {
				err := pipeTo.Start()
				if err != nil {
					fmt.Fprintln(os.Stderr, "subcommand failed: ", err.Error())
					fmt.Fprintln(os.Stderr, "aborting query")
					break
				}
			}
			t := spec.Handler(matches, writeTo)
			if mode == 1 {
				writeTo.Close()
				err := pipeTo.Wait()
				if err != nil {
					fmt.Fprintln(os.Stderr, "subcommand failed: ", err.Error())
				}
			}

			if timing {
				// some functions return no timing, because it doesn't apply to them
				if t != nil {
					fmt.Println("timing>")
					fmt.Println(t)
				}
			}
			handled = true
		}
	}
	if !handled {
		fmt.Fprintln(os.Stderr, "Could not handle the command. type 'help' to get a help menu")
	}
}

func optionHandler(cmd []string, out io.Writer) *Timing {
	switch cmd[1] {
	case "async":
		if async {
			// so we don't get any insert errors after disabling async
			fmt.Fprintln(out, "flushing any pending async inserts", async)
			forceInsertsFlush <- true
		}
		async = !async
		fmt.Fprintln(out, "async is now", async)
	case "dt":
		dateTime = !dateTime
		fmt.Fprintln(out, "datetime printing is now", dateTime)
	case "r":
		recordsOnly = !recordsOnly
		fmt.Fprintln(out, "records-only is now", recordsOnly)
	case "t":
		timing = !timing
		fmt.Fprintln(out, "timing is now", timing)
	case "comp":
		cl.DisableCompression()
		fmt.Fprintln(out, "compression is now disabled")
	case "db":
		if cmd[2] == "" {
			fmt.Fprintf(os.Stderr, "database argument must be set")
			break
		}
		db = cmd[2]
	case "user":
		if cmd[2] == "" {
			fmt.Fprintf(os.Stderr, "user argument must be set")
			break
		}
		user = cmd[2]
	case "pass":
		if cmd[2] == "" {
			fmt.Fprintf(os.Stderr, "password argument must be set")
			break
		}
		pass = cmd[2]
	default:
		fmt.Fprintf(os.Stderr, "unrecognized option")
	}
	return nil
}

func createAdminHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	name := strings.TrimSpace(cmd[1])
	pass := strings.TrimSpace(cmd[2])
	err := cl.CreateClusterAdmin(name, pass)
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func updateAdminPassHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	name := strings.TrimSpace(cmd[1])
	pass := strings.TrimSpace(cmd[2])
	err := cl.UpdateClusterAdmin(name, pass)
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func listAdminHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	l, err := cl.GetClusterAdminList()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for k, val := range l {
		fmt.Fprintln(out, "##", k)
		for k, v := range val {
			fmt.Fprintf(out, "%25s %v\n", k, v)
		}
	}
	timings.Printed = time.Now()
	return timings
}

func listDbHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	list, err := cl.GetDatabaseList()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for _, item := range list {
		fmt.Fprintln(out, item["name"])
	}
	timings.Printed = time.Now()
	return timings
}

func createDbHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	err := cl.CreateDatabase(cmd[1])
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func deleteDbHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	err := cl.DeleteDatabase(cmd[1])
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func deleteAdminHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	err := cl.DeleteClusterAdmin(strings.TrimSpace(cmd[1]))
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func deleteServerHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	id, err := strconv.ParseInt(cmd[1], 10, 32)
	err = cl.RemoveServer(int(id))
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func dropSeriesHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	_, err := cl.Query(cmd[0] + ";")
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func echoHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	timings.Executed = time.Now()
	fmt.Fprintln(out, cmd[1])
	timings.Printed = time.Now()
	return timings
}

// influxdb is typed, so try to parse as int, as float, and fall back to str
func parseTyped(value_str string) interface{} {
	valueInt, err := strconv.ParseInt(strings.TrimSpace(value_str), 10, 64)
	if err == nil {
		return valueInt
	}
	valueFloat, err := strconv.ParseFloat(value_str, 64)
	if err == nil {
		return valueFloat
	}
	return value_str
}

func bindHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	// for some reason this call returns error (401): Invalid username/password
	//err := cl.AuthenticateDatabaseUser(db, user, pass)
	// so for now, the slightly less efficient way:
	err := getClient()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func connHandler(cmd []string, out io.Writer) *Timing {
	fmt.Fprintf(out, "Host        : %s\n", cfg.Host)
	fmt.Fprintf(out, "User        : %s\n", cfg.Username)
	fmt.Fprintf(out, "Pass        : %s\n", cfg.Password)
	fmt.Fprintf(out, "Db          : %s\n", cfg.Database)
	fmt.Fprintf(out, "secure      : %t\n", cfg.IsSecure)
	fmt.Fprintf(out, "udp         : %t\n", cfg.IsUDP)
	fmt.Fprintf(out, "compression : ?\n") // can't query client for this
	fmt.Fprintf(out, "Client      : %s\n", cfg.HttpClient)
	return nil
}

func insertHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	series_name := cmd[1]
	cols_str := strings.TrimPrefix(cmd[2], " ")
	var cols []string
	if cols_str != "" {
		cols_str = cols_str[1 : len(cols_str)-1] // strip surrounding ()
		tmp_cols := strings.Split(cols_str, ",")
		cols = make([]string, len(tmp_cols))
		for i, name := range tmp_cols {
			cols[i] = strings.TrimSpace(name)
		}
	} else {
		cols = []string{"time", "sequence_number", "value"}
	}
	vals_str := cmd[3]
	// vals_str could be: foo,bar,"avg(something,123)",quux
	reader := csv.NewReader(strings.NewReader(vals_str))
	values, err := reader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not parse values"+err.Error()+"\n")
		return timings
	}

	if len(values) != len(cols) {
		fmt.Fprintf(os.Stderr, "Number of values (%d) must match number of colums (%d): Columns are: %v\n", len(values), len(cols), cols)
		return timings
	}
	point := make([]interface{}, len(cols), len(cols))

	for i, value_str := range values {
		point[i] = parseTyped(value_str)
	}

	serie := &client.Series{
		Name:    series_name,
		Columns: cols,
		Points:  [][]interface{}{point},
	}

	if async {
		asyncInserts <- serie
		err = nil
	} else {
		ts := time.Now()
		err = cl.WriteSeries([]*client.Series{serie})
		sync_inserts_timer.Update(time.Since(ts))
	}
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}
func committer() {
	toCommit := make([]*client.Series, 0, AsyncCapacity)

	commit := func() int {
		size := len(toCommit)
		if size == 0 {
			return 0
		}
		t := metrics.GetOrRegisterTimer("inserts_async_"+strconv.FormatInt(int64(len(toCommit)), 10), metrics.DefaultRegistry)
		defer func(start time.Time) { t.Update(time.Since(start)) }(time.Now())
		err := cl.WriteSeries(toCommit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write %d series: %s\n", len(toCommit), err.Error())
		}
		toCommit = make([]*client.Series, 0, AsyncCapacity)
		return size
	}

	timer := time.NewTimer(AsyncMaxWait)

CommitLoop:
	for {
		select {
		case serie, ok := <-asyncInserts:
			if ok {
				toCommit = append(toCommit, serie)
			} else {
				// no more input, commit whatever we have and break
				asyncInsertsCommitted <- commit()
				break CommitLoop
			}
			// if capacity reached, commit
			if len(toCommit) == AsyncCapacity {
				commit()
				timer.Reset(AsyncMaxWait)
			}
		case <-timer.C:
			commit()
			timer.Reset(AsyncMaxWait)
		case <-forceInsertsFlush:
			commit()
			timer.Reset(AsyncMaxWait)
		}
	}
}

func pingHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	err := cl.Ping()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func listServersHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	list, err := cl.Servers()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for _, server := range list {
		fmt.Fprintln(out, "## id", server["id"])
		for k, v := range server {
			if k != "id" {
				fmt.Fprintf(out, "%25s %v\n", k, v)
			}
		}
	}
	timings.Printed = time.Now()
	return timings
}

func listSeriesHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	list_series, err := cl.Query(cmd[0])
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for _, series := range list_series {
		for _, p := range series.Points {
			fmt.Fprintln(out, p[1])
		}
	}
	timings.Printed = time.Now()
	return timings
}

func listShardspacesHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	shardSpaces, err := cl.GetShardSpaces()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	dbLenMax := len("Database")
	nameLenMax := len("Name")
	regexLenMax := len("Regex")
	retentionLenMax := len("Retention")
	durationLenMax := len("Duration")

	for _, s := range shardSpaces {
		if len(s.Database) > dbLenMax {
			dbLenMax = len(s.Database)
		}
		if len(s.Name) > nameLenMax {
			nameLenMax = len(s.Name)
		}
		if len(s.Regex) > regexLenMax {
			regexLenMax = len(s.Regex)
		}
		if len(s.RetentionPolicy) > retentionLenMax {
			retentionLenMax = len(s.RetentionPolicy)
		}
		if len(s.ShardDuration) > durationLenMax {
			durationLenMax = len(s.ShardDuration)
		}
	}
	headerFmt := fmt.Sprintf("%%%ds %%%ds %%%ds %%%ds %%%ds %%2s %%5s\n", dbLenMax, nameLenMax, regexLenMax, retentionLenMax, durationLenMax)
	rowFmt := fmt.Sprintf("%%%ds %%%ds %%%ds %%%ds %%%ds %%2d %%5d\n", dbLenMax, nameLenMax, regexLenMax, retentionLenMax, durationLenMax)
	fmt.Fprintf(out, headerFmt, "Database", "Name", "Regex", "Retention", "Duration", "RF", "Split")
	for _, s := range shardSpaces {
		fmt.Fprintf(out, rowFmt, s.Database, s.Name, s.Regex, s.RetentionPolicy, s.ShardDuration, s.ReplicationFactor, s.Split)
	}
	timings.Printed = time.Now()
	return timings
}

func selectHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	series, err := cl.Query(cmd[0] + ";")
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	type Spec struct {
		Header string
		Row    string
	}
	specs := map[string]Spec{
		"time":            {"%20s", "%20f"},
		"sequence_number": {"%16s", "      %10f"},
		"value":           {"%20s", "%20f"},
	}
	if dateTime {
		specs["time"] = Spec{"%33s", "%33s"}
	}
	defaultSpec := Spec{"%20s", "%20v"}
	var spec Spec
	var ok bool

	for _, serie := range series {
		if !recordsOnly {
			fmt.Fprintln(out, "##", serie.Name)
		}

		colrows := make([]string, len(serie.Columns), len(serie.Columns))

		for i, col := range serie.Columns {
			if spec, ok = specs[col]; !ok {
				spec = defaultSpec
			}
			if !recordsOnly {
				fmt.Fprintf(out, spec.Header, col)
			}
			colrows[i] = spec.Row
		}
		if !recordsOnly {
			fmt.Fprintln(out)
		}
		for _, p := range serie.Points {
			for i, fmtStr := range colrows {
				if i == 0 && dateTime {
					msFloat := p[i].(float64)
					ns := (int64(msFloat) % 1000) * 1000000
					s := int64(msFloat / 1000)
					d := time.Unix(s, ns)
					fmt.Fprintf(out, fmtStr, d)
				} else {
					fmt.Fprintf(out, fmtStr, p[i])
				}
			}
			fmt.Fprintln(out)
		}
	}
	timings.Printed = time.Now()
	return timings
}

func rawHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	result, err := cl.Query(cmd[1] + ";")
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	spew.Dump(result)
	timings.Printed = time.Now()
	return timings
}

func writeRcHandler(cmd []string, out io.Writer) *Timing {
	timings := makeTiming()
	tpl := `host = "%s"
port = %d
user = "%s"
pass = "%s"
db = "%s"
`
	rc, err := os.Create(path_rc)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	_, err = fmt.Fprintf(rc, tpl, host, port, user, pass, db)

	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	return timings
}
