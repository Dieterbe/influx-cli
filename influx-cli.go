package main

import (
	"github.com/andrew-d/go-termutil"
	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/client"
	"github.com/shavac/readline"

	"flag"
	"fmt"
	"os"
	usr "os/user"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var host, user, pass, db string
var port int
var cl *client.Client
var handlers []HandlerSpec
var timing bool

type Handler func(cmd []string) *Timing

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
var regexCreateAdmin = "^create admin ([a-zA-Z0-9_-]+) (.+)"
var regexCreateDb = "^create db ([a-zA-Z0-9_-]+)"
var regexDeleteAdmin = "^delete admin ([a-zA-Z0-9_-]+)"
var regexDeleteDb = "^delete db ([a-zA-Z0-9_-]+)"
var regexDeleteServer = "^delete server (.+)"
var regexDropSeries = "^drop series .+"
var regexEcho = "^echo (.+)"
var regexInsert = "^insert into ([a-zA-Z0-9_-]+) ?(\\(.+\\))? values \\((.*)\\)"
var regexInsertQuoted = "^insert into \"(.+)\" ?(\\(.+\\))? values \\((.*)\\)"
var regexListAdmin = "^list admin"
var regexListDb = "^list db"
var regexListSeries = "^list series.*"
var regexListServers = "^list servers$"
var regexOption = "^\\\\([a-z]+) ?([a-zA-Z0-9_-]+)?"
var regexPing = "^ping$"
var regexRaw = "^raw (.+)"
var regexSelect = "^select .*"
var regexUpdateAdmin = "^update admin ([a-zA-Z0-9_-]+) (.+)"

func init() {
	flag.StringVar(&host, "host", "localhost", "host to connect to")
	flag.IntVar(&port, "port", 8086, "port to connect to")
	flag.StringVar(&user, "user", "root", "influxdb username")
	flag.StringVar(&pass, "pass", "root", "influxdb password")
	flag.StringVar(&db, "db", "", "database to use")

	handlers = []HandlerSpec{
		HandlerSpec{regexBind, bindHandler},
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
		HandlerSpec{regexOption, optionHandler},
		HandlerSpec{regexPing, pingHandler},
		HandlerSpec{regexRaw, rawHandler},
		HandlerSpec{regexSelect, selectHandler},
		HandlerSpec{regexUpdateAdmin, updateAdminPassHandler},
	}
}

func printHelp() {
	out := `Help:

options & current session
-------------------------

\t               : toggle timing, which displays timing of
                   query execution + network and output displaying
                   (default: false)
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


data i/o
--------

insert into <name> [(col1[,col2[...]])] values (val1[,val2[,val3[...]]])
                           : insert values into the given columns for given series name.
                             columns is optional and defaults to (time, sequence_number, value)
select ...                 : select statement for data retrieval


misc
----

raw <str>        : execute query raw (fallback for unsupported queries)
echo <str>       : echo string + newline.
                   this is useful when the input is not visible, i.e. from scripts
commands         : this menu
help             : this menu
exit / ctrl-D    : exit the program
`
	fmt.Println(out)
}

func getClient() error {
	cfg := &client.ClientConfig{
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

func main() {
	flag.Parse()

	history_path := "~/.influx_history"
	cur_usr, err := usr.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		os.Exit(2)
	}
	if history_path[:1] == "~" {
		history_path = strings.Replace(history_path, "~", cur_usr.HomeDir, 1)
	}
	err = readline.ReadHistoryFile(history_path)
	if err != nil {
		if err.Error() != "no such file or directory" {
			fmt.Fprintf(os.Stderr, "Cannot read '%s': %s\n", history_path, err.Error())
			os.Exit(1)
		}
	}

	err = getClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
	interactive := true
	if !termutil.Isatty(os.Stdin.Fd()) {
		interactive = false
	}
	ui(interactive)
	err = readline.WriteHistoryFile(history_path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot write to '%s': %s\n", history_path, err.Error())
		os.Exit(1)
	}
}

func ui(interactive bool) {
	prompt := ""
	if interactive {
		prompt = "influx> "
	}

L:
	for {
		switch result := readline.ReadLine(&prompt); true {
		case result == nil:
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
			cmd := strings.TrimSuffix(strings.TrimSpace(*result), ";")
			handle(cmd)
		}
	}
}

func handle(cmd string) {
	handled := false
	for _, spec := range handlers {
		re := regexp.MustCompile(spec.Match)
		if matches := re.FindStringSubmatch(cmd); len(matches) > 0 {
			t := spec.Handler(matches)
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

func optionHandler(cmd []string) *Timing {
	switch cmd[1] {
	case "t":
		timing = !timing
		fmt.Println("timing is now", timing)
	case "comp":
		cl.DisableCompression()
		fmt.Println("compression is now disabled")
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

func createAdminHandler(cmd []string) *Timing {
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

func updateAdminPassHandler(cmd []string) *Timing {
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

func listAdminHandler(cmd []string) *Timing {
	timings := makeTiming()
	l, err := cl.GetClusterAdminList()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for k, val := range l {
		fmt.Println("##", k)
		for k, v := range val {
			fmt.Printf("%25s %v\n", k, v)
		}
	}
	timings.Printed = time.Now()
	return timings
}

func listDbHandler(cmd []string) *Timing {
	timings := makeTiming()
	list, err := cl.GetDatabaseList()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for _, item := range list {
		fmt.Println(item["name"])
	}
	timings.Printed = time.Now()
	return timings
}

func createDbHandler(cmd []string) *Timing {
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

func deleteDbHandler(cmd []string) *Timing {
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

func deleteAdminHandler(cmd []string) *Timing {
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

func deleteServerHandler(cmd []string) *Timing {
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

func dropSeriesHandler(cmd []string) *Timing {
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

func echoHandler(cmd []string) *Timing {
	timings := makeTiming()
	timings.Executed = time.Now()
	fmt.Println(cmd[1])
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

func bindHandler(cmd []string) *Timing {
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

func insertHandler(cmd []string) *Timing {
	timings := makeTiming()
	series_name := cmd[1]
	cols_str := strings.TrimPrefix(cmd[2], " ")
	var cols []string
	if cols_str != "" {
		cols_str = cols_str[1 : len(cols_str)-1] // strip surrounding ()
		cols = strings.Split(cols_str, ",")
	} else {
		cols = []string{"time", "sequence_number", "value"}
	}
	vals_str := cmd[3]
	values := strings.Split(vals_str, ",")
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

	err := cl.WriteSeries([]*client.Series{serie})

	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func pingHandler(cmd []string) *Timing {
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

func listServersHandler(cmd []string) *Timing {
	timings := makeTiming()
	list, err := cl.Servers()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for _, server := range list {
		fmt.Println("## id", server["id"])
		for k, v := range server {
			if k != "id" {
				fmt.Printf("%25s %v\n", k, v)
			}
		}
	}
	timings.Printed = time.Now()
	return timings
}

func listSeriesHandler(cmd []string) *Timing {
	timings := makeTiming()
	list_series, err := cl.Query(cmd[0])
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	for _, series := range list_series {
		for _, p := range series.Points {
			fmt.Println(p[1])
		}
	}
	timings.Printed = time.Now()
	return timings
}

func selectHandler(cmd []string) *Timing {
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
	defaultSpec := Spec{"%20s", "%20v"}
	var spec Spec
	var ok bool

	for _, serie := range series {
		fmt.Println("##", serie.Name)

		colrows := make([]string, len(serie.Columns), len(serie.Columns))

		for i, col := range serie.Columns {
			if spec, ok = specs[col]; !ok {
				spec = defaultSpec
			}
			fmt.Printf(spec.Header, col)
			colrows[i] = spec.Row
		}
		fmt.Println()
		for _, p := range serie.Points {
			for i, fmtStr := range colrows {
				fmt.Printf(fmtStr, p[i])
			}
			fmt.Println()
		}
	}
	timings.Printed = time.Now()
	return timings
}

func rawHandler(cmd []string) *Timing {
	timings := makeTiming()
	out, err := cl.Query(cmd[1] + ";")
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		return timings
	}
	spew.Dump(out)
	timings.Printed = time.Now()
	return timings
}
