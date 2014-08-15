package main

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/client"
	"github.com/shavac/readline"

	"flag"
	"fmt"
	"os"
	"regexp"
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

func init() {
	flag.StringVar(&host, "host", "localhost", "host to connect to")
	flag.IntVar(&port, "port", 8086, "port to connect to")
	flag.StringVar(&user, "user", "root", "influxdb username")
	flag.StringVar(&pass, "pass", "root", "influxdb password")
	flag.StringVar(&db, "db", "", "database to use")

	handlers = []HandlerSpec{
		HandlerSpec{"^\\\\(.)", optionHandler},
		HandlerSpec{"^create db ([a-zA-Z0-9_-]+)", createDbHandler},
		HandlerSpec{"^drop db ([a-zA-Z0-9_-]+)", dropDbHandler},
		HandlerSpec{"^list db", listDbHandler},
		HandlerSpec{"^list series.*", listSeriesHandler},
		HandlerSpec{"^list servers$", listServersHandler},
		HandlerSpec{"^ping$", pingHandler},
		HandlerSpec{"^select .*", selectHandler},
		HandlerSpec{".*", defaultHandler},
	}
}

func printHelp() {
	out := `Help:

commands      : this menu
help          : this menu

\t               : toggle option timing (display timings of query execution + network and output displayig)
                   (default: false)

create db <name>           : create database
drop db <name>             : drop database
list db                    : list databases
list series [/regex/[i]]   : list series, optionally filtered by regex
list servers               : list servers
ping                       : ping the server
select ...                 : select statement for data retrieval

*                : execute query raw (fallback for unsupported queries)

exit / ctrl-D    : exit the program
`
	fmt.Println(out)
}

func main() {
	flag.Parse()
	cfg := &client.ClientConfig{
		Host:     fmt.Sprintf("%s:%d", host, port),
		Username: user,
		Password: pass,
		Database: db,
	}
	var err error
	cl, err = client.NewClient(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	err = cl.Ping()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(2)
	}
	ui()
}

func ui() {
	prompt := "influx> "

L:
	for {
		switch result := readline.ReadLine(&prompt); true {
		case result == nil:
			println()
			break L
		case *result == "exit":
			readline.AddHistory(*result)
			println()
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
			break
		}
	}
}

func optionHandler(cmd []string) *Timing {
	switch cmd[1] {
	case "t":
		timing = !timing
		fmt.Println("timing is now", timing)
	default:
		fmt.Fprintf(os.Stderr, "unrecognized option")
	}
	return nil
}

func listDbHandler(cmd []string) *Timing {
	timings := makeTiming()
	list, err := cl.GetDatabaseList()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
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
	err := cl.CreateDatabase(name)
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return timings
	}
	timings.Printed = time.Now()
	return timings
}

func dropDbHandler(cmd []string) *Timing {
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

func pingHandler(cmd []string) *Timing {
	timings := makeTiming()
	err := cl.Ping()
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
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
		fmt.Fprintf(os.Stderr, err.Error())
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
		fmt.Fprintf(os.Stderr, err.Error())
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
		fmt.Fprintf(os.Stderr, err.Error())
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

func defaultHandler(cmd []string) *Timing {
	fmt.Println("executing query RAW!")
	timings := makeTiming()
	out, err := cl.Query(cmd[0] + ";")
	timings.Executed = time.Now()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return timings
	}
	spew.Dump(out)
	timings.Printed = time.Now()
	return timings
}
