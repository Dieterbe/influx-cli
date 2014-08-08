package main

import (
	"github.com/influxdb/influxdb/client"
	"github.com/shavac/readline"

	"flag"
	"fmt"
	"os"
	"strings"
)

var host, user, pass, db string
var port int
var cl *client.Client

func init() {
	flag.StringVar(&host, "host", "localhost", "host to connect to")
	flag.IntVar(&port, "port", 8086, "port to connect to")
	flag.StringVar(&user, "user", "root", "influxdb username")
	flag.StringVar(&pass, "pass", "root", "influxdb password")
	flag.StringVar(&db, "db", "", "database to use")
}

func printHelp() {
	out := `Help:

commands      : this menu
help          : this menu
list db       : list databases
list series   : list series
*             : execute query raw (fallback for unsupported queries)
exit / ctrl-D : exit the program
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
			cmd := strings.TrimSuffix(*result, ";")
			handle(cmd)
		}
	}
}

func handle(cmd string) {
	switch cmd {
	case "list db":
		list, err := cl.GetDatabaseList()
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}
		for _, item := range list {
			fmt.Println(item["name"])
		}
	case "list series":
		list_series, err := cl.Query(cmd)
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
		}
		for _, series := range list_series {
			for _, p := range series.Points {
				fmt.Println(p[1])
			}
		}
	default:
		// this logic is actually to process select queries
		// should maybe have something more generic for the rest
		fmt.Println("executing query RAW!")
		series, err := cl.Query(cmd + ";")
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			return
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
	}
}
