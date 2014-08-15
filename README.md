commandline client for influxdb, in Go
similar to mysql, pgsql, etc.

features
--------

* readline (history searching and navigation)
* you can talk to influxdb... from the commandline!

Currently supported:

```
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


```

installation
------------

```
go get github.com/Dieterbe/influx-cli
```

running
-------

`$GOPATH/bin/influx-cli` or just `influx-cli` if you put `$GOPATH/bin` in your `$PATH`

```
$ influx-cli --help
Usage of influx-cli:
  -db="": database to use
  -host="localhost": host to connect to
  -pass="root": influxdb password
  -port=8086: port to connect to
  -user="root": influxdb username
```

TODO
----

* readline persistence
* implement more commands
