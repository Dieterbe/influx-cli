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

create db <name> : create database
drop db <name>   : drop database
list db          : list databases
list series      : list series
list servers     : list servers
ping             : ping the server
select ...       : select statement for data retrieval

*                : execute query raw (fallback for unsupported queries)

exit / ctrl-D    : exit the program

```

TODO
----

* readline persistence
* implement many more commands
