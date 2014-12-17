commandline client for influxdb, in Go
similar to mysql, pgsql, etc.

features
--------

* implements allmost all available influxdb api features
* makes influxdb features available through the query language, even when influxdb itself only supports them as API calls.
* readline (history searching and navigation. uses ~/.influx_history)
* ability to read commands from stdin, pipe command/query out to external process or redirect to a file


installation
------------

```
go get github.com/Dieterbe/influx-cli
```

configuration
-------------

The commandline options (see below) can also be stored in `~/.influxrc`
Although this is entirely optional.

For example:

```
host = "localhost"
port = 8086
user = "root"
pass = "root"
db = ""
asyncCapacity = 100  # in datapoints
asyncMaxWait = 1000  # in ms
```

The values in use at runtime follow this order of preference:  

  defaults -> influxrc -> commandline args -> interactive updates

Pro-tip: you can use the `writerc` command at runtime to generate this file,
it will export the current runtime values.


running
-------

`$GOPATH/bin/influx-cli` or just `influx-cli` if you put `$GOPATH/bin` in your `$PATH`

```
Usage: influx-cli [flags] [query to execute on start]

Flags:
  -async=false: when enabled, asynchronously flushes inserts
  -db="": database to use
  -host="localhost": host to connect to
  -pass="root": influxdb password
  -port=8086: port to connect to
  -recordsOnly=false: when enabled, doesn't display header
  -user="root": influxdb username

Note: you can also pipe queries into stdin, one line per query
```

usage
-----

```

options & current session
-------------------------

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

```
