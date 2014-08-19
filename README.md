commandline client for influxdb, in Go
similar to mysql, pgsql, etc.

features
--------

* implements allmost all available influxdb api features
* makes influxdb features available through the query language, even when influxdb itself only supports them as API calls.
* readline (history searching and navigation. uses ~/.influx_history)


installation
------------

```
go get github.com/Dieterbe/influx-cli
```

running
-------

`$GOPATH/bin/influx-cli` or just `influx-cli` if you put `$GOPATH/bin` in your `$PATH`

```
Usage of ./influx:
  -db="": database to use
  -host="localhost": host to connect to
  -pass="root": influxdb password
  -port=8086: port to connect to
  -user="root": influxdb username
```

usage
-----

```

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

```
