HanaStress
==========

<p>A testing tool for SAP HANA. This tool can be very destructive and should never go near production systems. Use at your own risk.</p>

<h3>Installation</h3>
==========
<ul>
<li>This requires you have the SAP HANA Database CLient Installed on your machine</li>
<li>Run <code>git clone https://github.com/Centiq/HanaStress.git /opt/hanastress</code></li>
</ul>

<h3>Running HanaStress</h3>
<h5>Example 1: View Help</h5>
```
root@hana01:/opt# /opt/hanastress/hanastress.py -h
Usage: hanastress.py [options]

Options:
  -h, --help            show this help message and exit
  -v, --verbose         Give detailed messages
  -l HOST, --host=HOST  The hostname of the DB instance
  -i INSTANCE, --instance=INSTANCE
                        The DB instance to connect to
  -u USER, --user=USER  The user to use for the DB connection
  -p PASSWORD, --password=PASSWORD
                        The password to use for the DB connection
  -g GENERATE, --generate=GENERATE
                        Generate a schema. Usage: --create {SCHEMA_TYPE}. Can
                        be:
  -t TABLES, --tables=TABLES
                        Used with '--generate'. The amount of tables to
                        create. Default: 50
  -s, --rowstorage      Used with '--generate', Will set table type to Row
                        Store instead of Column Store
  -r ROWS, --rows=ROWS  Used with '--generate', set the amount of rows for
                        each table. Default: 100
  -k THREADS, --threads=THREADS
                        The amount of threads to use
  --destroy             This will DESTROY all the schemas owned by the given
                        user (expect their default schema). BE CAREFUL!
```

<h5>Example 2: Generating Data</h5>
```
/opt/hanastress/hanastress.py -v --host localhost -i 00 -u HANASTRESS -p MYPASSWORD -g anarchy --tables 100 --rows 100000 --threads 10
```
<p>This will create 100 tables with 100000 rows of information each, using 10 threads.</p>
