# Fake tickets management system for testing various area of DSE and Cassandra

Romain Anselin - 2022

!!! WARNING: DO NOT USE THIS FOR OTHER PURPOSE THAN TESTING !!!
These scripts allow to feed a table and reproduce anti-pattern
They should not be followed practices and are just created
for the purpose of testing

Sample python programs for auto-insert and reading (badly) content

## To run this program:

- copy the `conf_dummy.ini` file to `conf_myproject.ini` and apply the path of the configuration file in the `astra-insert.py`. ie:
```
### Change file here!
conf_file = '/home/romain/dev/fake-tickets/conf_myproject.ini'
```

- edit the `conf_myproject.ini` with your own credentials
- run the program. ie: 

```
python ./insert-astra.py 0 10
```

## How it works
Main program:
- `insert-astra.py` is the main program and contains the connection builder that leverage `helper.py` to define configuration for Cassandra, DSE or Astra as per the `isAstra` flag of the conf file.
Libraries:
- `helper.py` for generic functions
- `faketickets.py` contains example insert and reads, along SAI and Solr examples
- `deletebackups.py` is an example listing OpsCenter data and generating a delete script for adhoc backups
- `cassqueries.py` is a dumping area for other sample queries on Cassandra