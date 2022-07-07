# Fake tickets management system for testing various area of DSE and Cassandra

Romain Anselin - 2022

!!! WARNING: DO NOT USE THIS FOR OTHER PURPOSE THAN TESTING !!!
These scripts allow to feed a table and reproduce anti-pattern
They should not be followed practices and are just created
for the purpose of testing

Sample python programs for auto-insert and reading (badly) content

To run this program:

- copy the `conf_dummy.ini` file to `conf_myproject.ini` and apply the path of the configuration file in the `helper.py`. ie:
    `config.read('/home/romain/dev/python-driver-sample/astra-test/conf_bootcamp.ini')`
- edit the `conf_myproject.ini` with your own credentials
