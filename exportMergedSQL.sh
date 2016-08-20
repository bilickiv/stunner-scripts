#!/bin/sh
mysql -h 127.0.0.1 -u root -e "use test; select * from DATA where pubIP is not null limit 10" > test.txt