#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test adding real table to a second database
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Databases
duro::db create TEST1 $dbenv
duro::db create TEST2 $dbenv

# Create table
set tx [duro::begin $dbenv TEST1]
duro::table create T1 {
   {INTATTR INTEGER}
   {STRATTR STRING}
} {{INTATTR}} $tx

duro::commit $tx

# Add table to database #2
set tx [duro::begin $dbenv TEST2]
duro::table add T1 $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST1]

duro::table expr T {(SYS_DBTABLES WHERE TABLENAME="T1") {DBNAME}} $tx
if {![duro::table contains T {DBNAME TEST2} $tx]} {
    puts "Table T1 should be in database TEST2, but is not"
    exit 1
}
duro::table drop T $tx

# Drop table
duro::table drop T1 $tx

duro::commit $tx

# Drop databases

duro::db drop TEST1 $dbenv
duro::db drop TEST2 $dbenv
