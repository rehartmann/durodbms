#!/usr/bin/tclsh

# $Id$
#
# Test creation of table with invalid definition
#

load .libs/libdurotcl.so

source tcl/util.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

# Try to create table

if {![catch {
   duro::table create T {
       {A INTEGER}
       {A STRING}
   } {{A}} $tx
}]} {
    puts "Creation of table should fail, but succeeded"
    exit 1
}

if {![catch {
    duro::ptable T tx1
}]} {
    puts "Table T should not exist, but does"
    exit 1
}

# Check if the transaction is still intact
duro::table create T {{A INTEGER}} {{A}} $tx

duro::rollback $tx

# Close DB environment
duro::env close $dbenv
