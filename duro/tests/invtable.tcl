#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test creation of table with invalid definition
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

#
# Try to create table with invalid definitions
#

if {![catch {
   duro::table create "T 1" {
       {A INTEGER}
   } {{A}} $tx
}]} {
   puts "Creation of table \"T 1\" should fail, but succeeded"
   exit 1
}

if {![catch {
    duro::table drop "T 1" $tx
}]} {
    puts "Table \"T 1\" should not exist, but does"
    exit 1
}

if {![catch {
   duro::table create T1 {
       {A INTEGER}
       {A STRING}
   } {{A}} $tx
}]} {
    puts "Creation of table T1 should fail, but succeeded"
    exit 1
}

if {![catch {
    duro::table drop T1 $tx
}]} {
    puts "Table T1 should not exist, but does"
    exit 1
}

if {![catch {
    duro::table create T2 {
       {A1 STRING}
       {A2 STRING}
    } {{A1 A1}} $tx
}]} {
    puts "Creation of table T2 should fail, but succeeded"
    exit 1
}

if {![catch {
    duro::table drop T2 $tx
}]} {
    puts "Table T2 should not exist, but does"
    exit 1
}

if {![catch {
    duro::table create T3 {
       {A1 STRING}
       {A2 STRING}
    } {{A1} {A1 A2}} $tx
}]} {
    puts "Creation of table T3 should fail, but succeeded"
    exit 1
}

if {![catch {
    duro::table drop T3 $tx
}]} {
    puts "Table T3 should not exist, but does"
    exit 1
}

# Check if the transaction is still intact
duro::table create T {{A INTEGER}} {{A}} $tx

duro::rollback $tx

# Close DB environment
duro::env close $dbenv
