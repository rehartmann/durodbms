#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test user-defined types
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

# Create type

set tx [duro::begin $dbenv TEST]

#
# Define type NAME. Only rep is a string without leading or trailing blanks
#
duro::type define NAME {
    {NAME {{NAME STRING}} {NOT (NAME MATCHES " .* ")}}
} $tx

duro::type implement NAME $tx

duro::table create T1 {
   {NO INTEGER}
   {NAME NAME}
} {{NO}} $tx

duro::insert T1 {NO 1 NAME {NAME "Peter Potter"}} $tx

set tpl {NO 2 NAME {NAME " John Johnson"}}
if {![catch {
    duro::insert T1 $tpl $tx
}]} {
    puts "Insertion of tuple $tpl should fail, but succeeded"
    exit 1
}

duro::table drop T1 $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv
