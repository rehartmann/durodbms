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
    {NAME {{NAME STRING}} {NOT ((NAME MATCHES "^ .*") \
            OR (NAME MATCHES ".* $"))}}
} $tx

duro::type implement NAME $tx

duro::table create T1 {
   {NO INTEGER}
   {NAME NAME}
} {{NO}} $tx

duro::insert T1 {NO 1 NAME {NAME "Potter"}} $tx

set tpl {NO 2 NAME {NAME " Johnson"}}
if {![catch {
    duro::insert T1 $tpl $tx
}]} {
    puts "Insertion of tuple $tpl should fail, but succeeded"
    exit 1
}

duro::table drop T1 $tx

#
# Define type PNAME. Only rep is (FIRSTNAME,LASTNAME)
#

duro::type define PNAME {
    {PNAME {{FIRSTNAME STRING} {LASTNAME STRING}}}
} $tx

duro::type implement PNAME $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

duro::table create T2 {
   {NO INTEGER}
   {NAME PNAME}
} {{NO}} $tx

set tpl {NO 1 NAME {PNAME "Peter" "Potter"}}
duro::insert T2 $tpl $tx

if {![duro::table contains T2 $tpl $tx]} {
    puts "T2 should contain $tpl, but does not."
    exit 2
}

array set a [duro::expr {TUPLE FROM (T2 WHERE THE_LASTNAME(NAME) = "Potter")} $tx]

if {($a(NO) != 1) || ($a(NAME) != {PNAME Peter Potter})} {
    puts "T2 has wrong value"
    exit 2
}

duro::commit $tx

# Close DB environment
duro::env close $dbenv
