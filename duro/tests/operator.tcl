#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test create, insert, UNWRAP on table with tuple attribute
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

# Create update operators
duro::operator strmul -updates {a} {a STRING b STRING} {
    append a $b
} $tx

duro::operator strmul -updates {a} {a STRING b INTEGER} {
    set h $a
    for {set i 1} {$i < $b} {incr i} {
        append a $h
    }
} $tx

# Create read-only operator
duro::operator concat -returns STRING {a STRING b STRING} {
    return $a$b
} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

# Invoke update operator
set v foo
duro::call strmul v STRING bar STRING $tx

if {![string equal $v foobar]} {
    puts [format "result is %s, should be %s" $v foobar]
    exit 1
}

set v foo
duro::call strmul v STRING 3 INTEGER $tx

if {![string equal $v foofoofoo]} {
    puts [format "result is %s, should be %s" $v foofoofoo]
    exit 1
}

# Invoke read-only operator
set v [duro::expr {concat("X", "Y")} $tx]

if {![string equal $v XY]} {
    puts "result is %s, should be %s" $v XY
    exit 1
}

duro::commit $tx
