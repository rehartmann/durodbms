#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test creating, invoking, deleting operators
#

load .libs/libdurotcl.so

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv
set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

# Start transaction
set tx [duro::begin $dbenv TEST]

# Create overloaded update operator

duro::operator create strmul -updates {a} {a STRING b STRING} {
    append a $b
} $tx

duro::operator create strmul -updates {a} {a STRING b INTEGER} {
    set h $a
    for {set i 1} {$i < $b} {incr i} {
        append a $h
    }
} $tx

# Create read-only operator
duro::operator create concat -returns STRING {a STRING b STRING} {
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

set i X
set v foo
duro::call strmul v STRING 3 INTEGER $tx

if {![string equal $v foofoofoo]} {
    puts [format "result is %s, should be %s" $v foofoofoo]
    exit 1
}

if {$i != "X"} {
    puts "global variable was modified by operator"
    exit 1
}

# Invoke read-only operator
set v [duro::expr {concat("X", "Y")} $tx]

if {![string equal $v XY]} {
   puts "result is %s, should be %s" $v XY
   exit 1
}

# Destroy operator
duro::operator drop strmul $tx

# try to invoke deleted operator

if {![catch {
    duro::call strmul v STRING bar STRING $tx
}]} {
    puts "Operator invocation should fail, but succeded"
    exit 1
}

duro::commit $tx
