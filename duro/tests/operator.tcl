#!/usr/bin/tclsh

# $Id$
#
# Test create, insert, UNWRAP on table with tuple attribute
#

load .libs/libdurotcl.so duro

source tcl/util.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv
set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

# Create update operator
duro::operator append -updates {a} $tx {{a STRING} {b STRING}} {
    set a "$a$b"
}

# Create read-only operator
duro::operator concat -returns STRING $tx {{a STRING} {b STRING}} {
    return $a$b
}

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

# Invoke update operator
set v foo
duro::call append $tx v STRING bar STRING

if {![string equal $v foobar]} {
    puts [format "result is %s, should be %s" $v foobar]
    exit 1
}

# Invoke read-only operator
set v [duro::expr {concat("X", "Y")} $tx]

if {![string equal $v XY]} {
    puts "result is %s, should be %s" $v XY
    exit 1
}

duro::commit $tx
