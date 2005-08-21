#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test string operators
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create $dbenv TEST

set tx [duro::begin $dbenv TEST]

set len [duro::expr {LENGTH("Ab-")} $tx]
if {$len != 3} {
    error "wrong string length: $len"
}

set str [duro::expr {"\"\t\\"} $tx]
if {$str != "\"\t\\"} {
    error "wrong string value: $str"
}

set str [duro::expr {SUBSTRING("ABC-.+abc", 2, 3)} $tx]
if {$str != "C-."} {
    error "wrong string value: $str"
}

set str [duro::expr {SUBSTRING('ABC-.+ab', 2, 6)} $tx]
if {$str != "C-.+ab"} {
    error "wrong string value: $str"
}

set str [duro::expr {SUBSTRING("ABC-.+ab", 0, 8)} $tx]
if {$str != "ABC-.+ab"} {
    error "wrong string value: $str"
}

if {![catch {[duro::expr {SUBSTRING("ABC-.+ab", 2, 7)} $tx]}]} {
    error "should have failed"
}

if {![catch {[duro::expr {SUBSTRING("ABC-.+ab", 0, 9)} $tx]}]} {
    error "should have failed"
}

duro::commit $tx

# Close DB environment
duro::env close $dbenv
