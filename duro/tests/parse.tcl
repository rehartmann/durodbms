#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test handling of syntax errors
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

# Create real tables
duro::table create T {
   {K INTEGER}
   {S1 STRING}
} {{K}} $tx

# Suppress error msgs
proc dberror {msg} {
} 

if {![catch {duro::table expr TU {T UNION T UNION } $tx} msg]} {
    error "creating TU should fail, but succeeded"
}

# set errcode [lindex $errorCode 1]
# if {$errcode != "RDB_SYNTAX"} {
#     error "wrong error: $errcode"
# }

if {![catch {duro::table expr TX {EXTEND T { K } ADD (2*K AS K2} $tx} msg]} {
    error "creating TX should fail, but succeeded"
}

# set errcode [lindex $errorCode 1]
# if {$errcode != "RDB_SYNTAX"} {
#     error "wrong error: $errcode"
# }

if {![catch {duro::table expr TX {EXTEND T { K } ADD (2*X AS K2)} $tx} msg]} {
    error "creating TX should fail, but succeeded"
}

set errcode [lindex $errorCode 1]
if {![string match "ATTRIBUTE_NOT_FOUND_ERROR(*)" $errcode]} {
    error "wrong error: $errcode"
}

duro::commit $tx

# Close DB environment
duro::env close $dbenv
