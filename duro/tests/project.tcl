#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test create, insert, update with projection tables
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env open tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

# Create real table
duro::table create R {
   {A INTEGER}
   {B STRING yo}
   {C STRING}
} {{A}} $tx

# Create project tables
duro::table expr -global P1 {R {ALL BUT B}} $tx
duro::table expr -global P2 {R {B}} $tx

# Insert tuple into real table
duro::insert R {A 1 B Bla C c} $tx
duro::insert R {A 2 B Blubb C d} $tx
duro::insert R {A 3 B Blubb C e} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

if {![catch {
    duro::table contains P1 {} $tx
}]} {
    puts "P1 contains empty tuple, but should not"
    exit 1
}

set tpl {A 1 C c}
if {![duro::table contains P1 $tpl $tx]} {
    puts "P1 does not contain $tpl, but should"
    exit 1
}

set tpl {A 1 C d}
if {[duro::table contains P1 $tpl $tx]} {
    puts "P1 contains $tpl, but should not"
    exit 1
}

# Insert tuple into virtual table
duro::insert P1 {A 4 C f} $tx

# Update table
duro::update R {A = 4} A 5 $tx

set a [duro::array create R {A asc} $tx]

checkarray $a { {A 1 B Bla C c} {A 2 B Blubb C d}
       {A 3 B Blubb C e} {A 5 B yo C f} } $tx

duro::array drop $a

set a [duro::array create P1 {A asc} $tx]

checkarray $a { {A 1 C c} {A 2 C d} {A 3 C e} {A 5 C f} } $tx

duro::array drop $a

set a [duro::array create P2 {B desc} $tx]

checkarray $a { {B yo} {B Blubb} {B Bla} } $tx

duro::array drop $a

# Drop tables
duro::table drop P1 $tx
duro::table drop P2 $tx
duro::table drop R $tx

duro::commit $tx
