#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test non-unique (user) indexes
#

load .libs/libdurotcl.so

source tests/testutil.tcl

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

duro::table create T1 {
   {A INTEGER}
   {B STRING}
   {C STRING}
   {D STRING x}
} {{A}} $tx

duro::index create IX1 T1 {B asc} $tx

duro::insert T1 {A 1 B Bli C X} $tx
duro::insert T1 {A 2 B Bla C Y} $tx
duro::insert T1 {A 3 B Bla C X} $tx
duro::insert T1 {A 4 B Blubb C Y} $tx

duro::table expr t {T1 WHERE B="Bla"} $tx

set ta [duro::array create t {A asc} $tx]

set len [duro::array length $ta]
if {$len != 2} {
    puts "Array length is $len, should be 2"
    exit 1
}

set tpl [duro::array index $ta 0 $tx]
set stpl {A 2 B Bla C Y D x}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

set tpl [duro::array index $ta 1 $tx]
set stpl {A 3 B Bla C X D x}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

duro::array drop $ta
duro::table drop t $tx

duro::commit $tx

duro::env close $dbenv

set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set tpl [duro::expr {TUPLE FROM (T1 WHERE B="Bli")} $tx]
set stpl {B Bli A 1 C X D x}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

set tpl [duro::expr {TUPLE FROM (T1 WHERE A=4 AND B="Blubb")} $tx]
set stpl {A 4 B Blubb C Y D x}
if {![tequal $tpl $stpl]} {
    puts "Tuple should be $stpl, but is $tpl"
    exit 1
}

if {![duro::expr {IS_EMPTY (T1 WHERE B="Blubb" AND C="X")} $tx]} {
    puts "Expression should be empty, but is not"
    exit 1
}

duro::update T1 {B = "Bla"} A {5 - A} $tx
set a [duro::array create T1 {A asc} $tx]
checkarray $a {{A 1 B Bli C X D x} {A 2 B Bla C X D x}
        {A 3 B Bla C Y D x} {A 4 B Blubb C Y D x}} $tx
duro::array drop $a

# Must have no effect
duro::update T1 {B = "Bla" AND C = "Z"} D {"y"} $tx
set a [duro::array create T1 {A asc} $tx]
checkarray $a {{A 1 B Bli C X D x} {A 2 B Bla C X D x}
        {A 3 B Bla C Y D x} {A 4 B Blubb C Y D x}} $tx
duro::array drop $a

duro::update T1 {B = "Bla"} D {"y"} $tx
set a [duro::array create T1 {A asc} $tx]
checkarray $a {{A 1 B Bli C X D x} {A 2 B Bla C X D y}
        {A 3 B Bla C Y D y} {A 4 B Blubb C Y D x}} $tx
duro::array drop $a

duro::update T1 {B = "Bla" AND C = "X"} D {"z"} $tx
set a [duro::array create T1 {A asc} $tx]
checkarray $a {{A 1 B Bli C X D x} {A 2 B Bla C X D z}
        {A 3 B Bla C Y D y} {A 4 B Blubb C Y D x}} $tx
duro::array drop $a

# Must have no effect
duro::delete T1 {B = "Bla" AND C = "Z"} $tx
set a [duro::array create T1 {A asc} $tx]
checkarray $a {{A 1 B Bli C X D x} {A 2 B Bla C X D z}
        {A 3 B Bla C Y D y} {A 4 B Blubb C Y D x}} $tx
duro::array drop $a

duro::delete T1 {B = "Bla" AND C = "Y"} $tx
set a [duro::array create T1 {A asc} $tx]
checkarray $a {{A 1 B Bli C X D x} {A 2 B Bla C X D z}
        {A 4 B Blubb C Y D x}} $tx
duro::array drop $a

duro::delete T1 {B = "Bla"} $tx
set a [duro::array create T1 {A asc} $tx]
checkarray $a {{A 1 B Bli C X D x} {A 4 B Blubb C Y D x}} $tx
duro::array drop $a

duro::commit $tx
