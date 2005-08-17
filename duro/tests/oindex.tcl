#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test ordered indexes
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

duro::table create T1 {
   {A INTEGER}
   {B STRING}
   {C STRING}
   {D STRING x}
} {{A}} $tx

duro::index create IX1 T1 {B asc} $tx

duro::insert T1 {A 1 B Blb C Y} $tx
duro::insert T1 {A 2 B Blb C Y} $tx
duro::insert T1 {A 3 B Bla C X} $tx
duro::insert T1 {A 4 B Bli C X} $tx
duro::insert T1 {A 5 B Blubb C Y} $tx

duro::table expr t {T1 WHERE B = "Blb"} $tx

set a [duro::array create t {A asc} $tx]

checkarray $a {{A 1 B Blb C Y D x} {A 2 B Blb C Y D x}} $tx

duro::array drop $a

duro::table drop t $tx

duro::table expr t {T1 WHERE B >= "Blab" AND B < "Blubb" } $tx

set plan [duro::table showplan t $tx]
if {![string match "*IX1*" $plan]} {
    error "IX1 should be used, but is not"
}

set a [duro::array create t {A asc} $tx]

checkarray $a {{A 1 B Blb C Y D x} {A 2 B Blb C Y D x}
        {A 4 B Bli C X D x}} $tx

duro::array drop $a

duro::table drop t $tx

duro::table expr t {T1 WHERE B > "Blb" } $tx

set plan [duro::table showplan t $tx]
if {![string match "*IX1*" $plan]} {
    error "IX1 should be used, but is not"
}

set a [duro::array create t {A asc} $tx]

checkarray $a {{A 4 B Bli C X D x} {A 5 B Blubb C Y D x}} $tx

duro::array drop $a

duro::table drop t $tx

duro::table expr t {T1 WHERE B < "Blo" } $tx

set a [duro::array create t {A asc} $tx]

checkarray $a {{A 1 B Blb C Y D x} {A 2 B Blb C Y D x}
        {A 3 B Bla C X D x} {A 4 B Bli C X D x}} $tx

duro::array drop $a

duro::table drop t $tx

duro::update T1 {B >= "Blb" AND B <= "Blo"} D {"u"} $tx

duro::update T1 {B > "Blb" AND B <= "Blo"} D {"v"} $tx

set a [duro::array create T1 {A asc} $tx]

checkarray $a {{A 1 B Blb C Y D u} {A 2 B Blb C Y D u}
        {A 3 B Bla C X D x} {A 4 B Bli C X D v}
        {A 5 B Blubb C Y D x}} $tx

duro::array drop $a

duro::delete T1 {B > "Blb" AND B < "Blubb"} $tx

set a [duro::array create T1 {A asc} $tx]

checkarray $a {{A 1 B Blb C Y D u} {A 2 B Blb C Y D u}
        {A 3 B Bla C X D x} {A 5 B Blubb C Y D x}} $tx

duro::array drop $a

#
# Test sorting by index
#

duro::update T1 {A = 2} B {"Ble"} $tx

set a [duro::array create T1 {B asc} $tx]

checkarray $a {{A 3 B Bla C X D x} {A 1 B Blb C Y D u} {A 2 B Ble C Y D u}
        {A 5 B Blubb C Y D x}} $tx

duro::array drop $a

duro::table expr v {T1 WHERE A > 1} $tx

set a [duro::array create v {B asc} $tx]

checkarray $a {{A 3 B Bla C X D x} {A 2 B Ble C Y D u}
        {A 5 B Blubb C Y D x}} $tx

duro::array drop $a

duro::commit $tx

duro::env close $dbenv
