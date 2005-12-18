#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0"

# $Id$
#
# Test conversion operators
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

# Create real table
duro::table create T1 {
   {A INTEGER}
   {B DOUBLE}
   {C STRING}
} {{A}} $tx

# Insert tuple
duro::insert T1 {A 1 B 2.0 C 03} $tx

duro::table expr -global X {EXTEND T1 ADD (
        A AS A_I, DOUBLE(A) * 1.1 AS A_R, "S" || STRING(A) AS A_S,
        INTEGER(B) AS B_I, B * 1.1 AS B_R, "S" || STRING(B) AS B_S,
        INTEGER(C) AS C_I, DOUBLE(C) * 1.1 AS C_R, "S" || C AS C_S)
} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set tpl [duro::expr {TUPLE FROM X} $tx]

set stpl {A 1 B 2.0 C 03 A_I 1 A_R 1.1 A_S S1 B_I 2 B_R 2.2 B_S S2
        C_I 3 C_R 3.3 C_S S03}
if {![tequal $tpl $stpl]} {
    error "Tuple should be $tpl, but is $stpl"
}

duro::commit $tx
