#!/usr/bin/tclsh

# $Id$
#
# Test for DIVIDEBY PER
# (Test case taken from an example in: C. Date, An Introduction to Database Systems,
# 7th edition)
#

load .libs/libdurotcl.so duro

source tcl/util.tcl

proc tequal {t1 t2} {
    return [string equal [lsort $t1] [lsort $t2]]
}

proc checkarray {a l} {
    set alen [duro::array length $a]
    if {$alen != [llength $l]} {
        puts "# of tuples is $alen, expected [llength $l]"
        exit 1
    }
    for {set i 0} {$i < $alen} {incr i} {
        set t [duro::array index $a $i]
        set xt [lindex $l $i]
        if {![tequal $t $xt]} {
            puts "Tuple value is $t, expected $xt"
            exit 1
        }
    }
}   

# Create DB environment
file delete -force tests/dbenv
file mkdir tests/dbenv

set dbenv [duro::env create tests/dbenv]

# Create Database
duro::db create TEST $dbenv

set tx [duro::begin $dbenv TEST]

# Create tables

duro::table create S {
    {SNO STRING}
} {{SNO}} $tx

duro::table create P {
    {PNO STRING}
} {{PNO}} $tx

duro::table create SP {
    {SNO STRING}
    {PNO STRING}
} {{SNO PNO}} $tx

duro::table expr -global DIV {S DIVIDEBY P PER SP} $tx

duro::table insert S {SNO S1} $tx
duro::table insert S {SNO S2} $tx
duro::table insert S {SNO S3} $tx
duro::table insert S {SNO S4} $tx
duro::table insert S {SNO S5} $tx

duro::table insert P {PNO P1} $tx

duro::table insert SP {SNO S1 PNO P1} $tx
duro::table insert SP {SNO S1 PNO P2} $tx
duro::table insert SP {SNO S1 PNO P3} $tx
duro::table insert SP {SNO S1 PNO P4} $tx
duro::table insert SP {SNO S1 PNO P5} $tx
duro::table insert SP {SNO S1 PNO P6} $tx
duro::table insert SP {SNO S2 PNO P1} $tx
duro::table insert SP {SNO S2 PNO P2} $tx
duro::table insert SP {SNO S3 PNO P2} $tx
duro::table insert SP {SNO S4 PNO P2} $tx
duro::table insert SP {SNO S4 PNO P4} $tx
duro::table insert SP {SNO S4 PNO P5} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set a [duro::array create DIV {SNO asc} $tx]
checkarray $a { {SNO S1} {SNO S2} }
duro::array drop $a

duro::table delete P {PNO = "P1"} $tx
duro::table insert P {PNO P2} $tx
duro::table insert P {PNO P4} $tx

set a [duro::array create DIV {SNO asc} $tx]
checkarray $a { {SNO S1} {SNO S4} }
duro::array drop $a

duro::table insert P {PNO P1} $tx
duro::table insert P {PNO P3} $tx
duro::table insert P {PNO P5} $tx
duro::table insert P {PNO P6} $tx

set a [duro::array create DIV $tx]
checkarray $a { {SNO S1} }
duro::array drop $a

# Drop tables

duro::table drop S $tx
duro::table drop P $tx
duro::table drop SP $tx

duro::commit $tx
