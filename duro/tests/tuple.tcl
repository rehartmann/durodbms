#!/usr/bin/tclsh

# $Id$
#
# Test create, insert on table with tuple attribute
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

# Create table with tuple attribute
duro::table create T1 {
    {SCATTR STRING}
    {TPATTR {tuple {A INTEGER} {B STRING}}}
} {{SCATTR}} $tx

duro::table insert T1 {SCATTR Bla TPATTR {A 1 B Blubb}} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

set a [duro::array create T1 $tx]

checkarray $a { {SCATTR Bla TPATTR {A 1 B Blubb}} }

duro::array drop $a

# Drop table
duro::table drop T1 $tx

duro::commit $tx
