#!/usr/bin/tclsh

# $Id$
#
# Test create, insert, update with projection tables
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

# Create real table
duro::table create R {
   {INTATTR INTEGER}
   {STRATTR STRING yo}
} {{INTATTR}} $tx

# Create project tables
duro::table expr -global P1 {R {ALL BUT STRATTR}} $tx
duro::table expr -global P2 {R {STRATTR}} $tx

# Insert tuple into real table
duro::table insert R {INTATTR 1 STRATTR Bla} $tx
duro::table insert R {INTATTR 2 STRATTR Blubb} $tx
duro::table insert R {INTATTR 3 STRATTR Blubb} $tx

duro::commit $tx

# Close DB environment
duro::env close $dbenv

# Reopen DB environment
set dbenv [duro::env open tests/dbenv]

set tx [duro::begin $dbenv TEST]

# Insert tuple into virtual table
duro::table insert P1 {INTATTR 4} $tx

# Update virtual table
duro::table update R {INTATTR=4} INTATTR 5 $tx

set a [duro::array create R {INTATTR asc} $tx]

checkarray $a { {INTATTR 1 STRATTR Bla} {INTATTR 2 STRATTR Blubb}
       {INTATTR 3 STRATTR Blubb} {INTATTR 5 STRATTR yo} }

duro::array drop $a

set a [duro::array create P1 {INTATTR asc} $tx]

checkarray $a { {INTATTR 1} {INTATTR 2} {INTATTR 3} {INTATTR 5} }

duro::array drop $a

set a [duro::array create P2 {STRATTR desc} $tx]

checkarray $a { {STRATTR yo} {STRATTR Blubb} {STRATTR Bla} }

duro::array drop $a

# Drop tables
duro::table drop P1 $tx
duro::table drop P2 $tx
duro::table drop R $tx

duro::commit $tx
