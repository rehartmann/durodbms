# $Id$

#
# Utility procedures for Durotcl
#

package provide duro 0.8

namespace eval duro {

namespace export ptable tables env begin commit rollback db table insert \
        update delete array operator call expr type

# Print all tuples in a table
proc ptable {tbl tx} {
    set arr [duro::array create $tbl $tx]
    duro::array foreach i $arr {
        puts $i
    } $tx
    duro::array drop $arr
}

# Return a list of all tables of a database
proc tables {db sys tx} {
    set tables ""
    set cond ""
    if {!$sys} {
        set cond "WHERE IS_USER"
    }

    duro::table expr t "(SYS_RTABLES $cond) \
            JOIN (SYS_DBTABLES WHERE DBNAME = \"$db\")" $tx
    set arr [duro::array create t $tx]
    set i 0
    duro::array foreach tpl $arr {
        ::array set a $tpl
        lappend tables $a(TABLENAME)
    } $tx
    duro::array drop $arr
    duro::table drop t $tx

    duro::table expr t "(SYS_VTABLES $cond) \
            JOIN (SYS_DBTABLES WHERE DBNAME = \"$db\")" $tx
    set arr [duro::array create t $tx]
    set i 0
    duro::array foreach tpl $arr {
        ::array set a $tpl
        lappend tables $a(TABLENAME)
    } $tx
    duro::array drop $arr
    duro::table drop t $tx

    return $tables
}

}
