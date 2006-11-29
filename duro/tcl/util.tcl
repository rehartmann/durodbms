# $Id$

#
# Utility procedures for Durotcl
#

package provide duro 0.12

namespace eval duro {

namespace export tables env begin commit rollback db table insert \
        update delete array operator call expr type

# Return a list of all tables of a database
proc tables {flag tx} {
    set db [duro::txdb $tx]
    set tables ""
    if {$flag == "-user" || $flag == "-real" || $flag == "-virtual"} {
        set cond "WHERE IS_USER"
    } else {
        set cond ""
    }

    if {$flag != "-virtual"} {
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
    }

    if {$flag != "-real"} {
        duro::table expr t "(SYS_VTABLES $cond) \
                JOIN (SYS_DBTABLES WHERE DBNAME = \"$db\")" $tx
        set arr [duro::array create t $tx]
        duro::array foreach tpl $arr {
            ::array set a $tpl
            lappend tables $a(TABLENAME)
        } $tx
        duro::array drop $arr
        duro::table drop t $tx
    }

    return $tables
}

}
