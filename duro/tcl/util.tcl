# $Id$

#
# Utility procedures for Durotcl
#

package provide duro 0.8

namespace eval duro {

namespace export ptable env begin commit rollback db table insert \
        update delete array operator call expr type

# Print all tuples in a table
proc ptable {tbl tx} {
    set arr [duro::array create $tbl $tx]
    duro::array foreach i $arr {
        puts $i
    } $tx
    duro::array drop $arr
}

}
