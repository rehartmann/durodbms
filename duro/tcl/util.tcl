# $Id$

#
# Utility procedures for Durotcl
#

namespace eval duro {

# Print the contents of a table
proc ptable {tbl tx} {
    set arr [duro::array create $tbl $tx]
    duro::array foreach i $arr {
        puts $i
    }
    duro::array drop $arr
}

}
