#
# $Id$
#

#
# Compare tuples
# (Works only with built-in attribute types)
#
proc tequal {t1 t2} {
    if {[llength $t1] != [llength $t2]} {
        return 0
    }

    array set ta1 $t1
    array set ta2 $t2

    foreach i [array names ta1] {
        if {![string equal $ta1($i) $ta2($i)]} {
            return 0
        }
    }
    return 1
}
