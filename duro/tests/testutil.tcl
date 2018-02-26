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

proc checkarray {a l tx} {
    set alen [duro::array length $a]
    if {$alen != [llength $l]} {
        error "# of tuples is $alen, expected [llength $l]"
    }
    for {set i 0} {$i < $alen} {incr i} {
        set t [duro::array index $a $i $tx]
        set xt [lindex $l $i]
        if {![tequal $t $xt]} {
            error "Tuple value is $t, expected $xt"
        }
    }
}   

proc duro_assert {exp tx} {
    if {![duro::expr $exp $tx]} {
        error "$exp is FALSE"
    }
}
