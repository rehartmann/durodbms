#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0" ${1+"$@"}

# Copyright (C) 2004 René Hartmann.
# See the file COPYING for redistribution information.

# $Id$

package require duro

proc dump_rtable {out t tx} {
    array set attrs [duro::table attrs $t $tx]
    foreach i [array names attrs] {
        lappend alist [list $i $attrs($i)]
    }
    puts -nonewline $out "duro::table create $t \{$alist\}"
    puts $out " \{[duro::table keys $t $tx]\} \$tx"

    set a [duro::array create $t $tx]
    duro::array foreach tpl $a {
        puts $out "duro::insert $t \{$tpl\} \$tx" 
    } $tx
    duro::array drop $a
}

if {$argc < 1 || $argc > 2} {
    puts "usage: durodump.tcl environment \[output_file\]"
    exit 1
}

if {$argc == 2} {
    set outfile [lindex $argv 1]
} else {
    set outfile restore.tcl
}

set out [open $outfile w]

puts $out "#!/bin/sh"
puts $out "# Execute tclsh from the user's PATH \\"
puts $out {exec tclsh "$0" ${1+"$@"}}
puts $out "package require duro"

set dbenvpath [lindex $argv 0]
puts $out "set dbenv \[duro::env open $dbenvpath\]"

set dbenv [duro::env open $dbenvpath]
set dbs [duro::env dbs $dbenv]
set tables {}

foreach db $dbs {
    puts $out "duro::db create $db \$dbenv"

    set tx [duro::begin $dbenv $db]

    puts $out "set tx \[duro::begin \$dbenv $db\]"
    foreach t [duro::tables -real $tx] {
        if {[lsearch -exact $tables $t] == -1} {
            dump_rtable $out $t $tx
            lappend tables $t
        }
    }
    puts $out {duro::commit $tx}
    duro::commit $tx
}

duro::env close $dbenv

puts $out "duro::env close \$dbenv"

close $out
