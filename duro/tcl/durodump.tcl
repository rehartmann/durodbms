#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0" ${1+"$@"}

#
# $Id$
#
# Durodump - tool to dump tables to a file.
# Copyright (C) 2004-2012 Rene Hartmann.
# See the file COPYING for redistribution information.
#
# Synopsis
#
# durodump.tcl <var>envpath</var> [<var>output_file</var>]
#
# Description
#
# Durodump is a Tcl script which dumps all real user tables in the
# database environment
# specified by <var>envpath</var> to a file.
# If the name of the output file is not specified by the <var>output_file</var>
# argument, the output file will be named restore.tcl.

# Restoring the data
#
# The output file of Durodump is a Tcl script which must be run in order
# to restore the data. The environment directory which is to be restored
# can be passed as an argument. If no argument is given,
# the original environment directory is used.
# The environment directory must exist and is expected to be empty.
#
# Durodump is no longer part of the distribution, but still kept in the
# repository.
#

package require duro

proc dump_rtable {out t tx cr} {
    puts $out "set tx \[duro::begin \$dbenv [duro::txdb $tx]\]"
    if {$cr} {
        set alist [duro::table attrs $t $tx]
        puts -nonewline $out "duro::table create $t \{$alist\}"
        puts $out " \{[duro::table keys $t $tx]\} \$tx"
    }

    set a [duro::array create $t $tx]
    duro::array foreach tpl $a {
        puts $out "duro::insert $t \{$tpl\} \$tx" 
    } $tx
    puts $out {duro::commit $tx}
    duro::array drop $a
}

if {$argc == 0 || $argc > 2} {
    puts "usage: durodump.tcl environment \[output_file\]"
    exit 1
}

if {$argc == 2} {
    set outfile [lindex $argv 1]
} else {
    set outfile restore.tcl
}

set out [open $outfile w 0755]

puts $out "#!/bin/sh"
puts $out "# Execute tclsh from the user's PATH \\"
puts $out {exec tclsh "$0" ${1+"$@"}}
puts $out "# Created by Durodump"
puts $out "package require duro"

puts $out "if {\$argc > 1} {"
puts $out "    puts \"usage: $outfile environment\""
puts $out "    exit 1"
puts $out "}"

set dbenvpath [lindex $argv 0]
puts $out "if {\$argc == 1 } {"
puts $out "    set dbenv \[duro::env open -create \[lindex \$argv 0\]\]"
puts $out "} else {"
puts $out "    set dbenv \[duro::env open -create \{$dbenvpath\}\]"
puts $out "}"

set dbenv [duro::env open $dbenvpath]
set dbs [duro::env dbs $dbenv]
set tables {}

foreach db $dbs {
    puts $out "duro::db create \$dbenv $db"

    set tx [duro::begin $dbenv $db]

    foreach t [duro::tables -real $tx] {
        if {[lsearch -exact $tables $t] == -1} {
            dump_rtable $out $t $tx 1
            lappend tables $t
        }
    }
    duro::commit $tx
}

duro::env close $dbenv

puts $out "duro::env close \$dbenv"

close $out
