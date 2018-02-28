#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0" ${1+"$@"}

package require Tcl 8.4
package require tcltest 2.2

set dir [file dirname [file normalize [info script]]]

if {$::tcl_platform(platform) == "windows"} {
    set ::env(PATH) $::env(PATH)\;$dir\\..\\..\;$dir\\..
} else {
    set ::env(LD_LIBRARY_PATH) $dir/../..
}

set pgargidx [lsearch $argv -postgresql]
set targv [lreplace $argv $pgargidx [expr $pgargidx+1]]

::tcltest::configure -testdir $dir
::tcltest::configure -tmpdir [::tcltest::configure -testdir]
eval ::tcltest::configure $targv
::tcltest::runAllTests
