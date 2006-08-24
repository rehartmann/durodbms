#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0" ${1+"$@"}

set dir [file dirname [file normalize [info script]]]

set ::env(LD_LIBRARY_PATH) $dir/..:$dir

package require Tcl 8.4
package require tcltest 2.2
::tcltest::configure -testdir $dir
::tcltest::configure -tmpdir [::tcltest::configure -testdir]
eval ::tcltest::configure $argv
::tcltest::runAllTests
