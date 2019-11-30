#!/bin/sh
# Execute tclsh from the user's PATH \
exec tclsh "$0" ${1+"$@"}

package require Tcl 8.4
package require tcltest 2.2

set dir [file dirname [file normalize [info script]]]

if {$::tcl_platform(platform) == "windows"} {
    set ::env(PATH) $::env(PATH)\;$dir\\..\;$dir
} else {
    set ::env(LD_LIBRARY_PATH) $dir/..:$dir
}

#
# Option -postgresql [false|true] controls whether tests are also run
# with PostgreSQL
#
set pgargidx [lsearch $argv -postgresql]
set fdbargidx [lsearch $argv -foundationdb]
set targv [lreplace $argv $pgargidx [expr $fdbargidx+1]]

::tcltest::configure -testdir $dir
::tcltest::configure -tmpdir [::tcltest::configure -testdir]
eval ::tcltest::configure $targv
puts {Running Berkeley DB tests}
set env(DURO_STORAGE) BDB
::tcltest::runAllTests
if {[lindex $argv [expr $pgargidx+1]]=="true"} {
    # Run tests with PostgreSQL as storage engine
	puts {Running PostgreSQL tests}

    set dir [file dirname [file normalize [info script]]]

    if {$::tcl_platform(platform) == "windows"} {
        set ::env(PATH) $::env(PATH)\;$dir\\..\;$dir
    } else {
        set ::env(LD_LIBRARY_PATH) $dir/..:$dir
    }

    ::tcltest::configure -testdir $dir
    ::tcltest::configure -tmpdir [::tcltest::configure -testdir]
    eval ::tcltest::configure "$targv -skip {index oindex opt}"
    set env(DURO_STORAGE) POSTGRESQL
    ::tcltest::runAllTests
}
if {[lindex $argv [expr $fdbargidx+1]]=="true"} {
    # Run tests with FoundationDB as storage engine
	puts {Running FoundationDB tests}

    set dir [file dirname [file normalize [info script]]]

    if {$::tcl_platform(platform) == "windows"} {
        set ::env(PATH) $::env(PATH)\;$dir\\..\;$dir
    } else {
        set ::env(LD_LIBRARY_PATH) $dir/..:$dir
    }

    ::tcltest::configure -testdir $dir
    ::tcltest::configure -tmpdir [::tcltest::configure -testdir]
    eval ::tcltest::configure $targv
    set env(DURO_STORAGE) FOUNDATIONDB
    ::tcltest::runAllTests
}
