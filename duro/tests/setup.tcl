set ::SETUP {
    set scriptdir [file dirname [info script]]

    if {$::tcl_platform(platform) == "windows"} {
        load $scriptdir/../durotcl.dll
    } else {
        load [glob $scriptdir/../libdurotcl.so.*]
    }

    source $scriptdir/testutil.tcl

    # Create DB environment dir, ensure it's empty
    removeDirectory dbenv
    makeDirectory dbenv
}
