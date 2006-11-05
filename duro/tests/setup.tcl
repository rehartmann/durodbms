set ::SETUP {
    set scriptdir [file dirname [info script]]

    if {[info exists ::env(LIBDUROTCL)]} {
        load $scriptdir/../$::env(LIBDUROTCL)
    } else {
        load $scriptdir/../libdurotcl.so
    }
    source $scriptdir/testutil.tcl

    # Create DB environment dir, ensure it's empty
    removeDirectory dbenv
    makeDirectory dbenv
}
