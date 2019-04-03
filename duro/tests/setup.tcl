set ::SETUP {
    set scriptdir [file dirname [info script]]

    if {$::tcl_platform(platform) == "windows"} {
        load $scriptdir/../durotcl.dll
    } else {
        load [glob $scriptdir/../libdurotcl.so.*]
    }

    source $scriptdir/testutil.tcl

    if {[info exists env(DURO_STORAGE)] && $env(DURO_STORAGE) == "POSTGRESQL"} {
        catch {exec dropdb testdb}
        exec createdb testdb
        set dbenvname postgresql:///testdb
    } elseif {[info exists env(DURO_STORAGE)] && $env(DURO_STORAGE) == "FOUNDATIONDB"} {
		exec fdbcli < $scriptdir/fdbclear
        set dbenvname foundationdb://
	} else {
    	# Create DB environment dir, ensure it's empty
    	removeDirectory dbenv
    	makeDirectory dbenv
    	set dbenv [duro::env create [configure -tmpdir]/dbenv]
    	duro::env close $dbenv
    	set dbenvname [configure -tmpdir]/dbenv
    }
}
