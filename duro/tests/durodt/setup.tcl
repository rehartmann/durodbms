if {[info exists env(DURO_STORAGE)] && $env(DURO_STORAGE) == "POSTGRESQL"} {
    set ::SETUP {
        catch {exec dropdb testdb}
        exec createdb testdb
        set ::dbenvname postgresql:///testdb
        exec [file dirname [info script]]/../../dli/durodt -e $::dbenvname << {
            create_db('D');
        }
    }
    set ::CLEANUP {
        exec dropdb testdb
    }
} elseif {[info exists env(DURO_STORAGE)] && $env(DURO_STORAGE) == "FOUNDATIONDB"} {
    set ::SETUP {
		exec fdbcli < [file dirname [info script]]/../fdbclear
        set ::dbenvname foundationdb://
        exec [file dirname [info script]]/../../dli/durodt -e $::dbenvname << {
            create_db('D');
        }
    }
    set ::CLEANUP {
        exec fdbcli < [file dirname [info script]]/../fdbclear
    }
} else {
    set ::SETUP {
        cd $::tcltest::temporaryDirectory
        removeDirectory dbenv
        makeDirectory dbenv
        set ::dbenvname dbenv
        exec [file dirname [info script]]/../../dli/durodt << {
            create_env('dbenv');
            create_db('D');
        }
    }
    set ::CLEANUP {
        removeDirectory dbenv
    }
}
