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
