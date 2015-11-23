#!/bin/sh
# Execute wish from the user's PATH \
exec wish "$0" ${1+"$@"}

# Duroadmin - GUI administration tool for Duro.
# Copyright (C) 2004-2009, 2012-2014 Rene Hartmann.
# See the file COPYING for redistribution information.

set duro_version 1.1

package require -exact duro $duro_version
package require Tktable

# Global variables:
#
# dbenv		current DB environment ID
# db		currently selected database
# tableattrs	list of table attributes
# tablekey      table key (attribute list)
# tabletypes    array which maps table attributes to their types
# tabledefvals  array which maps table attributes to their types
# ltables	list of local tables
# types         list of scalar types
# keyvals	array which maps (row,key attribute) to key value
#

#
# Read log msgs from log file and write them into error log window
#
proc read_log {} {
    # Do nothing if the window is not mapped
    if {[wm state .errlog] != "normal"} {
        return
    }

    set data [read $::errf]
    .errlog.msgs configure -state normal
    .errlog.msgs insert end $data
    .errlog.msgs configure -state disabled

    # Read data again in 5 seconds
    after 5000 read_log
}

#
# Make error log window visible
#
proc show_errlog {} {
    wm state .errlog normal
}

proc show_tables {} {
    if {$::dbenv == "" || $::db == ""} {
        return
    }

    .tables delete 0 end

    # Read global tables
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        set tables [lsort [duro::tables \
                [expr {$::showsys ? "-all" : "-user"}] $tx]]
        duro::commit $tx
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
    }

    # Add local tables
    set tables [concat $tables [lsort $::ltables]]

    foreach tb $tables {
        .tables insert end $tb
    }
    .mbar.db entryconfigure 4 -state disabled
}

proc add_db {newdb} {
    if {$::db == ""} {
        # Delete old entries
        $::dbmenu delete 0

        set ::db $newdb
        .mbar.db.create entryconfigure 2 -state normal
        .mbar.db.create entryconfigure 3 -state normal
        .mbar.db.drop entryconfigure 1 -state normal
        .mbar.db entryconfigure 5 -state normal
    }

    # Add menu entry
    $::dbmenu add radiobutton -label $newdb -variable db -command show_tables

    .dbsframe.dbs configure -state normal
    .dbsframe.dbl configure -state normal
}

proc open_env_path {envpath} {
    if {[catch {
        set ::dbenv [duro::env open $envpath]
    } msg]} {
        if {[tk_messageBox -type retrycancel -title "Error" \
                -message "$msg - retry with recover option?" -icon error] \
                == {cancel}} {
            set ::dbenv ""
            return
        }
        if {[catch {
            set ::dbenv [duro::env open -recover $envpath]
        } msg]} {
            tk_messageBox -type ok -title "Error" -message $msg -icon error
            set ::dbenv ""
            return
        }
    }
    duro::env seterrfile $::dbenv $::errfile
    .mbar.file entryconfigure Close* -state normal

    # Get databases
    set dbs [duro::env dbs $::dbenv]
    wm title . "[file tail $envpath] - Duroadmin"

    # Add new entries
    foreach i $dbs {
        add_db $i
    }

    # Activate first DB
    show_tables

    .mbar.db.create entryconfigure 1 -state normal
}

proc open_env {} {
    set envpath [tk_chooseDirectory -mustexist true]
    if {$envpath == ""} {
        return
    }

    if {$::dbenv != ""} {
        close_env
    }

    open_env_path $envpath
}

proc create_env {} {
    set envpath [tk_chooseDirectory]
    if {$envpath == ""} {
        return
    }

    if {$::dbenv != ""} {
        close_env
    }

    if {[catch {
        if {![file exists $envpath]} {
            file mkdir $envpath
        }

        set ::dbenv [duro::env create $envpath]
        duro::env seterrfile $::dbenv $::errfile
    } msg]} {
        tk_messageBox -type ok -title "Error" -message $msg -icon error
        return
    }

    wm title . "Duroadmin - $envpath"

    .mbar.db.create entryconfigure 1 -state normal
    .mbar.file entryconfigure Close* -state normal
}

proc close_env {} {
    duro::env close $::dbenv
    set ::dbenv ""
    set ::db ""

    $::dbmenu entryconfigure 0 -label ""
    $::dbmenu delete 1 end
    .tables delete 0 end
    .mbar.file entryconfigure Close* -state disabled

    .dbsframe.dbs configure -state disabled
    .dbsframe.dbl configure -state disabled
    .mbar.db.create entryconfigure 2 -state disabled
    .mbar.db.create entryconfigure 2 -state disabled
}

proc clear_bottom_row {} {
    set rowcount [.tableframe.table cget -rows]
    set colcount [llength $::tableattrs]
    if {$colcount == 0} {
        set colcount 1
    }
    
    for {set i 0} {$i < $colcount} {incr i} {
        .tableframe.table set [expr {$rowcount - 1}],$i ""
    }
}

proc set_row {row tpl} {
    for {set j 0} {$j < [llength $::tableattrs]} {incr j} {
        array set ta $tpl
        set attrname [lindex $::tableattrs $j]
        set s $ta($attrname)
        if {$::tabletypes($attrname) == "binary"} {
            set s "(binary)"
        } elseif {$::tabletypes($attrname) == "boolean"} {
            set s [expr {$s ?  "TRUE" : "FALSE"}]
        }
        .tableframe.table set $row,$j $s
    }
    if {$::tableattrs == {}} {
        .tableframe.table set $row,0 ""
    }
}

proc add_tuples {arr tx rowcount} {
    .tableframe.table configure -rows [expr {$rowcount + 2}]
    for {set i 0} {$i < $rowcount} {incr i} {
        if {[catch {set tpl [duro::array index $arr $i $tx]} errmsg]} {
            if {[llength $::errorCode] != 2
                    || ![string match "not_found_error(*)" \
                            [lindex $::errorCode 1]]} {
                error $errmsg
            }
            break
        }
        set_row [expr {$i + 1}] $tpl
        array set ta $tpl
        foreach k $::tablekey {
            set ::keyvals($i,$k) $ta($k)
        }
    }
    if {$i < $rowcount} {
        .morebutton configure -state disabled
        .tableframe.table configure -rows [expr {$i + 2}]
    } else {
        .morebutton configure -state normal
    }
}

proc show_table {} {
    set table [.tables get anchor]
    if {$table == ""} {
        return
    }

    catch {unset ::keyvals}
    catch {unset ::modified}

    if {[catch {
        set tx [duro::begin $::dbenv $::db]

        array unset ::tabletypes
        array unset ::tabledefvals
        foreach i [duro::table attrs $table $tx] {
            set ::tabletypes([lindex $i 0]) [lindex $i 1]
            if {[llength $i] > 2} {
                set ::tabledefvals([lindex $i 0]) [lindex $i 2]
            } else {
                set ::tabledefvals([lindex $i 0]) {}
            }
        }
        set ::tableattrs [array names ::tabletypes]
        set ::tablekey [lindex [duro::table keys $table $tx] 0]

        .tableframe.table configure -state normal
        pack propagate . 0
        set cols [llength $::tableattrs]
        if {$cols == 0} {
            set cols 1
        }
        .tableframe.table configure -cols $cols -colwidth 16
        .tableframe.table configure -rows 2

        # Set table heading
        if {$::tableattrs != {}} {
            for {set i 0} {$i < [llength $::tableattrs]} {incr i} {
                .tableframe.table set 0,$i [lindex $::tableattrs $i]
            }
        } else {
            .tableframe.table set 0,0 ""
        }

        # Set values - # of rows displayed is limited to the value
        # of duroadmin(initrows)

        set arr [duro::array create $table $tx]
        add_tuples $arr $tx $::duroadmin(initrows)
        duro::array drop $arr
        duro::commit $tx
    } msg]} {
        catch {duro::array drop $arr}
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
        return
    }

    # Clear last row, which is for entering new rows
    clear_bottom_row

    .mbar.db.drop entryconfigure 2 -state normal
    if {[.tableframe.table cget -rows] > 2} {
        .mbar.db entryconfigure 3 -state normal
    } else {
        .mbar.db entryconfigure 3 -state disabled
    }
    .mbar.db entryconfigure 4 -state normal
    .tableframe.table activate 1,0
}

proc more_tuples {} {
    set table [.tables get anchor]
    if {$table == ""} {
        return
    }

    if {[catch {
        set tx [duro::begin $::dbenv $::db]

        # Set values - # of rows displayed is limited to the value
        # of duroadmin(initrows)
        set arr [duro::array create $table $tx]
        add_tuples $arr $tx [expr {[.tableframe.table cget -rows]
                + $::duroadmin(initrows)}]

        duro::array drop $arr
        duro::commit $tx
    } msg]} {
        catch {duro::array drop $arr}
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
        return
    }
}

proc create_db {} {
    toplevel .dialog
    wm title .dialog "Create database"
    wm geometry .dialog "+300+300"

    set ::newdbname ""

    label .dialog.l -text "Database name:"
    entry .dialog.dbname -textvariable newdbname

    set ::action ok
    frame .dialog.buttons
    button .dialog.buttons.ok -text OK -command {set action ok}
    button .dialog.buttons.cancel -text Cancel -command {set action cancel}

    pack .dialog.buttons -side bottom -padx 5 -pady 5
    pack .dialog.buttons.ok .dialog.buttons.cancel -side left
    pack .dialog.l .dialog.dbname -side left -padx 5 -pady 5

    focus .dialog.dbname

    set done 0
    grab .dialog
    while {!$done} {
        tkwait variable action
        if {$::action != "ok"} {
            destroy .dialog
            return
        }
        if {$::newdbname == ""} {
            tk_messageBox -type ok -title "Error" -parent .dialog \
                    -message "Please enter a database name." -icon warning
            continue
        }

        # Create database
        if {[catch {
            duro::db create $::dbenv $::newdbname
            set done 1
        } msg]} {
            tk_messageBox -type ok -title "Error" -message $msg -icon error \
                    -parent .dialog
        }
    }
    destroy .dialog
    add_db $::newdbname
    set ::db $::newdbname
    .mbar.db.drop entryconfigure 1 -state normal
    show_tables
}

proc rename_table {} {
    set table [.tables get anchor]

    toplevel .dialog
    wm title .dialog "Rename table"
    wm geometry .dialog "+300+300"

    set ::newtablename ""

    label .dialog.l -text "New table name:"
    entry .dialog.tablename -textvariable newtablename

    set ::action ok
    frame .dialog.buttons
    button .dialog.buttons.ok -text OK -command {set action ok}
    button .dialog.buttons.cancel -text Cancel -command {set action cancel}

    pack .dialog.buttons -side bottom -padx 5 -pady 5
    pack .dialog.buttons.ok .dialog.buttons.cancel -side left
    pack .dialog.l .dialog.tablename -side left -padx 5 -pady 5

    focus .dialog.tablename

    set done 0
    grab .dialog
    while {!$done} {
        tkwait variable action
        if {$::action != "ok"} {
            destroy .dialog
            return
        }
        if {$::newtablename == ""} {
            tk_messageBox -type ok -title "Error" -parent .dialog
                    -message "Please enter a table name." -icon warning
            continue
        }

        # Rename table
        if {[catch {
            set tx [duro::begin $::dbenv $::db]
            duro::table rename $table $::newtablename $tx
            duro::commit $tx
        } msg]} {
            catch {duro::rollback $tx}
            tk_messageBox -type ok -title "Error" -message $msg -icon error \
                    -parent .dialog
            continue
        }
        set done 1
    }
    destroy .dialog
    .mbar.db.drop entryconfigure 1 -state normal

    # If the table is local, replace it in the list of local tables
    set i [lsearch -exact $::ltables $table]
    if {$i != -1} {
        set ::ltables [lreplace $::ltables $i $i $::newtablename]
    }
    show_tables
}

proc get_types {tx} {
    set types {string boolean integer float binary}

    # Add user-defined types
    set tnames [duro::expr {sys_types {typename}} $tx]
    foreach i $tnames {
        lappend types [lindex $i 1]
    }

    return $types
}

proc set_attr_row {r} {
    set menucmd [concat "tk_optionMenu .dialog.tabledef.type$r ::type($r)" \
            $::types]
    if {$r == 0} {
        set ::mw [eval $menucmd]
    } else {
        eval $menucmd
    }
    set keycount [expr {[.dialog.tabledef cget -cols] - 2}]

    .dialog.tabledef window configure [expr $r + 1],1 \
            -window .dialog.tabledef.type$r
    set t$r string

    for {set i 0} {$i < $keycount} {incr i} {
        checkbutton .dialog.tabledef.key$r,$i -variable key($r,$i)
        .dialog.tabledef window configure [expr $r + 1],[expr $i + 2] \
                -window .dialog.tabledef.key$r,$i
    }

    set h [expr {-[winfo reqheight .dialog.tabledef.type$r] - 2 *
                [.dialog.tabledef.key0,0 cget -pady]}]
    .dialog.tabledef height [expr $r + 1] $h
}

proc add_attr_row {} {
    set rowcount [.dialog.tabledef cget -rows]
    .dialog.tabledef configure -rows [expr {$rowcount + 1}]
    set_attr_row [expr {$rowcount - 1}]
}

proc add_key {} {
    set rowcount [.dialog.tabledef cget -rows]
    set colcount [.dialog.tabledef cget -cols]
    set keycount [expr {$colcount - 2}]

    .dialog.tabledef configure -cols [expr {$colcount + 1}]
    set ::tabledef(0,$colcount) "Key #[expr $keycount + 1]"
    for {set i 0} {$i < $rowcount - 1} {incr i} {
        checkbutton .dialog.tabledef.key$i,$keycount \
                -variable key($i,$keycount)
        .dialog.tabledef window configure [expr $i + 1],[expr {$keycount + 2}] \
                -window .dialog.tabledef.key$i,$keycount
    }
}

proc create_rtable {} {
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        set ::types [get_types $tx]
        duro::commit $tx
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
        return
    }

    toplevel .dialog
    wm title .dialog "Create real table"
    wm geometry .dialog "+300+300"

    set ::newtablename ""

    label .dialog.l -text "Table name:"
    entry .dialog.tablename -textvariable newtablename
    set ::tableflag -global
    checkbutton .dialog.global -text "Global" -variable tableflag \
            -onvalue -global -offvalue -local

    set attrcount 4

    table .dialog.tabledef -titlerows 1 -rows [expr {$attrcount +1}] \
            -cols 3 -variable tabledef -anchor w

    set ::action ok
    frame .dialog.buttons
    button .dialog.buttons.ok -text OK -command {set action ok}
    button .dialog.buttons.cancel -text Cancel -command {set action cancel}
    button .dialog.buttons.addattr -text "Add attribute" -command add_attr_row
    button .dialog.buttons.addkey -text "Add key" -command add_key

    set ::tabledef(0,0) "Attribute name"
    set ::tabledef(0,1) Type
    set ::tabledef(0,2) "Key #1"

    for {set i 0} {$i < $attrcount} {incr i} {
        set_attr_row $i
    }

    .dialog.tabledef width 0 [string length ::tabledef(0,0)]

    # Change type in line #1 to boolean to set width
    $::mw invoke 1
    .dialog.tabledef width 1 [expr {-[winfo reqwidth .dialog.tabledef.type0] - 2 *
            [.dialog.tabledef.type0 cget -pady]}]

    # Change type back
    $::mw invoke 0

    .dialog.tabledef.key0,0 select

    pack .dialog.buttons -side bottom -padx 5 -pady 5
    pack .dialog.buttons.ok .dialog.buttons.cancel \
            .dialog.buttons.addattr .dialog.buttons.addkey -side left
    pack .dialog.tabledef -side bottom -anchor w -padx 5 -pady 5
    pack .dialog.l .dialog.tablename .dialog.global -side left -padx 5 -pady 5

    focus .dialog.tablename

    set done 0
    grab .dialog
    while {!$done} {
        tkwait variable action
        if {$::action != "ok"} {
            destroy .dialog
            return
        }
        if {$::newtablename == ""} {
            tk_messageBox -type ok -title "Error" -parent .dialog \
                    -message "Please enter a table name." -icon warning
            continue
        }

        # Build lists for attributes and keys
        set attrs {}

        set attrcount [expr {[.dialog.tabledef cget -rows] - 1}]
        set keycount [expr {[.dialog.tabledef cget -cols] - 2}]

        for {set i 0} {$i < $attrcount} {incr i} {
            if {[info exists ::tabledef([expr $i + 1],0)]} {
                set attrname $::tabledef([expr $i + 1],0)
                if {$attrname != ""} {
                    lappend attrs [list $attrname $::type($i)]
                }
            }
        }

        set keys {}
        for {set i 0} {$i < $keycount} {incr i} {
            set key {}
            for {set j 0} {$j < $attrcount} {incr j} {
                if {[info exists ::tabledef([expr $j + 1],0)]} {
                    set attrname $::tabledef([expr $j + 1],0)
                    if {$attrname != ""} {
                        if {$::key($j,$i)} {
                            lappend key $attrname
                        }
                    }
                }
            }
            if {$i == 0 || $key != {}} {
                lappend keys $key
            }
        }

        if {[catch {
            # Create table
            set tx [duro::begin $::dbenv $::db]
            duro::table create $::tableflag $::newtablename $attrs $keys $tx
            duro::commit $tx
            set done 1
        } msg]} {
            catch {duro::rollback $tx}
            tk_messageBox -type ok -title "Error" -message $msg -icon error \
                    -parent .dialog
        }
     }

    if {$::tableflag == "-local"} {
        lappend ::ltables $::newtablename
    }
    show_tables

    destroy .dialog
}

proc create_vtable {} {
    toplevel .dialog
    wm title .dialog "Create virtual table"
    wm geometry .dialog "+300+300"

    set ::newtablename ""

    label .dialog.l -text "Table name:"
    entry .dialog.tablename -textvariable newtablename
    set ::tableflag -global
    checkbutton .dialog.global -text "Global" -variable tableflag \
            -onvalue -global -offvalue -local
    label .dialog.defl -text "Table definition:"
    text .dialog.tabledef

    set ::action ok
    frame .dialog.buttons
    button .dialog.buttons.ok -text OK -command {set action ok}
    button .dialog.buttons.cancel -text Cancel -command {set action cancel}

    pack .dialog.buttons -side bottom -padx 5 -pady 5
    pack .dialog.buttons.ok .dialog.buttons.cancel -side left
    pack .dialog.tabledef -side bottom -padx 5
    pack .dialog.defl -side bottom -anchor w -padx 5
    pack .dialog.l .dialog.tablename .dialog.global -side left -padx 5 -pady 5

    focus .dialog.tablename

    set done 0
    grab .dialog
    while {!$done} {
        tkwait variable action
        if {$::action != "ok"} {
            destroy .dialog
            return
        }
        if {$::newtablename == ""} {
            tk_messageBox -type ok -title "Error" -parent .dialog \
                    -message "Please enter a table name." -icon warning
            continue
        }

        if {[catch {
            # Create virtual table
            set tx [duro::begin $::dbenv $::db]
            duro::table expr $::tableflag $::newtablename \
                    [.dialog.tabledef get 1.0 end] $tx
            duro::commit $tx
            set done 1
        } msg]} {
            catch {duro::rollback $tx}
            tk_messageBox -type ok -title "Error" -message $msg -icon error \
                    -parent .dialog
        }
    }

    if {$::tableflag == "-local"} {
        lappend ::ltables $::newtablename
    }
    show_tables

    destroy .dialog
}

proc drop_db {} {
    if {[tk_messageBox -type okcancel -title "Drop database" \
            -message "Drop database \"$::db\"?" -icon question] != "ok"} {
        return
    }

    duro::db drop $::dbenv $::db

    $::dbmenu delete $::db
    set ::db [$::dbmenu entrycget 0 -value]
    if {$::db == ""} {
        .dbsframe.dbs configure -state disabled
        .dbsframe.dbl configure -state disabled
        .mbar.db.create entryconfigure 2 -state disabled
        .mbar.db.drop entryconfigure 1 -state disabled
        .mbar.db entryconfigure 5 -state disabled
    }
}

proc drop_table {} {
    set table [.tables get anchor]

    if {[tk_messageBox -type okcancel -title "Drop table" \
            -message "Drop table \"$table\"?" -icon question] != "ok"} {
        return
    }

    # Drop table
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        duro::table drop $table $tx
        duro::commit $tx

        .tables delete anchor
        .mbar.db.drop entryconfigure 2 -state disabled
        .mbar.db entryconfigure 4 -state disabled

        # If the table is local, delete it from the list of local tables
        set ti [lsearch $::ltables $table]
        if {$ti != -1} {
            lreplace ::ltables $ti $ti
        }

        # Clear table
        .tableframe.table configure -rows 1
        set colcount [.tableframe.table cget -cols]
        for {set i 0} {$i < $colcount} {incr i} {
            .tableframe.table set 0,$i ""
        }
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
    }
}

proc insert_tuple {} {
    set table [.tables get anchor]
    set rowcount [.tableframe.table cget -rows]
    set sysval 0

    .tableframe.table configure -rows [expr $rowcount + 1]
    clear_bottom_row
    catch {unset ::modified([expr $rowcount - 2])}
    .tableframe.table activate $rowcount,0

    # Build tuple
    set tpl ""
    set i 0
    foreach attr $::tableattrs {
        # Ignore value if there is a default of serial() or similar
        if {![string match {*()} $::tabledefvals($attr)]} {
            lappend tpl $attr [.tableframe.table get [expr {$rowcount - 1}],$i]
        } else {
            set sysval 1
        }
        incr i
    }

    # Insert tuple
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        duro::insert $table $tpl $tx
        duro::commit $tx
    } msg]} {
        catch {duro::rollback $tx}
        .tableframe.table configure -rows $rowcount
        tk_messageBox -type ok -title "Error" -message $msg -icon error
        return
    }
    if {$sysval} {
        show_table
    } else {
	    array set ta $tpl
	    foreach i $::tablekey {
	        set ::keyvals([expr {$rowcount - 2}],$i) $ta($i)
	    }
	}
}

proc quote_str {str} {
    set res "\""
    for {set i 0} {$i < [string length $str]} {incr i} {
        set c [string index $str $i]
        if {$c == "\"" || $c == "\\" } {
            append res \\$c
        } else {
            append res $c
        }
    }
    append res "\""
    return $res
}

proc eq_exp {row} {
    # Build condition
    set exp ""
    for {set i 0} {$i < [llength $::tablekey]} {incr i} {
        set a [lindex $::tablekey $i]
        if {$i > 0} {
            append exp " AND "
        }
        if {[must_quote $::tabletypes($a)]} {
            append exp "$a=[quote_str $::keyvals([expr {$row - 1}],$a)]"
        } else {
            append exp "$a=$::keyvals([expr {$row - 1}],$a)"
        }
    }
    return $exp
}

proc get_row {row} {
    set table [.tables get anchor]
    if {$table == ""} {
        return
    }

    set rexp "TUPLE FROM ($table WHERE [eq_exp $row])"
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        set_row $row [duro::expr $rexp $tx]
        duro::commit $tx
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
    }
}

proc update_tuple {row} {
    set table [.tables get anchor]
    if {$table == ""} {
        return
    }

    catch {unset ::modified([expr {$row - 1}])}

    # Move active cell to next row
    .tableframe.table activate [expr {$row + 1}],0

    # Build update arguments
    set updattrs {}
    for {set i 0} {$i < [llength $::tableattrs]} {incr i} {
        set a [lindex $::tableattrs $i]
        lappend updattrs $a
        if {[must_quote $::tabletypes($a)]} {
            lappend updattrs [quote_str [.tableframe.table get $row,$i]]
        } else {
            lappend updattrs [.tableframe.table get $row,$i]
        }
    }

    # Updating table with no attrbutes makes no sense
    if {$updattrs == {}} {
        return
    }

    # Update tuple
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        set uexp \{[eq_exp $row]\}
        # Must use eval, because updattrs is a list of arguments
        eval duro::update $table $uexp $updattrs $tx
        duro::commit $tx
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
        get_row $row
    }

    # Update ::keyvals
    for {set i 0} {$i < [llength $::tableattrs]} {incr i} {
        set attr [lindex $::tableattrs $i]
        if {[lsearch -exact $::tablekey $attr] >= 0} {
            set ::keyvals([expr {$row - 1}],$attr) \
                    [.tableframe.table get $row,$i]
        }
    }
}

proc update_row {} {
    if {$::tableattrs == {}} {
        .tableframe.table set active ""
    }
    scan [.tableframe.table index active] %d,%d row col
    set rowcount [.tableframe.table cget -rows]

    if {$row == [expr {$rowcount - 1}]} {
        insert_tuple
    } else {
        update_tuple $row
    }
}

proc must_quote {type} {
    return [expr {$type == "string" || $type == "binary"}]
}

proc del_row {} {
    set table [.tables get anchor]
    if {$table == ""} {
        return
    }

    scan [.tableframe.table index active] %d,%d row col
    set rowcount [.tableframe.table cget -rows]
    if {$row == [expr {$rowcount - 1}]} {
        return
    }

    # Delete tuple from DB table
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        set eqexp [eq_exp $row]
        if {$eqexp == ""} {
            # Table is nullary
            duro::delete $table $tx
        } else {
           duro::delete $table [eq_exp $row] $tx
        }
        duro::commit $tx
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
        return
    }

    # Delete row from table widget
    .tableframe.table delete rows $row

    # Delete row from keyvals
    for {set i [expr {$row - 1}]} {$i < [expr {$rowcount - 3}]} {incr i} {
        foreach k $::tablekey {
            set ::keyvals($i,$k) $::keyvals([expr {$i + 1}],$k)
        }
    }
    foreach k $::tablekey {
        unset ::keyvals([expr {$rowcount - 3}],$k)
    }

    # If last row was deleted, disable row deletion
    if {$row >= [expr {$rowcount - 2}]} {
        .mbar.db entryconfigure 3 -state disabled
    }
}

proc valcell {s} {
    scan [.tableframe.table index active] %d,%d row col

    # Mark row as modified
    set ::modified([expr {$row - 1}]) 1

    # Don't allow control characters as trailing characters in cell values
    # (prevents the return from being written into the cell)
    return [expr {![string is control -strict [string index $s end]]}]
}

proc browse {old new} {
    set rowcount [.tableframe.table cget -rows]
    scan $old %d,%d orow ocol
    scan $new %d,%d nrow ncol
    if {$nrow < [expr {$rowcount - 1}]} {
        .mbar.db entryconfigure 3 -state normal
    } else {
        .mbar.db entryconfigure 3 -state disabled
    }
    if {$old == ""} {
        return
    }

    set i [expr $orow - 1]
    if {$nrow != $orow && $orow < [expr {$rowcount - 1}]
            && [info exists ::modified($i)]} {
        get_row $orow
        unset ::modified($i)
    }
}

proc exec_script {} {
    toplevel .dialog
    wm title .dialog "Execute script"

    set ::script ""

    label .dialog.scriptl -text "Script: (Transaction is available as \$tx)"
    text .dialog.script -height 10

    set ::action ""
    frame .dialog.buttons
    button .dialog.buttons.execute -text Execute \
        -command {set action execute}
    button .dialog.buttons.close -text Close -command {set action close}

    label .dialog.outputl -text "Output:"
    text .dialog.output -height 10 -state disabled

    pack .dialog.output -side bottom
    pack .dialog.outputl -side bottom -anchor w
    pack .dialog.buttons -side bottom -padx 5 -pady 5
    pack .dialog.buttons.execute .dialog.buttons.close -side left
    pack .dialog.script -side bottom
    pack .dialog.scriptl -side bottom -anchor w

    focus .dialog.script

    grab .dialog
    while {$::action != "close"} {
        tkwait variable action
        if {$::action == "execute"} {
            if {[catch {
                # Execute command
                set tx [duro::begin $::dbenv $::db]
                set res [eval [.dialog.script get 1.0 end]]
                duro::commit $tx
            } msg]} {
                catch {duro::rollback $tx}
                tk_messageBox -type ok -title "Error" -message $msg \
                        -icon error -parent .dialog \
            } else {
                if {$res == ""} {
                    set res "(ok)"
                }
                .dialog.output configure -state normal
                .dialog.output insert end $res\n
                .dialog.output configure -state disabled
                .dialog.output see end
            }
        }
    }
    destroy .dialog
}

proc about {} {
    toplevel .about
    wm title .about "About Duroadmin"
    wm geometry .about "+300+300"

    set ::newtablename ""

    label .about.l1 -text "Duroadmin"
    text .about.t -width 40 -height 3
    .about.t insert end "Duro $::duro_version, (C) 2003-2012 Rene Hartmann."
    .about.t insert end "\n\nDuro comes with ABSOLUTELY NO WARRANTY."
    .about.t configure -state disabled

    frame .about.buttons
    button .about.buttons.ok -text OK -command {destroy .about}

    pack .about.l1 .about.t -side top -anchor w -padx 10 -pady 10
    pack .about.buttons -side bottom
    pack .about.buttons.ok -side left

    grab .about
    tkwait window .about
    destroy .about
}

proc quit {} {
    if {$::dbenv != ""} {
        duro::env close $::dbenv
    }

    close $::errf
    catch {file delete $::errfile}
    exit
}

set duroadmin(initrows) 20
set dbenv ""
set ltables {}

wm title . Duroadmin
menu .mbar
. config -menu .mbar

menu .mbar.file
.mbar add cascade -label File -menu .mbar.file -underline 0

.mbar.file add command -label "Open Environment..." -command open_env
.mbar.file add command -label "Create Environment..." -command create_env
.mbar.file add command -label "Close Environment" -command close_env \
        -state disabled
.mbar.file add separator
.mbar.file add command -label Quit -command quit

menu .mbar.view
.mbar add cascade -label View -menu .mbar.view -underline 0

.mbar.view add checkbutton -label "Show system tables" -variable showsys \
        -command show_tables
.mbar.view add command -label "Show errlor log" -command show_errlog

menu .mbar.db
.mbar add cascade -label Database -menu .mbar.db -underline 0

menu .mbar.db.create
.mbar.db add cascade -label Create -menu .mbar.db.create
menu .mbar.db.drop
.mbar.db add cascade -label Drop -menu .mbar.db.drop
.mbar.db add command -label "Delete row" -command del_row -state disabled
.mbar.db add command -label "Rename table" -command rename_table \
        -state disabled
.mbar.db add command -label "Execute script..." -command exec_script \
        -state disabled

.mbar.db.create add command -label "Database" -state disabled \
        -command create_db
.mbar.db.create add command -label "Real table" -state disabled \
        -command create_rtable
.mbar.db.create add command -label "Virtual table" -state disabled \
        -command create_vtable
.mbar.db.drop add command -label "Database" -state disabled \
        -command drop_db
.mbar.db.drop add command -label "Table" -state disabled \
        -command drop_table

menu .mbar.help
.mbar add cascade -label "Help" -menu .mbar.help -underline 0
.mbar.help add command -label "About Duroadmin" -command about

frame .dbsframe
label .dbsframe.dbl -text "Database:" -state disabled
set dbmenu [tk_optionMenu .dbsframe.dbs db ""]
.dbsframe.dbs configure -state disabled

listbox .tables -yscrollcommand ".tablesbar set"
scrollbar .tablesbar -orient vertical -command ".tables yview"

frame .tableframe
table .tableframe.table -multiline 1 -wrap 1 -titlerows 1 -rows [expr {$duroadmin(initrows) + 2}] \
        -xscrollcommand ".tableframe.xbar set" \
        -yscrollcommand ".tableframe.ybar set" \
        -variable tbcontent -state disabled -anchor w \
        -validatecommand {valcell %S} -validate 1 \
        -browsecommand {browse %s %S}
scrollbar .tableframe.xbar -orient horizontal -command ".tableframe.table xview"
scrollbar .tableframe.ybar -orient vertical -command ".tableframe.table yview"

button .morebutton -text More -state disabled -command more_tuples

pack .dbsframe -anchor w
pack .tables .tablesbar -side left -fill y
pack .morebutton -side bottom -anchor ne
pack .tableframe -side left -anchor nw -fill both
pack .dbsframe.dbl .dbsframe.dbs -side left -padx 0.2c
grid .tableframe.table -sticky nswe
grid .tableframe.ybar -row 0 -column 1 -sticky wsn
grid .tableframe.xbar -sticky ewn
grid columnconfigure .tableframe 0 -weight 1
grid rowconfigure .tableframe 0 -weight 1

bind .tables <<ListboxSelect>> show_table
bind .tableframe.table <Control-Return> { }
bind .tableframe.table <Return> update_row

wm protocol . WM_DELETE_WINDOW quit

toplevel .errlog
wm withdraw .errlog
wm title .errlog "Error Log"
wm protocol .errlog WM_DELETE_WINDOW { wm withdraw .errlog }
wm group .errlog .
bind .errlog <Map> { if {"%W" == ".errlog"} {read_log} }

text .errlog.msgs -state disabled
pack .errlog.msgs

if {$argc > 1} {
    puts stderr "duroadmin: too many arguments"
    exit 1
}

# Open log file (use temporary file)
if {$tcl_platform(platform) == "unix"} {
    set tmpdir /tmp
} else {
    set tmpdir [pwd]
}
catch {set tmpdir $env(TMP)}
catch {set tmpdir $env(TEMP)}
catch {set tmpdir $env(TMPDIR)}
set errfile [file join $tmpdir "da[pid]"]
set ::errf [open $errfile w+]

if {$argc == 1} {
    open_env_path [lindex $argv 0]
}
