#!/bin/sh
# Execute wish from the user's PATH \
exec wish "$0" ${1+"$@"}

# Copyright (C) 2004 René Hartmann.
# See the file COPYING for redistribution information.

# $Id$

package require duro
package require Tktable

# Global variables:
#
# dbenv	current DB environment ID
# db	currently selected database

proc show_tables {} {
    if {$::dbenv == "" || $::db == ""} {
        return
    }

    .tables delete 0 end

    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        set tables [duro::tables $::db $::showsys $tx]
        duro::commit $tx
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
    }

    foreach tb $tables {
        .tables insert end $tb
    }
}

proc add_db {newdb} {
    if {$::db == ""} {
        # Delete old entries
        $::dbmenu delete 0

        set ::db $newdb
        .mbar.db.create entryconfigure 2 -state normal
        .mbar.db.create entryconfigure 3 -state normal
        .mbar.db.drop entryconfigure 1 -state normal
    }

    # Add menu entry
    $::dbmenu add radiobutton -label $newdb -variable db -command show_tables

    .dbsframe.dbs configure -state normal
    .dbsframe.dbl configure -state normal
}

proc open_env_path {envpath} {
    set ::dbenv [duro::env open $envpath]
    .mbar.file entryconfigure Close* -state normal

    # Get databases
    set dbs [duro::env dbs $::dbenv]

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

    if {![file exists $envpath]} {
        file mkdir $envpath
    }

    set ::dbenv [duro::env create $envpath]
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
    for {set i 0} {$i < [llength $::tableattrs]} {incr i} {
        .tableframe.table set [expr $rowcount - 1],$i ""
    }
}

proc show_table {} {
    set table [.tables get anchor]
    if {$table == ""} {
        return
    }

    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        set ::tableattrs [duro::table attrs $table $tx]

        .tableframe.table configure -state normal
        pack propagate . 0
        .tableframe.table configure -cols [llength $::tableattrs] -colwidth 16

        # Set table heading
        for {set i 0} {$i < [llength $::tableattrs]} {incr i} {
            .tableframe.table set 0,$i [lindex $::tableattrs $i]
        }

        # Set values - # of rows displayed is limited to the value
        # of duroadmin(initrows)
        set arr [duro::array create $table $tx]
        .tableframe.table configure -rows [expr {$::duroadmin(initrows) + 2}]
        for {set i 0} {$i < $::duroadmin(initrows)} {incr i} {
            if {[catch {array set ta [duro::array index $arr $i $tx]} err]} {
                break
            }
            for {set j 0} {$j < [llength $::tableattrs]} {incr j} {
                set s $ta([lindex $::tableattrs $j])
                if {![string is print $s]} {
                    set s "(nonprintable)"
                }
                .tableframe.table set [expr {$i + 1}],$j $s
            }
        }                
        duro::array drop $arr
        duro::commit $tx

        set rowcount [expr {$i + 2}]
        .tableframe.table configure -rows $rowcount
    } msg]} {
        catch {duro::array drop $arr}
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
    }

    # Clear last row, which is for entering new rows
    clear_bottom_row

    .mbar.db.drop entryconfigure 2 -state normal
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

    pack .dialog.buttons -side bottom
    pack .dialog.buttons.ok .dialog.buttons.cancel -side left
    pack .dialog.l .dialog.dbname -side left

    grab .dialog
    tkwait variable action
    destroy .dialog
    if {$::action != "ok"} {
        return
    }
    if {$::newdbname == ""} {
        return
    }

    # Create database
    duro::db create $::newdbname $::dbenv
    add_db $::newdbname
    set ::db $::newdbname
    .mbar.db.drop entryconfigure 1 -state normal
    show_tables
}

proc create_rtable {} {
    toplevel .dialog
    wm title .dialog "Create table"
    wm geometry .dialog "+300+300"

    set ::newtablename ""

    label .dialog.l -text "Table name:"
    entry .dialog.tablename -textvariable newtablename

    set ::action ok
    frame .dialog.buttons
    button .dialog.buttons.ok -text OK -command {set action ok}
    button .dialog.buttons.cancel -text Cancel -command {set action cancel}

    table .dialog.tabledef -titlerows 1 -rows 5 -cols 3 -variable tabledef \
            -anchor w

    set ::tabledef(0,0) "Attribute name"
    set ::tabledef(0,1) Type
    set ::tabledef(0,2) "Is key"

    set attrcount 4

    for {set i 0} {$i < $attrcount} {incr i} {
        if {$i == 0} {
            set mw [tk_optionMenu .dialog.tabledef.type$i type($i) \
                    STRING INTEGER RATIONAL BINARY]
        } else {
            tk_optionMenu .dialog.tabledef.type$i type($i) \
                    STRING INTEGER RATIONAL BINARY
        }
        .dialog.tabledef window configure [expr $i + 1],1 \
                -window .dialog.tabledef.type$i
        set t$i STRING

        checkbutton .dialog.tabledef.key$i,0 -variable key($i,0)
        .dialog.tabledef window configure [expr $i + 1],2 \
                -window .dialog.tabledef.key$i,0
        if {$i == 0} {
            set h [expr {-[winfo reqheight .dialog.tabledef.type$i] - 2 *
                    [.dialog.tabledef.key0,0 cget -pady]}]
        }
        .dialog.tabledef height [expr $i + 1] $h
    }

    .dialog.tabledef width 0 [string length ::tabledef(0,0)]

    # Change type in line #1 to RATIONAL to set width
    $mw invoke 2
    .dialog.tabledef width 1 [expr {-[winfo reqwidth .dialog.tabledef.type0] - 2 *
           [.dialog.tabledef.type0 cget -pady]}]

    # Change type back
    $mw invoke 0

    .dialog.tabledef.key0,0 select

    pack .dialog.buttons -side bottom
    pack .dialog.buttons.ok .dialog.buttons.cancel -side left
    pack .dialog.tabledef -side bottom
    pack .dialog.l .dialog.tablename -side left

    set done 0
    grab .dialog
    while {!$done} {
        tkwait variable action
        if {$::action != "ok"} {
            destroy .dialog
            return
        }
        if {$::newtablename == ""} {
            destroy .dialog
            return
        }

        # Build lists for attributes and keys
        set attrs {}
        set keys {}

        for {set i 0} {$i < $attrcount} {incr i} {
            if {[info exists ::tabledef([expr $i + 1],0)]} {
                set attrname $::tabledef([expr $i + 1],0)
                if {$attrname != ""} {
                    lappend attrs [list $attrname $::type($i)]
                    if {$::key($i,0)} {
                        lappend keys $attrname
                    }
                }
            }
        }

        if {[catch {
            # Create table
            set tx [duro::begin $::dbenv $::db]
            duro::table create $::newtablename $attrs $keys $tx
            duro::commit $tx
            set done 1
        } msg]} {
            catch {duro::rollback $tx}
            tk_messageBox -type ok -title "Error" -message $msg -icon error
        }
     }

    .tables insert end $::newtablename

    destroy .dialog
}

proc create_vtable {} {
    toplevel .dialog
    wm title .dialog "Create virtual table"
    wm geometry .dialog "+300+300"

    set ::newtablename ""

    label .dialog.l -text "Table name:"
    entry .dialog.tablename -textvariable newtablename

    set ::action ok
    frame .dialog.buttons
    button .dialog.buttons.ok -text OK -command {set action ok}
    button .dialog.buttons.cancel -text Cancel -command {set action cancel}

    label .dialog.defl -text "Table definition:"
    text .dialog.tabledef

    pack .dialog.buttons -side bottom
    pack .dialog.buttons.ok .dialog.buttons.cancel -side left
    pack .dialog.tabledef -side bottom
    pack .dialog.defl -side bottom -anchor w
    pack .dialog.l .dialog.tablename -side left

    set done 0
    grab .dialog
    while {!$done} {
        tkwait variable action
        if {$::action != "ok"} {
            destroy .dialog
            return
        }
        if {$::newtablename == ""} {
            destroy .dialog
            return
        }

        if {[catch {
            # Create virtual table
            set tx [duro::begin $::dbenv $::db]
            duro::table expr -global $::newtablename [.dialog.tabledef get 1.0 end] $tx
            duro::commit $tx
            set done 1
        } msg]} {
            catch {duro::rollback $tx}
            tk_messageBox -type ok -title "Error" -message $msg -icon error
        }
    }

    .tables insert end $::newtablename

    destroy .dialog
}

proc drop_db {} {
    if {[tk_messageBox -type okcancel -title "Drop database" \
            -message "Drop database \"$::db\"?" -icon question] != "ok"} {
        return
    }

    duro::db drop $::db $::dbenv

    $::dbmenu delete $::db
    set ::db [$::dbmenu entrycget 0 -value]
    if {$::db == ""} {
        .dbsframe.dbs configure -state disabled
        .dbsframe.dbl configure -state disabled
        .mbar.db.create entryconfigure 2 -state disabled
        .mbar.db.drop entryconfigure 1 -state disabled
    }
}

proc drop_table {} {
    set table [.tables get active]

    if {[tk_messageBox -type okcancel -title "Drop table" \
            -message "Drop table \"$table\"?" -icon question] != "ok"} {
        return
    }

    # Drop table
    if {[catch {
        set tx [duro::begin $::dbenv $::db]
        duro::table drop $table $tx
        duro::commit $tx

        .tables delete active
        .mbar.db.drop entryconfigure 2 -state disabled
    } msg]} {
        catch {duro::rollback $tx}
        tk_messageBox -type ok -title "Error" -message $msg -icon error
    }
}

proc update_row {} {
    set table [.tables get anchor]
    set rowcount [.tableframe.table cget -rows]
    scan [.tableframe.table index active] %d,%d row col

    if {$row < [expr {$rowcount - 1}]} {
        return
    }

    # Add empty row and move active cell to this row
    .tableframe.table configure -rows [expr $rowcount + 1]
    clear_bottom_row
    .tableframe.table activate $rowcount,0

    # Build tuple
    set tpl ""
    set i 0
    foreach attr $::tableattrs {
        lappend tpl $attr [.tableframe.table get $row,$i]
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
    }
}

# Don't allow control characters as cell values (prevents the return
# from being written into the cell)
proc valcell {s} {
    return [expr {![string is control -strict [string index $s end]]}]
}

set duroadmin(initrows) 20
set dbenv ""

wm title . Duroadmin
menu .mbar
. config -menu .mbar

menu .mbar.file
.mbar add cascade -label File -menu .mbar.file

.mbar.file add command -label "Open Environment..." -command open_env
.mbar.file add command -label "Create Environment..." -command create_env
.mbar.file add command -label "Close Environment" -command close_env \
        -state disabled
.mbar.file add separator
.mbar.file add command -label Quit -command exit

menu .mbar.view
.mbar add cascade -label View -menu .mbar.view

.mbar.view add checkbutton -label "Show system tables" -variable showsys \
        -command show_tables

menu .mbar.db
.mbar add cascade -label Database -menu .mbar.db

menu .mbar.db.create
.mbar.db add cascade -label Create -menu .mbar.db.create
menu .mbar.db.drop
.mbar.db add cascade -label Drop -menu .mbar.db.drop

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

frame .dbsframe
label .dbsframe.dbl -text "Database:" -state disabled
set dbmenu [tk_optionMenu .dbsframe.dbs db ""]
.dbsframe.dbs configure -state disabled

listbox .tables -yscrollcommand ".tablesbar set"
scrollbar .tablesbar -orient vertical -command ".tables yview"

frame .tableframe
table .tableframe.table -titlerows 1 -rows 20 \
        -xscrollcommand ".tableframe.xbar set" \
        -yscrollcommand ".tableframe.ybar set" \
        -variable tbcontent -state disabled -anchor w \
        -validatecommand {valcell %S} -validate 1
scrollbar .tableframe.xbar -orient horizontal -command ".tableframe.table xview"
scrollbar .tableframe.ybar -orient vertical -command ".tableframe.table yview"

button .morebutton -text More -state disabled

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
bind .tableframe.table <Return> update_row

if {$argc > 1} {
    puts stderr "duroadmin: too many arguments"
    exit 1
}

if {$argc == 1} {
    open_env_path [lindex $argv 0]
}
