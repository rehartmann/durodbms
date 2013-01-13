/*
 * $Id$
 *
 * Copyright (C) 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "io.h"
#include "internal.h"

RDB_type RDB_IOSTREAM_ID;

/*
 * Add type IO_STREAM for basic I/O support from the duro library.
 */
int
RDB_add_io(RDB_exec_context *ecp)
{
    static RDB_attr io_stream_comp = { "id", &RDB_INTEGER };

    static RDB_possrep io_stream_rep = {
            "iostream_id",
        1,
        &io_stream_comp
    };

    RDB_IOSTREAM_ID.kind = RDB_TP_SCALAR;
    RDB_IOSTREAM_ID.ireplen = sizeof(RDB_int);
    RDB_IOSTREAM_ID.name = io_stream_rep.name;
    RDB_IOSTREAM_ID.def.scalar.builtin = RDB_TRUE;
    RDB_IOSTREAM_ID.def.scalar.repc = 1;
    RDB_IOSTREAM_ID.def.scalar.repv = &io_stream_rep;
    RDB_IOSTREAM_ID.def.scalar.arep = &RDB_INTEGER;
    RDB_IOSTREAM_ID.def.scalar.constraintp = NULL;
    RDB_IOSTREAM_ID.def.scalar.sysimpl = RDB_TRUE;
    RDB_IOSTREAM_ID.compare_op = NULL;

    return RDB_add_type(&RDB_IOSTREAM_ID, ecp);
}
