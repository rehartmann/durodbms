/*
 * $Id$
 *
 * Copyright (C) 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "io.h"

#include "internal.h"

RDB_type RDB_IO_STREAM;

/*
 * Add type IO_STREAM for basic I/O support from the duro library.
 */
int
_RDB_add_io(RDB_exec_context *ecp)
{
    static RDB_attr io_stream_comp = { "FILENO", &RDB_INTEGER };

    static RDB_possrep io_stream_rep = {
        "IO_STREAM",
        1,
        &io_stream_comp
    };

    RDB_IO_STREAM.kind = RDB_TP_SCALAR;
    RDB_IO_STREAM.ireplen = sizeof(RDB_int);
    RDB_IO_STREAM.name = "IO_STREAM";
    RDB_IO_STREAM.def.scalar.builtin = RDB_TRUE;
    RDB_IO_STREAM.def.scalar.repc = 1;
    RDB_IO_STREAM.def.scalar.repv = &io_stream_rep;
    RDB_IO_STREAM.def.scalar.arep = &RDB_INTEGER;
    RDB_IO_STREAM.def.scalar.constraintp = NULL;
    RDB_IO_STREAM.def.scalar.sysimpl = RDB_TRUE;
    RDB_IO_STREAM.compare_op = NULL;

    return _RDB_add_type(&RDB_IO_STREAM, ecp);
}
