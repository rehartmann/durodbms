/*
 * $Id$
 *
 * Copyright (C) 2011-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * I/O operators
 */

#include "ioop.h"
#include <rel/rdb.h>
#include <rel/opmap.h>
#include <rel/tostr.h>
#include <rel/typeimpl.h>

#include <errno.h>
#include <string.h>
#include <stdio.h>

#if defined(_WIN32)
#define STDIN_FILENO 0
#define STDOUT_FILENO 1
#define STDERR_FILENO 2
#else
#include <unistd.h>
#endif

enum {
    IOSTREAMS_MAX = 63
};

static FILE *iostreams[IOSTREAMS_MAX + 1] = { NULL }; /* Initalize with zeroes */

RDB_object DURO_STDIN_OBJ;
RDB_object DURO_STDOUT_OBJ;
RDB_object DURO_STDERR_OBJ;

/** @page io-ops Built-in I/O operators

These operators are only available in Duro D/T.

OPERATOR put_line(line string) UPDATES {};

Write <var>line</var> to standard output, followed by a newline.

OPERATOR put(data string) UPDATES {};

OPERATOR put(data integer) UPDATES {};

OPERATOR put(data float) UPDATES {};

OPERATOR put(data boolean) UPDATES {};

OPERATOR put(data <em>TUPLE</em>) UPDATES {};

OPERATOR put(data <em>RELATION</em>) UPDATES {};

OPERATOR put(data <em>ARRAY</em>) UPDATES {};

Write <var>data</var> to standard output.

OPERATOR get_line(line string) UPDATES {line};

Read a line from standard input and store it in line, without the trailing newline.

OPERATOR put_line(ios io_stream, line string) UPDATES {};

Write <var>line</var> to the I/O stream <var>ios</var>, followed by a newline.

OPERATOR put(io_stream ios, data string) UPDATES {};

OPERATOR put(io_stream ios, data binary) UPDATES {};

OPERATOR put(io_stream ios, data integer) UPDATES {};

OPERATOR put(io_stream ios, data float) UPDATES {};

OPERATOR put(io_stream ios, data boolean) UPDATES {};

OPERATOR put(io_stream ios, data <em>TUPLE</em>) UPDATES {};

OPERATOR put(io_stream ios, data <em>RELATION</em>) UPDATES {};

OPERATOR put(io_stream ios, data <em>ARRAY</em>) UPDATES {};

Write <var>data</var> to the I/O stream <var>ios</var>.

OPERATOR get_line(io_stream ios, line string) UPDATES {line};

Read a line from I/O stream <var>ios</var> and store it in line, without the trailing newline.

OPERATOR open(ios io_stream, string path, string mode) UPDATES {ios};

Open file <var>path</var> in mode <var>mode</var> and store the resulting I/O stream
in <var>ios</var>.

OPERATOR close(ios io_stream) UPDATES {};

Close I/O stream <var>ios</var>.

OPERATOR eof() RETURNS boolean;

Return TRUE if the end-of-file indicator was set while reading
from standard input.

OPERATOR eof(ios io_stream) RETURNS boolean;

Return TRUE if the end-of-file indicator was set while reading
from <var>ios</var>.

*/

/*
 * Get file number from io_stream
 */
static int
get_fileno(const RDB_object *objp, RDB_exec_context *ecp)
{
    int fno = (int) RDB_obj_int(objp);
    if (fno < 0 || fno > IOSTREAMS_MAX || iostreams[fno] == NULL) {
        RDB_raise_invalid_argument("invalid file number", ecp);
        return RDB_ERROR;
    }
    return fno;
}

static int
op_put_line_iostream_string(int argc, RDB_object *argv[],
        RDB_operator *op, RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }
    
    if (fprintf(iostreams[fno], "%s\n", RDB_obj_string(argv[1])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
put_nonscalar(FILE *fp, const RDB_object *objp,
    RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object strobj;
    
    RDB_init_obj(&strobj);
    if (RDB_obj_to_str(&strobj, objp, ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&strobj, ecp);
        return RDB_ERROR;
    }
    if (fputs(RDB_obj_string(&strobj), fp) == EOF) {
        RDB_destroy_obj(&strobj, ecp);
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
        
    RDB_destroy_obj(&strobj, ecp);
    return RDB_OK;
}

static int
op_put_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_string(argv[0]), stdout) == EOF) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fwrite(RDB_obj_irep(argv[0], NULL), RDB_binary_length(argv[0]), 1,
            stdout) != 1) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%d", (int) RDB_obj_int(argv[0])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object dstobj;

    RDB_init_obj(&dstobj);
    if (RDB_obj_to_string(&dstobj, argv[0], ecp) != RDB_OK)
        goto error;
    if (fputs(RDB_obj_string(&dstobj), stdout) == EOF) {
        RDB_errcode_to_error(errno, ecp, txp);
        goto error;
    }
    return RDB_destroy_obj(&dstobj, ecp);

error:
    RDB_destroy_obj(&dstobj, ecp);;
    return RDB_ERROR;
}

static int
op_put_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_bool(argv[0]) ? "TRUE" : "FALSE", stdout) == EOF) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_nonscalar(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *dataobjp;
    int fno;

    if (argc != 1 && argc != 2) {
        RDB_raise_invalid_argument("invalid # of arguments", ecp);
        return RDB_ERROR;
    }

    dataobjp = argv[argc - 1];
    if (RDB_obj_type(dataobjp) != NULL
            && RDB_type_is_scalar(RDB_obj_type(dataobjp))) {
        RDB_raise_type_mismatch("put", ecp);
        return RDB_ERROR;
    }

    if (argc == 2) {
        /* Get file number from arg #1 */
        fno = get_fileno(argv[0], ecp);
        if (fno == RDB_ERROR)
            return RDB_ERROR;
    }        

    return put_nonscalar(argc == 1 ? stdout : iostreams[fno], dataobjp,
            ecp, txp);
}

static int
op_put_iostream_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fputs(RDB_obj_string(argv[1]), iostreams[fno]) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_iostream_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fwrite(RDB_obj_irep(argv[1], NULL), RDB_binary_length(argv[1]), 1,
            iostreams[fno]) != 1) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_iostream_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fno], "%d", (int) RDB_obj_int(argv[1])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_iostream_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fno], "%f", (double) RDB_obj_float(argv[1])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_iostream_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fputs(RDB_obj_bool(argv[1]) ? "TRUE" : "FALSE", iostreams[fno]) == EOF) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_put_line_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *callargv[3];

    callargv[0] = &DURO_STDOUT_OBJ;
    callargv[1] = argv[0];
    
    return op_put_line_iostream_string(2, callargv, op, ecp, txp);
}

static int
get_line(FILE *fp, RDB_object *linep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    char buf[128];
    size_t len;

    if (RDB_string_to_obj(linep, "", ecp) != RDB_OK)
        return RDB_ERROR;

    if (fgets(buf, sizeof(buf), fp) == NULL) {
        if (ferror(fp)) {
            RDB_errcode_to_error(errno, ecp, txp);
            return RDB_ERROR;
        }
        return RDB_OK;
    }
    len = strlen(buf);

    /* Read until a complete line has been read */
    while (buf[len - 1] != '\n') {
        if (RDB_append_string(linep, buf, ecp) != RDB_OK)
            return RDB_ERROR;
        if (fgets(buf, sizeof(buf), fp) == NULL) {
            if (ferror(fp)) {
                RDB_errcode_to_error(errno, ecp, txp);
                return RDB_ERROR;
            }
            return RDB_OK;
        }
        len = strlen(buf);
    }
    buf[len - 1] = '\0';
    return RDB_append_string(linep, buf, ecp);
}

static int
op_get_line(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    return get_line(stdin, argv[0], ecp, txp);
}

static int
op_get_line_iostream(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    return get_line(iostreams[fno], argv[1], ecp, txp);
}

static int
op_close(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }
    if (fclose(iostreams[fno]) != 0) {
        iostreams[fno] = NULL;
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    iostreams[fno] = NULL;
    return RDB_OK;
}

/* Initialize *iosp using fno */
static int
init_iostream(RDB_object *iosp, int fno, RDB_exec_context *ecp)
{
    int ret;
    RDB_object fobj;
    RDB_object *fobjp;

    /* Check fno */
    if (fno > IOSTREAMS_MAX) {
        RDB_raise_not_supported("file number too high", ecp);
        return RDB_ERROR;
    }

    /* Call selector */
    RDB_init_obj(&fobj);
    RDB_int_to_obj(&fobj, (RDB_int) fno);
    fobjp = &fobj;
    ret = RDB_call_ro_op_by_name("io_stream", 1, &fobjp, ecp, NULL, iosp);
    RDB_destroy_obj(&fobj, ecp);
    return ret;
}    

static int
op_open(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int fno;

    /* Open file */
    FILE *fp = fopen(RDB_obj_string(argv[1]), RDB_obj_string(argv[2]));
    if (fp == NULL) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }

    /* Get file number */
    fno = fileno(fp);

    /* Set table entry */
    iostreams[fno] = fp;

    /* Set argument #1 */
    return init_iostream(argv[0], fno, ecp);
}

int
op_eof(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, struct RDB_transaction *txp,
        RDB_object *resultp)
{
    RDB_bool_to_obj(resultp, feof(stdin));
    return RDB_OK;
}

int
op_eof_iostream(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, struct RDB_transaction *txp,
        RDB_object *resultp)
{
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    RDB_bool_to_obj(resultp, feof(iostreams[fno]));
    return RDB_OK;
}

int
RDB_add_io_ops(RDB_op_map *opmapp, RDB_exec_context *ecp)
{
    static RDB_parameter put_string_params[1];
    static RDB_parameter put_binary_params[1];
    static RDB_parameter put_int_params[1];
    static RDB_parameter put_float_params[1];
    static RDB_parameter put_bool_params[1];

    static RDB_parameter put_iostream_string_params[2];
    static RDB_parameter put_iostream_binary_params[2];
    static RDB_parameter put_iostream_int_params[2];
    static RDB_parameter put_iostream_float_params[2];
    static RDB_parameter put_iostream_bool_params[2];

    static RDB_parameter get_line_params[1];
    static RDB_parameter get_line_iostream_params[2];
    static RDB_parameter open_params[3];
    static RDB_parameter close_params[1];

    static RDB_type *eof_iostream_param_typ;

    put_string_params[0].typ = &RDB_STRING;
    put_string_params[0].update = RDB_FALSE;
    put_binary_params[0].typ = &RDB_BINARY;
    put_binary_params[0].update = RDB_FALSE;
    put_int_params[0].typ = &RDB_INTEGER;
    put_int_params[0].update = RDB_FALSE;
    put_float_params[0].typ = &RDB_FLOAT;
    put_float_params[0].update = RDB_FALSE;
    put_bool_params[0].typ = &RDB_BOOLEAN;
    put_bool_params[0].update = RDB_FALSE;

    put_iostream_string_params[0].typ = &RDB_IO_STREAM;
    put_iostream_string_params[0].update = RDB_FALSE;
    put_iostream_string_params[1].typ = &RDB_STRING;
    put_iostream_string_params[1].update = RDB_FALSE;
    put_iostream_binary_params[0].typ = &RDB_IO_STREAM;
    put_iostream_binary_params[0].update = RDB_FALSE;
    put_iostream_binary_params[1].typ = &RDB_BINARY;
    put_iostream_binary_params[1].update = RDB_FALSE;
    put_iostream_int_params[0].typ = &RDB_IO_STREAM;
    put_iostream_int_params[0].update = RDB_FALSE;
    put_iostream_int_params[1].typ = &RDB_INTEGER;
    put_iostream_int_params[1].update = RDB_FALSE;
    put_iostream_float_params[0].typ = &RDB_IO_STREAM;
    put_iostream_float_params[0].update = RDB_FALSE;
    put_iostream_float_params[1].typ = &RDB_FLOAT;
    put_iostream_float_params[1].update = RDB_FALSE;
    put_iostream_bool_params[0].typ = &RDB_IO_STREAM;
    put_iostream_bool_params[0].update = RDB_FALSE;
    put_iostream_bool_params[1].typ = &RDB_BOOLEAN;
    put_iostream_bool_params[1].update = RDB_FALSE;

    get_line_params[0].typ = &RDB_STRING;
    get_line_params[0].update = RDB_TRUE;
    get_line_iostream_params[0].typ = &RDB_IO_STREAM;
    get_line_iostream_params[0].update = RDB_FALSE;
    get_line_iostream_params[1].typ = &RDB_STRING;
    get_line_iostream_params[1].update = RDB_TRUE;

    close_params[0].typ = &RDB_IO_STREAM;
    close_params[0].update = RDB_FALSE;

    open_params[0].typ = &RDB_IO_STREAM;
    open_params[0].update = RDB_TRUE;
    open_params[1].typ = &RDB_STRING;
    open_params[1].update = RDB_FALSE;
    open_params[2].typ = &RDB_STRING;
    open_params[2].update = RDB_FALSE;

    if (RDB_put_upd_op(opmapp, "put_line", 1, put_string_params, &op_put_line_string,
            ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "put_line", 2, put_iostream_string_params,
            &op_put_line_iostream_string, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "put", 1, put_string_params, &op_put_string, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 1, put_binary_params, &op_put_binary, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 1, put_int_params, &op_put_int, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 1, put_float_params, &op_put_float, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 1, put_bool_params, &op_put_bool, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", -1, NULL, &op_put_nonscalar, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "put", 2, put_iostream_string_params,
            &op_put_iostream_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 2, put_iostream_binary_params,
            &op_put_iostream_binary, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 2, put_iostream_int_params,
            &op_put_iostream_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 2, put_iostream_float_params,
            &op_put_iostream_float, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "put", 2, put_iostream_bool_params,
            &op_put_iostream_bool, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "get_line", 1, get_line_params, &op_get_line, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "get_line", 2, get_line_iostream_params,
            &op_get_line_iostream, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "close", 1, close_params, &op_close, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "open", 3, open_params, &op_open, ecp)
            != RDB_OK)
        return RDB_ERROR;

    eof_iostream_param_typ = &RDB_IO_STREAM;

    if (RDB_put_global_ro_op("eof", 0, NULL, &RDB_BOOLEAN, &op_eof, ecp)
            != RDB_OK) {
        return RDB_ERROR;
    }

    if (RDB_put_global_ro_op("eof", 1, &eof_iostream_param_typ,
            &RDB_BOOLEAN, &op_eof_iostream, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_init_obj(&DURO_STDIN_OBJ);
    if (init_iostream(&DURO_STDIN_OBJ, STDIN_FILENO, ecp) != RDB_OK)
        return RDB_ERROR;
    iostreams[STDIN_FILENO] = stdin;

    RDB_init_obj(&DURO_STDOUT_OBJ);
    if (init_iostream(&DURO_STDOUT_OBJ, STDOUT_FILENO, ecp) != RDB_OK)
        return RDB_ERROR;
    iostreams[STDOUT_FILENO] = stdout;

    RDB_init_obj(&DURO_STDERR_OBJ);
    if (init_iostream(&DURO_STDERR_OBJ, STDERR_FILENO, ecp) != RDB_OK)
        return RDB_ERROR;
    iostreams[STDERR_FILENO] = stderr;

    return RDB_OK;
}
