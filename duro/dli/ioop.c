/*
 * $Id$
 *
 * Copyright (C) 2011-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * I/O operators.
 */

#include "ioop.h"
#include <rel/rdb.h>
#include <rel/opmap.h>
#include <rel/tostr.h>

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
    IOSTREAMS_MAX = 64
};

static FILE *iostreams[IOSTREAMS_MAX] = { NULL }; /* Initalize with zeros */

RDB_object DURO_STDIN_OBJ;
RDB_object DURO_STDOUT_OBJ;
RDB_object DURO_STDERR_OBJ;

/** @page io-ops Built-in I/O operators

These operators are only available in Duro D/T.

OPERATOR PRINTLN(STRING) UPDATES {};

OPERATOR PRINTLN(INTEGER) UPDATES {};

OPERATOR PRINTLN(FLOAT) UPDATES {};

OPERATOR PRINTLN(BOOLEAN) UPDATES {};

OPERATOR PRINTLN(<em>TUPLE</em>) UPDATES {};

OPERATOR PRINTLN(<em>RELATION></em>) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, <em>ARRAY</em>) UPDATES {};

OPERATOR PRINT(STRING) UPDATES {};

OPERATOR PRINT(INTEGER) UPDATES {};

OPERATOR PRINT(FLOAT) UPDATES {};

OPERATOR PRINT(BOOLEAN) UPDATES {};

OPERATOR PRINT(<em>TUPLE</em>) UPDATES {};

OPERATOR PRINT(<em>RELATION></em>) UPDATES {};

OPERATOR PRINT(<em>ARRAY</em>) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, STRING) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, INTEGER) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, FLOAT) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, BOOLEAN) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, <em>TUPLE</em>) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, <em>RELATION></em>) UPDATES {};

OPERATOR PRINTLN(IO_STREAM, <em>ARRAY</em>) UPDATES {};

OPERATOR PRINT(IO_STREAM, STRING) UPDATES {};

OPERATOR PRINT(IO_STREAM, INTEGER) UPDATES {};

OPERATOR PRINT(IO_STREAM, FLOAT) UPDATES {};

OPERATOR PRINT(IO_STREAM, BOOLEAN) UPDATES {};

OPERATOR PRINT(IO_STREAM, <em>TUPLE</em>) UPDATES {};

OPERATOR PRINT(IO_STREAM, <em>RELATION></em>) UPDATES {};

OPERATOR PRINT(IO_STREAM, <em>ARRAY</em>) UPDATES {};

OPERATOR READLN(LINE STRING) UPDATES {LINE};

OPERATOR OPEN(IOS IO_STREAM, STRING PATH, STRING MODE) UPDATES {IOS};

OPERATOR CLOSE(IOS IO_STREAM) UPDATES {};

*/

/*
 * Get file number from IO_STREAM
 */
static int get_fileno(const RDB_object *objp, RDB_exec_context *ecp)
{
    int fno = (int) RDB_obj_int(objp);
    if (fno >= IOSTREAMS_MAX || iostreams[fno] == NULL) {
        RDB_raise_invalid_argument("invalid file number", ecp);
        return RDB_ERROR;
    }
    return fno;
}

static int
op_println_iostream_string(int argc, RDB_object *argv[],
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
op_println_iostream_int(int argc, RDB_object *argv[],
        RDB_operator *op, RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fno], "%d\n", (int) RDB_obj_int(argv[1])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_iostream_float(int argc, RDB_object *argv[],
        RDB_operator *op, RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fno], "%f\n", (double) RDB_obj_float(argv[1])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_iostream_bool(int argc, RDB_object *argv[],
        RDB_operator *op, RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fno], "%s\n",
            RDB_obj_bool(argv[1]) ? "TRUE" : "FALSE") == EOF) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
print_nonscalar(FILE *fp, const RDB_object *objp, 
    RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object strobj;
    
    RDB_init_obj(&strobj);
    if (_RDB_obj_to_str(&strobj, objp, ecp, txp) != RDB_OK) {
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
op_print_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_string(argv[0]), stdout) == EOF) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%d", (int) RDB_obj_int(argv[0])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%f", (double) RDB_obj_float(argv[0])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_bool(argv[0]) ? "TRUE" : "FALSE", stdout) == EOF) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_nonscalar(int argc, RDB_object *argv[], RDB_operator *op,
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
        RDB_raise_type_mismatch("PRINT", ecp);
        return RDB_ERROR;
    }

    if (argc == 2) {
        /* Get file number from arg #1 */
        fno = get_fileno(argv[0], ecp);
        if (fno == RDB_ERROR)
            return RDB_ERROR;
    }        

    return print_nonscalar(argc == 1 ? stdout : iostreams[fno], dataobjp,
            ecp, txp);
}

static int
op_print_iostream_string(int argc, RDB_object *argv[], RDB_operator *op,
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
op_print_iostream_int(int argc, RDB_object *argv[], RDB_operator *op,
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
op_print_iostream_float(int argc, RDB_object *argv[], RDB_operator *op,
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
op_print_iostream_bool(int argc, RDB_object *argv[], RDB_operator *op,
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
op_println_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *callargv[3];

    callargv[0] = &DURO_STDOUT_OBJ;
    callargv[1] = argv[0];
    
    return op_println_iostream_string(2, callargv, op, NULL, txp);
}

static int
op_println_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%d\n", (int) RDB_obj_int(argv[0])) < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *callargv[3];

    callargv[0] = &DURO_STDOUT_OBJ;
    callargv[1] = argv[0];
    callargv[2] = argv[1];
    
    return op_println_iostream_float(3, callargv,
            NULL, ecp, txp);
}

static int
op_println_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *callargv[3];

    callargv[0] = &DURO_STDOUT_OBJ;
    callargv[1] = argv[0];
    callargv[2] = argv[1];
    
    return op_println_iostream_bool(3, callargv,
            NULL, ecp, txp);
}

static int
op_println_nonscalar(int argc, RDB_object *argv[], RDB_operator *op,
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
        RDB_raise_type_mismatch("PRINTLN", ecp);
        return RDB_ERROR;
    }

    if (argc == 2) {
        /* Get file number from arg #1 */
        fno = get_fileno(argv[0], ecp);
        if (fno == RDB_ERROR)
            return RDB_ERROR;
    }        

    if (print_nonscalar(argc == 1 ? stdout : iostreams[fno], dataobjp,
            ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (fprintf(argc == 1 ? stdout : iostreams[fno], "\n") < 0) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
readln(FILE *fp, RDB_object *linep, RDB_exec_context *ecp,
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
op_readln(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    return readln(stdin, argv[0], ecp, txp);
}

static int
op_readln_iostream(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fno = get_fileno(argv[0], ecp);
    if (fno == RDB_ERROR) {
        return RDB_ERROR;
    }

    return readln(iostreams[fno], argv[1], ecp, txp);
}

static int
op_close(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg*/
    int fno = RDB_obj_int(argv[0]);
    if (fno >= IOSTREAMS_MAX || iostreams[fno] == NULL) {
        RDB_raise_invalid_argument("invalid file number", ecp);
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
    if (fno >= IOSTREAMS_MAX) {
        RDB_raise_not_supported("file number too high", ecp);
        return RDB_ERROR;
    }

    /* Call selector */
    RDB_init_obj(&fobj);
    RDB_int_to_obj(&fobj, (RDB_int) fno);
    fobjp = &fobj;
    ret = RDB_call_ro_op_by_name("IO_STREAM", 1, &fobjp, ecp, NULL, iosp);
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
_RDB_add_io_ops(RDB_op_map *opmapp, RDB_exec_context *ecp)
{
    static RDB_parameter print_string_params[1];
    static RDB_parameter print_int_params[1];
    static RDB_parameter print_float_params[1];
    static RDB_parameter print_bool_params[1];

    static RDB_parameter print_iostream_string_params[2];
    static RDB_parameter print_iostream_int_params[2];
    static RDB_parameter print_iostream_float_params[2];
    static RDB_parameter print_iostream_bool_params[2];

    static RDB_parameter readln_params[1];
    static RDB_parameter readln_iostream_params[2];
    static RDB_parameter open_params[3];
    static RDB_parameter close_params[1];

    print_string_params[0].typ = &RDB_STRING;
    print_string_params[0].update = RDB_FALSE;
    print_int_params[0].typ = &RDB_INTEGER;
    print_int_params[0].update = RDB_FALSE;
    print_float_params[0].typ = &RDB_FLOAT;
    print_float_params[0].update = RDB_FALSE;
    print_bool_params[0].typ = &RDB_BOOLEAN;
    print_bool_params[0].update = RDB_FALSE;

    print_iostream_string_params[0].typ = &RDB_IO_STREAM;
    print_iostream_string_params[0].update = RDB_FALSE;
    print_iostream_string_params[1].typ = &RDB_STRING;
    print_iostream_string_params[1].update = RDB_FALSE;
    print_iostream_int_params[0].typ = &RDB_IO_STREAM;
    print_iostream_int_params[0].update = RDB_FALSE;
    print_iostream_int_params[1].typ = &RDB_INTEGER;
    print_iostream_int_params[1].update = RDB_FALSE;
    print_iostream_float_params[0].typ = &RDB_IO_STREAM;
    print_iostream_float_params[0].update = RDB_FALSE;
    print_iostream_float_params[1].typ = &RDB_FLOAT;
    print_iostream_float_params[1].update = RDB_FALSE;
    print_iostream_bool_params[0].typ = &RDB_IO_STREAM;
    print_iostream_bool_params[0].update = RDB_FALSE;
    print_iostream_bool_params[1].typ = &RDB_BOOLEAN;
    print_iostream_bool_params[1].update = RDB_FALSE;

    readln_params[0].typ = &RDB_STRING;
    readln_params[0].update = RDB_TRUE;
    readln_iostream_params[0].typ = &RDB_IO_STREAM;
    readln_iostream_params[0].update = RDB_FALSE;
    readln_iostream_params[1].typ = &RDB_STRING;
    readln_iostream_params[1].update = RDB_TRUE;

    close_params[0].typ = &RDB_IO_STREAM;
    close_params[0].update = RDB_FALSE;

    open_params[0].typ = &RDB_IO_STREAM;
    open_params[0].update = RDB_TRUE;
    open_params[1].typ = &RDB_STRING;
    open_params[1].update = RDB_FALSE;
    open_params[2].typ = &RDB_STRING;
    open_params[2].update = RDB_FALSE;

    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_string_params, &op_println_string,
            ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_int_params, &op_println_int,
            ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_float_params, &op_println_float,
            ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_bool_params, &op_println_bool,
            ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", -1, NULL, &op_println_nonscalar, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_string_params,
            &op_println_iostream_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_int_params,
            &op_println_iostream_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_float_params,
            &op_println_iostream_float, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_bool_params,
            &op_println_iostream_bool, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_string_params, &op_print_string, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_int_params, &op_print_int, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_float_params, &op_print_float, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_bool_params, &op_print_bool, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", -1, NULL, &op_print_nonscalar, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_string_params,
            &op_print_iostream_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_int_params,
            &op_print_iostream_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_float_params,
            &op_print_iostream_float, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_bool_params,
            &op_print_iostream_bool, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "READLN", 1, readln_params, &op_readln, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "READLN", 2, readln_iostream_params,
            &op_readln_iostream, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "CLOSE", 1, close_params, &op_close, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "OPEN", 3, open_params, &op_open, ecp)
            != RDB_OK)
        return RDB_ERROR;

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
