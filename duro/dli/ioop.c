/*
 * $Id$
 *
 * Copyright (C) 2009 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 * I/O operators.
 */

#include <rel/rdb.h>
#include <rel/internal.h>
#include "tabletostr.h"

#include <errno.h>
#include <string.h>

enum {
    FILETAB_LEN = 64
};

static FILE *iostreams[FILETAB_LEN] = {};

static int
op_println_string(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (puts(RDB_obj_string(argv[0])) == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_int(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%d\n", (int) RDB_obj_int(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_float(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%f\n", (double) RDB_obj_float(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_bool(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (puts(RDB_obj_bool(argv[0]) ? "TRUE" : "FALSE") == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int print_nonscalar(FILE *fp, const RDB_object *objp, 
    RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object strobj;
    
    RDB_init_obj(&strobj);
    if (_RDB_obj_to_str(&strobj, objp, ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&strobj, ecp);
        return RDB_ERROR;
    }
    if (fputs(RDB_obj_string(&strobj), stdout) == EOF) {
        RDB_destroy_obj(&strobj, ecp);
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
        
    RDB_destroy_obj(&strobj, ecp);
    return RDB_OK;
}

/*
 * Get file number from IO_STREAM
 */
static int get_fileno(const RDB_object *objp, RDB_exec_context *ecp)
{
    int fileno = (int) RDB_obj_int(objp);
    if (fileno >= FILETAB_LEN || iostreams[fileno] == NULL) {
        RDB_raise_invalid_argument("invalid file number", ecp);
        return RDB_ERROR;
    }
    return fileno;
}

static int
op_println_nonscalar(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
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
        RDB_raise_type_mismatch(name, ecp);
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
    if (puts("") == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_iostream_string(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }
    
    if (fprintf(iostreams[fileno], "%s\n", RDB_obj_string(argv[1])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_iostream_int(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fileno], "%d\n", (int) RDB_obj_int(argv[1])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_iostream_float(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fileno], "%f\n", (double) RDB_obj_float(argv[1])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_println_iostream_bool(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fileno], "%s\n",
            RDB_obj_bool(argv[1]) ? "TRUE" : "FALSE") == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_string(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_string(argv[0]), stdout) == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_int(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%d", (int) RDB_obj_int(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_float(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (printf("%f", (double) RDB_obj_float(argv[0])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_bool(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (fputs(RDB_obj_bool(argv[0]) ? "TRUE" : "FALSE", stdout) == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_nonscalar(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
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
        RDB_raise_type_mismatch(name, ecp);
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
op_print_iostream_string(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fputs(RDB_obj_string(argv[1]), iostreams[fileno]) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_iostream_int(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fileno], "%d", (int) RDB_obj_int(argv[1])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_iostream_float(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fprintf(iostreams[fileno], "%f", (double) RDB_obj_float(argv[1])) < 0) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_print_iostream_bool(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg #1 */
    int fileno = get_fileno(argv[0], ecp);
    if (fileno == RDB_ERROR) {
        return RDB_ERROR;
    }

    if (fputs(RDB_obj_bool(argv[1]) ? "TRUE" : "FALSE", iostreams[fileno]) == EOF) {
        _RDB_handle_errcode(errno, ecp, txp);
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

    if (fgets(buf, sizeof(buf), stdin) == NULL) {
        if (ferror(stdin)) {
            _RDB_handle_errcode(errno, ecp, txp);
            return RDB_ERROR;
        }
        return RDB_OK;
    }
    len = strlen(buf);

    /* Read until a complete line has been read */
    while (buf[len - 1] != '\n') {
        if (RDB_append_string(linep, buf, ecp) != RDB_OK)
            return RDB_ERROR;
        if (fgets(buf, sizeof(buf), stdin) == NULL) {
            if (ferror(stdin)) {
                _RDB_handle_errcode(errno, ecp, txp);
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
op_readln(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    return readln(stdin, argv[0], ecp, txp);
}

static int
op_close(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /* Get file number from arg*/
    int fno = RDB_obj_int(argv[0]);
    if (fno >= FILETAB_LEN || iostreams[fno] == NULL) {
        RDB_raise_invalid_argument("invalid file number", ecp);
        return RDB_ERROR;
    }
    if (fclose(iostreams[fno]) != 0) {
        iostreams[fno] = NULL;
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    iostreams[fno] = NULL;
    return RDB_OK;
}

static int
op_open(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int fno;
    int ret;
    RDB_object fobj;
    RDB_object *fobjp;

    /* Open file */
    FILE *fp = fopen(RDB_obj_string(argv[1]), RDB_obj_string(argv[2]));
    if (fp == NULL) {
        _RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }

    /* Get file number */
    fno = fileno(fp);
    if (fno >= FILETAB_LEN) {
        RDB_raise_not_supported("file number too high", ecp);
        return RDB_ERROR;
    }

    iostreams[fno] = fp;

    /* Set argument #3 by calling the IO_STREAM selector */
    RDB_init_obj(&fobj);
    RDB_int_to_obj(&fobj, (RDB_int) fno);
    fobjp = &fobj;
    ret = RDB_call_ro_op("IO_STREAM", 1, &fobjp, ecp, NULL, argv[0]);
    RDB_destroy_obj(&fobj, ecp);
    return ret;
}

int
_RDB_add_io_ops(RDB_op_map *opmapp, RDB_exec_context *ecp)
{
    static RDB_type *print_string_types[1];
    static RDB_type *print_int_types[1];
    static RDB_type *print_float_types[1];
    static RDB_type *print_bool_types[1];

    static RDB_type *print_iostream_string_types[2];
    static RDB_type *print_iostream_int_types[2];
    static RDB_type *print_iostream_float_types[2];
    static RDB_type *print_iostream_bool_types[2];

    static RDB_type *readln_types[1];
    static RDB_type *open_types[3];
    static RDB_type *close_types[1];
    
    static RDB_bool print_updv[] = { RDB_FALSE };
    static RDB_bool print_iostream_updv[] = { RDB_FALSE, RDB_FALSE };
    static RDB_bool readln_updv[] = { RDB_TRUE };
    static RDB_bool open_updv[] = { RDB_TRUE, RDB_FALSE, RDB_FALSE };
    static RDB_bool close_updv[] = { RDB_FALSE };

    print_string_types[0] = &RDB_STRING;
    print_int_types[0] = &RDB_INTEGER;
    print_float_types[0] = &RDB_FLOAT;
    print_bool_types[0] = &RDB_BOOLEAN;

    print_iostream_string_types[0] = &RDB_IO_STREAM;
    print_iostream_string_types[1] = &RDB_STRING;
    print_iostream_int_types[0] = &RDB_IO_STREAM;
    print_iostream_int_types[1] = &RDB_INTEGER;
    print_iostream_float_types[0] = &RDB_IO_STREAM;
    print_iostream_float_types[1] = &RDB_FLOAT;
    print_iostream_bool_types[0] = &RDB_IO_STREAM;
    print_iostream_bool_types[1] = &RDB_BOOLEAN;

    readln_types[0] = &RDB_STRING;
    close_types[0] = &RDB_IO_STREAM;

    open_types[0] = &RDB_IO_STREAM;
    open_types[1] = &RDB_STRING;
    open_types[2] = &RDB_STRING;

    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_string_types, &op_println_string,
            print_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_int_types, &op_println_int,
            print_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_float_types, &op_println_float,
            print_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 1, print_bool_types, &op_println_bool,
            print_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", -1, NULL, &op_println_nonscalar, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_string_types,
            &op_println_iostream_string, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_int_types,
            &op_println_iostream_int, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_float_types,
            &op_println_iostream_float, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINTLN", 2, print_iostream_bool_types,
            &op_println_iostream_bool, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_string_types, &op_print_string, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_int_types, &op_print_int, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_float_types, &op_print_float, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 1, print_bool_types, &op_print_bool, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", -1, NULL, &op_print_nonscalar, print_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_string_types,
            &op_print_iostream_string, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_int_types,
            &op_print_iostream_int, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_float_types,
            &op_print_iostream_float, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_upd_op(opmapp, "PRINT", 2, print_iostream_bool_types,
            &op_print_iostream_bool, print_iostream_updv, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "READLN", 1, readln_types, &op_readln, readln_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "CLOSE", 1, close_types, &op_close, close_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_upd_op(opmapp, "OPEN", 3, open_types, &op_open, open_updv, ecp)
            != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}
