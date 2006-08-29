/*
 * $Id$
 *
 * Copyright (C) 2005-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
#include <regex.h>
#include <string.h>

/*
 * Built-in operators
 */

static int
neq_bool(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->var.bool_val != argv[1]->var.bool_val));
    return RDB_OK;
}

static int
neq_binary(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->var.bin.len != argv[1]->var.bin.len)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else if (argv[0]->var.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->var.bin.datap,
            argv[1]->var.bin.datap, argv[0]->var.bin.len) != 0));
    return RDB_OK;
}

static int 
op_vtable(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    RDB_expression *argexp;
    RDB_expression *exp;

    /*
     * Convert arguments to expressions
     */
    exp = RDB_ro_op(name, argc, ecp);
    if (exp == NULL)
        return RDB_ERROR;

    for (i = 0; i < argc; i++) {
        if (argv[i]->kind == RDB_OB_TABLE) {
            if (argv[i]->var.tb.is_persistent) {
                argexp = RDB_table_ref_to_expr(argv[i], ecp);
            } else if (argv[i]->var.tb.exp != NULL) {
                argexp = RDB_dup_expr(argv[i]->var.tb.exp, ecp);
            } else {
                argexp = RDB_obj_to_expr(argv[i], ecp);
            }
        } else {
            argexp = RDB_obj_to_expr(argv[i], ecp);
        }
        if (argexp == NULL) {
            RDB_drop_expr(exp, ecp);
            return RDB_ERROR;
        }
        RDB_add_arg(exp, argexp);
    }

    /*
     * Create virtual table
     */
    if (_RDB_vtexp_to_obj(exp, ecp, txp, retvalp) != RDB_OK) {
        RDB_drop_expr(exp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_vtable_wrapfn(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return op_vtable(name, argc, argv, ecp, txp, retvalp);
}

static int
op_project(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    for (i = 1; i < argc; i++) {
        char *attrname = RDB_obj_string(argv[i]);

        if (RDB_tuple_set(retvalp, attrname,
                RDB_tuple_get(argv[0], attrname), ecp) != RDB_OK)
            return RDB_ERROR;
    }
    return RDB_OK;
}

static int
op_remove(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    char **attrv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    attrv = malloc(sizeof (char *) * (argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    ret = RDB_remove_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    free(attrv);
    return ret;
}

static int
op_rename(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int i;
    int ret;
    int renc = (argc - 1) / 2;
    RDB_renaming *renv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    renv = malloc(sizeof(RDB_renaming) * renc);
    if (renv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < renc; i++) {
        if (argv[1 + i]->typ != &RDB_STRING
                || argv[2 + i]->typ != &RDB_STRING) {
            free(renv);
            RDB_raise_type_mismatch("RENAME argument must be STRING", ecp);
            return RDB_ERROR;
        }
        renv[i].from = RDB_obj_string(argv[1 + i * 2]);
        renv[i].to = RDB_obj_string(argv[2 + i * 2]);
    }

    ret = RDB_rename_tuple(argv[0], renc, renv, ecp, retvalp);
    free(renv);
    return ret;
}

static int
op_join(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{    
    if (argc != 2) {
        RDB_raise_invalid_argument("invalid argument to JOIN", ecp);
        return RDB_ERROR;
    }

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    return RDB_join_tuples(argv[0], argv[1], ecp, txp, retvalp);
}

static int
op_wrap(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i, j;
    int wrapc;
    RDB_wrapping *wrapv;

    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    if (argc < 1 || argc %2 != 1) {
        RDB_raise_invalid_argument("invalid number of arguments", ecp);
        return RDB_ERROR;
    }

    wrapc = argc % 2;
    if (wrapc > 0) {
        wrapv = malloc(sizeof(RDB_wrapping) * wrapc);
        if (wrapv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }
    for (i = 0; i < wrapc; i++) {
        wrapv[i].attrv = NULL;
    }

    for (i = 0; i < wrapc; i++) {
        RDB_object *objp;

        wrapv[i].attrc =  RDB_array_length(argv[i * 2 + 1], ecp);
        wrapv[i].attrv = malloc(sizeof (char *) * wrapv[i].attrc);
        if (wrapv[i].attrv == NULL) {
            RDB_raise_no_memory(ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
        for (j = 0; j < wrapv[i].attrc; j++) {
            objp = RDB_array_get(argv[i * 2 + 1], (RDB_int) j, ecp);
            if (objp == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }
            wrapv[i].attrv[j] = RDB_obj_string(objp);
        }        
        wrapv[i].attrname = RDB_obj_string(argv[i * 2 + 2]);
    }

    ret = RDB_wrap_tuple(argv[0], wrapc, wrapv, ecp, retvalp);

cleanup:
    for (i = 0; i < wrapc; i++) {
        free(wrapv[i].attrv);
    }
    if (wrapc > 0)
        free(wrapv);

    return ret;
}

static int
op_unwrap(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int ret;
    int i;
    char **attrv;

    if (argc < 1) {
        RDB_raise_invalid_argument("invalid argument to UNWRAP", ecp);
        return RDB_ERROR;
    }
    
    if (argv[0]->kind == RDB_OB_TABLE)
        return op_vtable(name, argc, argv, ecp, txp, retvalp);

    attrv = malloc(sizeof(char *) * (argc - 1));
    if (attrv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < argc - 1; i++) {
        attrv[i] = RDB_obj_string(argv[i + 1]);
    }

    ret = RDB_unwrap_tuple(argv[0], argc - 1, attrv, ecp, retvalp);
    free(attrv);
    return ret;
}

static int
integer_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->var.float_val);
    return RDB_OK;
}

static int
integer_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->var.double_val);
    return RDB_OK;
}

static int
integer_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_int_to_obj(retvalp, (RDB_int)
            strtol(argv[0]->var.bin.datap, &endp, 10));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to INTEGER failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
float_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) argv[0]->var.int_val);
    return RDB_OK;
}

static int
float_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) argv[0]->var.double_val);
    return RDB_OK;
}

static int
float_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_float_to_obj(retvalp, (RDB_float)
            strtod(argv[0]->var.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to DOUBLE failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
double_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp, (RDB_double) argv[0]->var.int_val);
    return RDB_OK;
}

static int
double_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp, (RDB_double) argv[0]->var.float_val);
    return RDB_OK;
}

static int
double_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_double_to_obj(retvalp, (RDB_double)
            strtod(argv[0]->var.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to DOUBLE failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
string_obj(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_obj_to_string(retvalp, argv[0], ecp);
}

static int
length_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    size_t len = mbstowcs(NULL, argv[0]->var.bin.datap, 0);
    if (len == -1) {
        RDB_raise_invalid_argument("", ecp);
        return RDB_ERROR;
    }

    RDB_int_to_obj(retvalp, (RDB_int) len);
    return RDB_OK;
}

static int
substring(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    int start = argv[1]->var.int_val;
    int len = argv[2]->var.int_val;
    int i;
    int cl;
    int bstart, blen;

    /* Operands must not be negative */
    if (len < 0 || start < 0) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    /* Find start of substring */
    bstart = 0;
    for (i = 0; i < start && bstart < argv[0]->var.bin.len - 1; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart, 4);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
            return RDB_ERROR;
        }
        bstart += cl;
    }
    if (bstart >= argv[0]->var.bin.len - 1) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    /* Find end of substring */
    blen = 0;
    for (i = 0; i < len && bstart + blen < argv[0]->var.bin.len; i++) {
        cl = mblen(((char *) argv[0]->var.bin.datap) + bstart + blen, 4);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
            return RDB_ERROR;
        }
        blen += cl > 0 ? cl : 1;
    }
    if (bstart + blen >= argv[0]->var.bin.len) {
        RDB_raise_invalid_argument("invalid SUBSTRING argument", ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(retvalp, ecp);
    retvalp->typ = &RDB_STRING;
    retvalp->kind = RDB_OB_BIN;
    retvalp->var.bin.len = blen + 1;
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    strncpy(retvalp->var.bin.datap, (char *) argv[0]->var.bin.datap
            + bstart, blen);
    ((char *) retvalp->var.bin.datap)[blen] = '\0';
    return RDB_OK;
}

static int
concat(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    size_t s1len = strlen(argv[0]->var.bin.datap);

    RDB_destroy_obj(retvalp, ecp);
    RDB_init_obj(retvalp);
    _RDB_set_obj_type(retvalp, &RDB_STRING);
    retvalp->var.bin.len = s1len + strlen(argv[1]->var.bin.datap) + 1;
    retvalp->var.bin.datap = malloc(retvalp->var.bin.len);
    if (retvalp->var.bin.datap == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    strcpy(retvalp->var.bin.datap, argv[0]->var.bin.datap);
    strcpy(((char *)retvalp->var.bin.datap) + s1len, argv[1]->var.bin.datap);
    return RDB_OK;
}

static int
matches(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    regex_t reg;
    int ret;

    ret = regcomp(&reg, argv[1]->var.bin.datap, REG_NOSUB);
    if (ret != 0) {
        RDB_raise_invalid_argument("invalid regular expression", ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool)
            (regexec(&reg, argv[0]->var.bin.datap, 0, NULL, 0) == 0));
    regfree(&reg);

    return RDB_OK;
}

static int
and(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val && argv[1]->var.bool_val);
    return RDB_OK;
}

static int
or(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->var.bool_val || argv[1]->var.bool_val);
    return RDB_OK;
}

static int
not(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool) !argv[0]->var.bool_val);
    return RDB_OK;
}

static int
lt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) < 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
let(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) <= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
gt(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) > 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
get(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;
    
    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->comparep)("compare", 2, argv, NULL, 0, ecp, txp,
            &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) >= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
negate_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, -argv[0]->var.int_val);
    return RDB_OK;
}

static int
negate_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, -argv[0]->var.float_val);
    return RDB_OK;
}

static int
negate_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp, -argv[0]->var.double_val);
    return RDB_OK;
}

static int
add_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val + argv[1]->var.int_val);
    return RDB_OK;
}

static int
add_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val + argv[1]->var.double_val);
    return RDB_OK;
}

static int
subtract_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val - argv[1]->var.int_val);
    return RDB_OK;
}

static int
subtract_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val - argv[1]->var.float_val);
    return RDB_OK;
}

static int
subtract_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val - argv[1]->var.double_val);
    return RDB_OK;
}

static int
multiply_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val * argv[1]->var.int_val);
    return RDB_OK;
}

static int
multiply_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val * argv[1]->var.float_val);
    return RDB_OK;
}

static int
multiply_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val * argv[1]->var.double_val);
    return RDB_OK;
}

static int
divide_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.int_val == 0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(retvalp, argv[0]->var.int_val / argv[1]->var.int_val);
    return RDB_OK;
}

static int
divide_float(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.float_val == 0.0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp,
            argv[0]->var.float_val / argv[1]->var.float_val);
    return RDB_OK;
}

static int
divide_double(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->var.double_val == 0.0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_double_to_obj(retvalp,
            argv[0]->var.double_val / argv[1]->var.double_val);
    return RDB_OK;
}

int
_RDB_add_builtin_ops(RDB_dbroot *dbrootp, RDB_exec_context *ecp)
{
    RDB_ro_op_desc *op;
    int ret;

    op = _RDB_new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("INTEGER", 1, &RDB_INTEGER, &integer_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("FLOAT", 1, &RDB_FLOAT, &float_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("FLOAT", 1, &RDB_FLOAT, &float_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("FLOAT", 1, &RDB_FLOAT, &float_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DOUBLE", 1, &RDB_DOUBLE, &double_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DOUBLE", 1, &RDB_DOUBLE, &double_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DOUBLE", 1, &RDB_DOUBLE, &double_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("STRING", 1, &RDB_STRING, &string_obj, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("STRING", 1, &RDB_STRING, &string_obj, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("STRING", 1, &RDB_STRING, &string_obj, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("LENGTH", 1, &RDB_INTEGER, &length_string, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("SUBSTRING", 3, &RDB_STRING, &substring, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_INTEGER;
    op->argtv[2] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("||", 2, &RDB_STRING, &concat, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("MATCHES", 2, &RDB_BOOLEAN, &matches, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("AND", 2, &RDB_BOOLEAN, &and, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("OR", 2, &RDB_BOOLEAN, &or, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("NOT", 1, &RDB_BOOLEAN, &not, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<", 2, &RDB_BOOLEAN, &lt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<=", 2, &RDB_BOOLEAN, &let, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">", 2, &RDB_BOOLEAN, &gt, ecp);
    if (op == NULL) { 
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op(">=", 2, &RDB_BOOLEAN, &get, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, &_RDB_eq_bool, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, _RDB_obj_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("=", 2, &RDB_BOOLEAN, &_RDB_eq_binary, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_bool, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BOOLEAN;
    op->argtv[1] = &RDB_BOOLEAN;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &_RDB_obj_not_equals, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_STRING;
    op->argtv[1] = &RDB_STRING;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("<>", 2, &RDB_BOOLEAN, &neq_binary, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_BINARY;
    op->argtv[1] = &RDB_BINARY;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 1, &RDB_INTEGER, &negate_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 1, &RDB_FLOAT, &negate_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 1, &RDB_DOUBLE, &negate_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("+", 2, &RDB_INTEGER, &add_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("+", 2, &RDB_FLOAT, &add_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("+", 2, &RDB_DOUBLE, &add_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 2, &RDB_INTEGER, &subtract_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 2, &RDB_FLOAT, &subtract_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("-", 2, &RDB_DOUBLE, &subtract_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("*", 2, &RDB_INTEGER, &multiply_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("*", 2, &RDB_FLOAT, &multiply_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("*", 2, &RDB_DOUBLE, &multiply_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("/", 2, &RDB_INTEGER, &divide_int, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_INTEGER;
    op->argtv[1] = &RDB_INTEGER;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    op = _RDB_new_ro_op("/", 2, &RDB_FLOAT, &divide_float, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_FLOAT;
    op->argtv[1] = &RDB_FLOAT;

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("/", 2, &RDB_DOUBLE, &divide_double, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    op->argtv[0] = &RDB_DOUBLE;
    op->argtv[1] = &RDB_DOUBLE;

    if (_RDB_put_ro_op(dbrootp, op, ecp) != RDB_OK)
        return RDB_ERROR;

    op = _RDB_new_ro_op("PROJECT", -1, NULL, &op_project, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    if (_RDB_put_ro_op(dbrootp, op, ecp) != RDB_OK)
        return RDB_ERROR;

    op = _RDB_new_ro_op("REMOVE", -1, NULL, &op_remove, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("RENAME", -1, NULL, &op_rename, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("JOIN", -1, NULL, &op_join, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("WRAP", -1, NULL, &op_wrap, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("UNWRAP", -1, NULL, &op_unwrap, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("UNION", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("MINUS", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("SEMIMINUS", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("INTERSECT", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("SEMIJOIN", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("DIVIDE_BY_PER", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("GROUP", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    op = _RDB_new_ro_op("UNGROUP", -1, NULL, &op_vtable_wrapfn, ecp);
    if (op == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    ret = _RDB_put_ro_op(dbrootp, op, ecp);
    if (ret != RDB_OK)
        return ret;

    return RDB_OK;
}
