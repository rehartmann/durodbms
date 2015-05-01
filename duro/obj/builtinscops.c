/*
 * Scalar built-in read-only operators.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "builtinscops.h"
#include "builtintypes.h"
#include "objinternal.h"

#include <string.h>
#include <math.h>
#include <regex.h>
#ifdef _WIN32
#include "Shlwapi.h"
#else
#include <fnmatch.h>
#endif

#include <stdio.h>

int
RDB_eq_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->val.bool_val == argv[1]->val.bool_val));
    return RDB_OK;
}

int
RDB_eq_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->val.bin.len != argv[1]->val.bin.len)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else if (argv[0]->val.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->val.bin.datap,
            argv[1]->val.bin.datap, argv[0]->val.bin.len) == 0));
    return RDB_OK;
}

static int
cast_as_integer_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, (RDB_int) argv[0]->val.float_val);
    return RDB_OK;
}

static int
neq_bool(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp,
            (RDB_bool) (argv[0]->val.bool_val != argv[1]->val.bool_val));
    return RDB_OK;
}

static int
neq_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[0]->val.bin.len != argv[1]->val.bin.len)
        RDB_bool_to_obj(retvalp, RDB_TRUE);
    else if (argv[0]->val.bin.len == 0)
        RDB_bool_to_obj(retvalp, RDB_FALSE);
    else
        RDB_bool_to_obj(retvalp, (RDB_bool) (memcmp(argv[0]->val.bin.datap,
            argv[1]->val.bin.datap, argv[0]->val.bin.len) != 0));
    return RDB_OK;
}

static int
cast_as_integer_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_int_to_obj(retvalp, (RDB_int)
            strtol(argv[0]->val.bin.datap, &endp, 10));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to integer failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
cast_as_float_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) argv[0]->val.int_val);
    return RDB_OK;
}

static int
cast_as_float_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *endp;

    RDB_float_to_obj(retvalp, (RDB_float)
            strtod(argv[0]->val.bin.datap, &endp));
    if (*endp != '\0') {
        RDB_raise_invalid_argument("conversion to float failed", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
cast_as_string(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_obj_to_string(retvalp, argv[0], ecp);
}

static int
cast_as_binary(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_binary_set(retvalp, 0, argv[0]->val.bin.datap,
            argv[0]->val.bin.len - 1, ecp);
}

static int
op_strlen(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    size_t len = mbstowcs(NULL, argv[0]->val.bin.datap, 0);
    if (len == -1) {
        RDB_raise_invalid_argument("obtaining string length failed", ecp);
        return RDB_ERROR;
    }

    RDB_int_to_obj(retvalp, (RDB_int) len);
    return RDB_OK;
}

static int
op_substr(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    int start = argv[1]->val.int_val;
    int len = argv[2]->val.int_val;
    int i;
    int cl;
    int bstart, blen;

    /* Operands must not be negative */
    if (len < 0 || start < 0) {
        RDB_raise_invalid_argument("invalid substr argument", ecp);
        return RDB_ERROR;
    }

    /* Find start of substring */
    bstart = 0;
    for (i = 0; i < start && bstart < argv[0]->val.bin.len - 1; i++) {
        cl = mblen(((char *) argv[0]->val.bin.datap) + bstart, MB_CUR_MAX);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid substr argument", ecp);
            return RDB_ERROR;
        }
        bstart += cl;
    }
    if (bstart >= argv[0]->val.bin.len - 1) {
        RDB_raise_invalid_argument("invalid substr argument", ecp);
        return RDB_ERROR;
    }

    /* Find end of substring */
    blen = 0;
    for (i = 0; i < len && bstart + blen < argv[0]->val.bin.len; i++) {
        cl = mblen(((char *) argv[0]->val.bin.datap) + bstart + blen,
                MB_CUR_MAX);
        if (cl == -1) {
            RDB_raise_invalid_argument("invalid substr argument", ecp);
            return RDB_ERROR;
        }
        blen += cl > 0 ? cl : 1;
    }
    if (bstart + blen >= argv[0]->val.bin.len) {
        RDB_raise_invalid_argument("invalid substr argument", ecp);
        return RDB_ERROR;
    }

    return RDB_string_n_to_obj(retvalp,
            (char *) argv[0]->val.bin.datap + bstart, blen, ecp);
}

static int
op_substr_b_remaining(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    const char *strp = RDB_obj_string(argv[0]);
    int pos = (int) RDB_obj_int(argv[1]);
    if (pos < 0 || pos > strlen(strp)) {
        RDB_raise_invalid_argument("invalid substr_b argument", ecp);
        return RDB_ERROR;
    }

    return RDB_string_to_obj(retvalp, strp + pos, ecp);
}

static int
op_substr_b(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    const char *strp = RDB_obj_string(argv[0]);
    int pos = (int) RDB_obj_int(argv[1]);
    RDB_int len = RDB_obj_int(argv[2]);

    if (pos < 0 || len < 0 || pos + len > strlen(strp) ) {
        RDB_raise_invalid_argument("invalid substr_b argument", ecp);
        return RDB_ERROR;
    }

    return RDB_string_n_to_obj(retvalp, strp + pos, (size_t) len, ecp);
}

static int
op_strfind_b(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    const char *haystack = RDB_obj_string(argv[0]);

    char *substrp = strstr(haystack, RDB_obj_string(argv[1]));

    if (substrp == NULL) {
        RDB_int_to_obj(retvalp, (RDB_int) -1);
    } else {
        RDB_int_to_obj(retvalp, (RDB_int) (substrp - haystack));
    }
    return RDB_OK;
}

static int
op_strfind_b_pos(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *substrp;
    const char *haystack = RDB_obj_string(argv[0]);
    int pos = (int) RDB_obj_int(argv[2]);

    if (pos < 0 || pos > strlen(haystack)) {
        RDB_raise_invalid_argument("Invalid strfind_b argument", ecp);
        return RDB_ERROR;
    }

    substrp = strstr(haystack + pos, RDB_obj_string(argv[1]));

    if (substrp == NULL) {
        RDB_int_to_obj(retvalp, (RDB_int) -1);
    } else {
        RDB_int_to_obj(retvalp, (RDB_int) (substrp - haystack));
    }
    return RDB_OK;
}

static int
op_concat(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    size_t s1len = strlen(argv[0]->val.bin.datap);
    size_t dstsize = s1len + strlen(argv[1]->val.bin.datap) + 1;

    if (retvalp->kind == RDB_OB_INITIAL) {
        /* Turn *retvalp into a string */
        RDB_set_obj_type(retvalp, &RDB_STRING);
        retvalp->val.bin.datap = RDB_alloc(dstsize, ecp);
        if (retvalp->val.bin.datap == NULL) {
            return RDB_ERROR;
        }
        retvalp->val.bin.len = dstsize;
    } else if (retvalp->typ == &RDB_STRING) {
        /* Grow string if necessary */
        if (retvalp->val.bin.len < dstsize) {
            void *datap = RDB_realloc(retvalp->val.bin.datap, dstsize, ecp);
            if (datap == NULL) {
                return RDB_ERROR;
            }
            retvalp->val.bin.datap = datap;
        }
    } else {
        RDB_raise_type_mismatch("invalid return type for || operator", ecp);
        return RDB_ERROR;
    }
    strcpy(retvalp->val.bin.datap, argv[0]->val.bin.datap);
    strcpy(((char *)retvalp->val.bin.datap) + s1len, argv[1]->val.bin.datap);
    return RDB_OK;
}

static int
op_starts_with(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *str = RDB_obj_string(argv[0]);
    char *pref = RDB_obj_string(argv[1]);
    size_t preflen = strlen(pref);

    if (preflen > strlen(str)) {
        RDB_bool_to_obj(retvalp, RDB_FALSE);
        return RDB_OK;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool) (strncmp(str, pref, preflen) == 0));
    return RDB_OK;
}

static int
op_like(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
#ifdef _WIN32
    BOOL res = PathMatchSpec(RDB_obj_string(argv[0]), RDB_obj_string(argv[1]));
    RDB_bool_to_obj(retvalp, (RDB_bool) res);
#else /* Use POSIX fnmatch() */
    int ret = fnmatch(RDB_obj_string(argv[1]), RDB_obj_string(argv[0]),
            FNM_NOESCAPE);
    if (ret != 0 && ret != FNM_NOMATCH) {
        RDB_raise_system("fnmatch() failed", ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool) (ret == 0));
#endif
    return RDB_OK;
}

static int
op_regex_like(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    regex_t reg;
    int ret;

    ret = regcomp(&reg, argv[1]->val.bin.datap, REG_EXTENDED);
    if (ret != 0) {
        RDB_raise_invalid_argument("invalid regular expression", ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, (RDB_bool)
            (regexec(&reg, argv[0]->val.bin.datap, 0, NULL, 0) == 0));
    regfree(&reg);

    return RDB_OK;
}

static int
and(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->val.bool_val && argv[1]->val.bool_val);
    return RDB_OK;
}

static int
or(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->val.bool_val || argv[1]->val.bool_val);
    return RDB_OK;
}

static int
xor(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool)
            argv[0]->val.bool_val != argv[1]->val.bool_val);
    return RDB_OK;
}

static int
not(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_bool_to_obj(retvalp, (RDB_bool) !argv[0]->val.bool_val);
    return RDB_OK;
}

static int
lt(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;

    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) < 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
let(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;

    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return RDB_ERROR;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) <= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
gt(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;

    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) > 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
get(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_object retval;
    int ret;

    RDB_init_obj(&retval);
    ret = (*argv[0]->typ->compare_op->opfn.ro_fp) (2, argv,
            argv[0]->typ->compare_op, ecp, txp, &retval);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&retval, ecp);
        return ret;
    }
    RDB_bool_to_obj(retvalp, RDB_obj_int(&retval) >= 0);
    RDB_destroy_obj(&retval, ecp);
    return RDB_OK;
}

static int
negate_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, -argv[0]->val.int_val);
    return RDB_OK;
}

static int
negate_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, -argv[0]->val.float_val);
    return RDB_OK;
}

static int
add_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->val.int_val + argv[1]->val.int_val);
    return RDB_OK;
}

static int
add_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val + argv[1]->val.float_val);
    return RDB_OK;
}

static int
subtract_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->val.int_val - argv[1]->val.int_val);
    return RDB_OK;
}

static int
subtract_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val - argv[1]->val.float_val);
    return RDB_OK;
}

static int
multiply_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->val.int_val * argv[1]->val.int_val);
    return RDB_OK;
}

static int
multiply_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val * argv[1]->val.float_val);
    return RDB_OK;
}

static int
divide_int(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->val.int_val == 0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(retvalp, argv[0]->val.int_val / argv[1]->val.int_val);
    return RDB_OK;
}

static int
divide_float(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    if (argv[1]->val.float_val == 0.0) {
        RDB_raise_invalid_argument("division by zero", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp,
            argv[0]->val.float_val / argv[1]->val.float_val);
    return RDB_OK;
}

static int
math_sqrt(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    double d = (double) RDB_obj_float(argv[0]);
    if (d < 0.0) {
        RDB_raise_invalid_argument(
                "square root of negative number is undefined", ecp);
        return RDB_ERROR;
    }
    RDB_float_to_obj(retvalp, (RDB_float) sqrt(d));
    return RDB_OK;
}

static int
math_sin(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) sin(RDB_obj_float(argv[0])));
    return RDB_OK;
}

static int
math_cos(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) cos(RDB_obj_float(argv[0])));
    return RDB_OK;
}

static int
math_atan(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp, (RDB_float) atan((double) RDB_obj_float(argv[0])));
    return RDB_OK;
}

static int
math_atan2(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    RDB_float_to_obj(retvalp,
            (RDB_float) atan2((double) RDB_obj_float(argv[0]),
                              (double) RDB_obj_float(argv[1])));
    return RDB_OK;
}

static int
op_getenv(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    char *valp = getenv(RDB_obj_string(argv[0]));

    /* If the environment variable does not exist, return empty string */
    return RDB_string_to_obj(retvalp, valp != NULL ? valp : "", ecp);
}

int
RDB_add_builtin_scalar_ro_ops(RDB_op_map *opmap, RDB_exec_context *ecp)
{
    RDB_type *paramtv[6];

    paramtv[0] = &RDB_FLOAT;
    if (RDB_put_ro_op(opmap, "cast_as_integer", 1, paramtv, &RDB_INTEGER,
            &cast_as_integer_float, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_int", 1, paramtv, &RDB_INTEGER,
            &cast_as_integer_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "cast_as_integer", 1, paramtv, &RDB_INTEGER, &cast_as_integer_string,
            ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_int", 1, paramtv, &RDB_INTEGER, &cast_as_integer_string,
            ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "cast_as_float", 1, paramtv, &RDB_FLOAT,
            &cast_as_float_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rat", 1, paramtv, &RDB_FLOAT,
            &cast_as_float_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rational", 1, paramtv, &RDB_FLOAT,
            &cast_as_float_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "cast_as_float", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rational", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_rat", 1, paramtv, &RDB_FLOAT, &cast_as_float_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    if (RDB_put_ro_op(opmap, "cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BINARY;
    if (RDB_put_ro_op(opmap, "cast_as_string", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_ro_op(opmap, "cast_as_char", 1, paramtv, &RDB_STRING, &cast_as_string, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "cast_as_binary", 1, paramtv, &RDB_BINARY, &cast_as_binary, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "strlen", 1, paramtv, &RDB_INTEGER, &op_strlen, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_INTEGER;
    paramtv[2] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "substr", 3, paramtv, &RDB_STRING, &op_substr, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_INTEGER;
    paramtv[2] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "substr_b", 3, paramtv, &RDB_STRING, &op_substr_b, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "substr_b", 2, paramtv, &RDB_STRING,
            &op_substr_b_remaining, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "strfind_b", 2, paramtv, &RDB_INTEGER, &op_strfind_b, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;
    paramtv[2] = &RDB_INTEGER;
    if (RDB_put_ro_op(opmap, "strfind_b", 3, paramtv, &RDB_INTEGER, &op_strfind_b_pos,
            ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;
    if (RDB_put_ro_op(opmap, "||", 2, paramtv, &RDB_STRING, &op_concat, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, "starts_with", 2, paramtv, &RDB_BOOLEAN, &op_starts_with, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "like", 2, paramtv, &RDB_BOOLEAN, &op_like, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "regex_like", 2, paramtv, &RDB_BOOLEAN,
            &op_regex_like, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "and", 2, paramtv, &RDB_BOOLEAN, &and, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "or", 2, paramtv, &RDB_BOOLEAN, &or, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "xor", 2, paramtv, &RDB_BOOLEAN, &xor, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "not", 1, paramtv, &RDB_BOOLEAN, &not, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, "<", 2, paramtv, &RDB_BOOLEAN, &lt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, "<=", 2, paramtv, &RDB_BOOLEAN, &let, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, ">", 2, paramtv, &RDB_BOOLEAN, &gt, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;
    paramtv[1] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_DATETIME;
    paramtv[1] = &RDB_DATETIME;

    if (RDB_put_ro_op(opmap, ">=", 2, paramtv, &RDB_BOOLEAN, &get, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "=", 2, paramtv, &RDB_BOOLEAN, &RDB_eq_bool, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    paramtv[0] = &RDB_BINARY;
    paramtv[1] = &RDB_BINARY;

    if (RDB_put_ro_op(opmap, "=", 2, paramtv, &RDB_BOOLEAN, &RDB_eq_binary, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_BOOLEAN;
    paramtv[1] = &RDB_BOOLEAN;

    if (RDB_put_ro_op(opmap, "<>", 2, paramtv, &RDB_BOOLEAN, &neq_bool, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;


    paramtv[0] = &RDB_BINARY;
    paramtv[1] = &RDB_BINARY;

    if (RDB_put_ro_op(opmap, "<>", 2, paramtv, &RDB_BOOLEAN, &neq_binary, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "-", 1, paramtv, &RDB_INTEGER, &negate_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "-", 1, paramtv, &RDB_FLOAT, &negate_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "+", 2, paramtv, &RDB_INTEGER, &add_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "+", 2, paramtv, &RDB_FLOAT, &add_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "-", 2, paramtv, &RDB_INTEGER, &subtract_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "-", 2, paramtv, &RDB_FLOAT, &subtract_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "*", 2, paramtv, &RDB_INTEGER, &multiply_int, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "*", 2, paramtv, &RDB_FLOAT, &multiply_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_INTEGER;
    paramtv[1] = &RDB_INTEGER;

    if (RDB_put_ro_op(opmap, "/", 2, paramtv, &RDB_INTEGER, &divide_int, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    paramtv[0] = &RDB_FLOAT;
    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "/", 2, paramtv, &RDB_FLOAT, &divide_float, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "sqrt", 1, paramtv, &RDB_FLOAT, &math_sqrt, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "sin", 1, paramtv, &RDB_FLOAT, &math_sin, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "cos", 1, paramtv, &RDB_FLOAT, &math_cos, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_put_ro_op(opmap, "atan", 1, paramtv, &RDB_FLOAT, &math_atan, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[1] = &RDB_FLOAT;

    if (RDB_put_ro_op(opmap, "atan2", 2, paramtv, &RDB_FLOAT, &math_atan2, ecp) != RDB_OK)
        return RDB_ERROR;

    paramtv[0] = &RDB_STRING;

    if (RDB_put_ro_op(opmap, "os.getenv", 1, paramtv, &RDB_STRING, &op_getenv, ecp) != RDB_OK)
        return RDB_ERROR;

    return RDB_OK;
}
