/*
 * Copyright (C) 2003-2009, 2011-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "object.h"
#include "excontext.h"
#include "expression.h"
#include "type.h"
#include "builtintypes.h"
#include "key.h"
#include "objinternal.h"
#include <gen/hashtabit.h>
#include <gen/hashmapit.h>
#include <gen/strfns.h>

#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>

enum {
    STR_BUF_INC = 64
};

/**@defgroup generic Scalar and generic functions
 * @{
 */

/**
 * Allocates <var>size</var> bytes of memory.
 *
 * If allocation fails, NULL is returned and an error valued is stored in *<var>ecp</var>.
 *
 * @returns
 * A pointer to the memory allocated, or NULL if the allocation failed.
 */
void *
RDB_alloc(size_t size, RDB_exec_context *ecp)
{
    void *p = malloc(size);
    if (p == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    return p;
}

/**
 * Changes the sie of the memory block pointed to by <var>p</var> by <var>size</var> bytes.
 *
 * If the operation fails, NULL is returned and an error valued is stored in *<var>ecp</var>.
 *
 * @returns
 * A pointer to the allocated memory.
 */
void *
RDB_realloc(void *p, size_t size, RDB_exec_context *ecp)
{
    p = realloc(p, size);
    if (p == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    return p;
}

/**
 * Frees the memory pointed to by <var>p</var>.
 */
void
RDB_free(void *p)
{
    free(p);
}

RDB_object *
RDB_new_obj(RDB_exec_context *ecp)
{
    RDB_object *objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return NULL;
    }
    RDB_init_obj(objp);
    return objp;
}

int
RDB_free_obj(RDB_object *objp, RDB_exec_context *ecp)
{
    int ret = RDB_destroy_obj(objp, ecp);
    RDB_free(objp);
    return ret;
}

/**
 * RDB_init_obj initializes the RDB_object structure pointed to by
<var>valp</var>. RDB_init_obj must be called before any other
operation can be performed on a RDB_object variable.
 */
void
RDB_init_obj(RDB_object *valp)
{
    valp->kind = RDB_OB_INITIAL;
    valp->typ = NULL;
}

/**
 * RDB_destroy_obj releases all resources associated with a RDB_object
 * structure.
 * If an error occurred, RDB_ERROR is returned and an error valued is stored in *<var>ecp</var>.
 * On success, RDB_OK is returned and *<var>ecp</var> is not modified.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.
 */
int
RDB_destroy_obj(RDB_object *objp, RDB_exec_context *ecp)
{
    /*
     * Do additional cleanup
     */
    if (objp->typ != NULL && objp->typ->cleanup_fp != NULL) {
        if (objp->typ->cleanup_fp(objp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    switch (objp->kind) {
    case RDB_OB_INITIAL:
    case RDB_OB_BOOL:
    case RDB_OB_INT:
    case RDB_OB_FLOAT:
    case RDB_OB_TIME:
        break;
    case RDB_OB_BIN:
        if (objp->val.bin.len > 0)
            RDB_free(objp->val.bin.datap);
        break;
    case RDB_OB_TUPLE:
    {
        RDB_hashtable_iter it;
        tuple_entry *entryp;

        RDB_init_hashtable_iter(&it, (RDB_hashtable *) &objp->val.tpl_tab);
        while ((entryp = RDB_hashtable_next(&it)) != NULL) {
            RDB_destroy_obj(&entryp->obj, ecp);
            RDB_free(entryp->key);
            RDB_free(entryp);
        }
        RDB_destroy_hashtable_iter(&it);
        RDB_destroy_hashtable(&objp->val.tpl_tab);
        break;
    }
    case RDB_OB_ARRAY:
    {
        if (objp->val.arr.elemv != NULL) {
            int i;

            for (i = 0; i < objp->val.arr.capacity; i++)
                RDB_destroy_obj(&objp->val.arr.elemv[i], ecp);
            RDB_free(objp->val.arr.elemv);
        }

        break;
    }
    case RDB_OB_TABLE:
        /* It could be a scalar type with a relation actual rep */
        if (objp->typ != NULL && !RDB_type_is_scalar(objp->typ))
            RDB_del_nonscalar_type(objp->typ, ecp);

        break;
    }

    return RDB_OK;
}

/**
 * RDB_bool_to_obj sets the RDB_object pointed to by <var>valp</var>
to the boolean value specified by <var>v</var>.

The RDB_object must either be newly initialized or of type
boolean.
 */
void
RDB_bool_to_obj(RDB_object *valp, RDB_bool v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_BOOLEAN);

    valp->typ = &RDB_BOOLEAN;
    valp->kind = RDB_OB_BOOL;
    valp->val.bool_val = v;
}

/**
 * RDB_int_to_obj sets the RDB_object pointed to by <var>valp</var>
to the integer value specified by <var>v</var>.

The RDB_object must either be newly initialized or of type
integer.
 */
void
RDB_int_to_obj(RDB_object *valp, RDB_int v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_INTEGER);

    valp->typ = &RDB_INTEGER;
    valp->kind = RDB_OB_INT;
    valp->val.int_val = v;
}

/**
 * RDB_float_to_obj sets the RDB_object pointed to by <var>valp</var>
to the RDB_float value specified by <var>v</var>.

The RDB_object must either be newly initialized or of type
float.
 */
void
RDB_float_to_obj(RDB_object *valp, RDB_float v)
{
    assert(valp->kind == RDB_OB_INITIAL || valp->typ == &RDB_FLOAT);

    valp->typ = &RDB_FLOAT;
    valp->kind = RDB_OB_FLOAT;
    valp->val.float_val = v;
}

/**
 * Sets the RDB_object *<var>objp</var>
 * to the datetime value specified by the argument <var>tm</var>.
 * *<var>objp</var> must either be newly initialized or of type
 * datetime.
 */
void
RDB_tm_to_obj(RDB_object *objp, const struct tm *tm)
{
    assert(objp->kind == RDB_OB_INITIAL || objp->typ == &RDB_DATETIME);

    objp->typ = &RDB_DATETIME;
    objp->kind = RDB_OB_TIME;
    objp->val.time.year = (int16_t) tm->tm_year + 1900;
    objp->val.time.month = tm->tm_mon + 1;
    objp->val.time.day = tm->tm_mday;
    objp->val.time.hour = tm->tm_hour;
    objp->val.time.minute = tm->tm_min;
    objp->val.time.second = tm->tm_sec;
}

static int
set_str_obj_len(RDB_object *objp, size_t len, RDB_exec_context *ecp)
{
    void *datap;

    if (objp->kind != RDB_OB_INITIAL && objp->typ != &RDB_STRING) {
        RDB_raise_type_mismatch("not a string", ecp);
        return RDB_ERROR;
    }

    if (objp->kind == RDB_OB_INITIAL) {
        datap = RDB_alloc(len, ecp);
        if (datap == NULL) {
            return RDB_ERROR;
        }
        objp->typ = &RDB_STRING;
        objp->kind = RDB_OB_BIN;
    } else {
        datap = RDB_realloc(objp->val.bin.datap, len, ecp);
        if (datap == NULL) {
            return RDB_ERROR;
        }
    }
    objp->val.bin.len = len;
    objp->val.bin.datap = datap;
    return RDB_OK;
}

/**
 * RDB_string_to_obj sets the RDB_object pointed to by <var>valp</var>
to the string value specified by <var>str</var>.

The RDB_object must either be newly initialized or of type
string.
 */
int
RDB_string_to_obj(RDB_object *valp, const char *str, RDB_exec_context *ecp)
{
    if (set_str_obj_len(valp, strlen(str) + 1, ecp) != RDB_OK)
        return RDB_ERROR;

    strcpy(valp->val.bin.datap, str);
    return RDB_OK;
}

/**
 * Sets *<var>valp</var> to the string that begins at <var>str</var>
 * limited to a length of <var>n</var> bytes.
<var>valp</var> must either be newly initialized or of type
string.
 */
int
RDB_string_n_to_obj(RDB_object *valp, const char *str, size_t n,
        RDB_exec_context *ecp)
{
    if (set_str_obj_len(valp, n + 1, ecp) != RDB_OK)
        return RDB_ERROR;

    strncpy(valp->val.bin.datap, str, n);
    ((char*) valp->val.bin.datap)[n] = '\0';
    return RDB_OK;
}

/**
 * Appends the string <var>str</var> to *<var>objp</var>.

*<var>objp</var> must be of type string.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_append_string(RDB_object *objp, const char *str, RDB_exec_context *ecp)
{
    int olen = strlen((char *) objp->val.bin.datap);
    int len = olen + strlen(str);

    if (len + 1 > objp->val.bin.len) {
        if (set_str_obj_len(objp,
                objp->val.bin.len + STR_BUF_INC > len + 1 ?
                objp->val.bin.len + STR_BUF_INC : len + 1,
                ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    strcpy(((char *)objp->val.bin.datap) + olen, str);
    return RDB_OK;
}

/**
 * Appends the 1-byte character <var>ch</var> to *<var>objp</var>.

*<var>objp</var> must be of type string.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_append_char(RDB_object *objp, char ch, RDB_exec_context *ecp)
{
    int olen = strlen((char *) objp->val.bin.datap);

    if (olen + 2 > objp->val.bin.len) {
        if (set_str_obj_len(objp,
                objp->val.bin.len + STR_BUF_INC > olen + 2 ?
                objp->val.bin.len + STR_BUF_INC : olen + 2,
                ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    ((char *)objp->val.bin.datap)[olen] = ch;
    ((char *)objp->val.bin.datap)[olen + 1] = '\0';
    return RDB_OK;
}

/**
 * RDB_obj_bool returns the value of the RDB_object pointed to by
<var>valp</var> as a RDB_bool. The RDB_object must be of type
BOOLEAN.

@returns

The value of the RDB_object.
 */
RDB_bool
RDB_obj_bool(const RDB_object *valp)
{
    return valp->val.bool_val;
}

/**
 * RDB_obj_int returns the value of the RDB_object pointed to by
<var>valp</var> as a RDB_int. The RDB_object must be of type
INTEGER.

@returns

The value of the RDB_object.
 */
RDB_int
RDB_obj_int(const RDB_object *valp)
{
    return valp->val.int_val;
}

/**
 * RDB_obj_float returns the value of the RDB_object pointed to by
<var>valp</var> as a RDB_float. The RDB_object must be of type
FLOAT.

@returns

The value of the RDB_object.
 */
RDB_float
RDB_obj_float(const RDB_object *valp)
{
    return valp->val.float_val;
}

/**
 * RDB_obj_string returns a pointer to the value of the RDB_object pointed to by
<var>valp</var> as a char *. The RDB_object must be of type string.

@returns

The string value of the RDB_object.
 */
char *
RDB_obj_string(const RDB_object *valp)
{
    return valp->val.bin.datap;
}

/**
 * RDB_binary_set copies <var>len</var> bytes from srcp to
the position <var>pos</var> in the RDB_object pointed to by <var>valp</var>.
<var>valp</var> must point either to a new initialized RDB_object
or to a RDB_object of type BINARY.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_binary_set(RDB_object *valp, size_t pos, const void *srcp, size_t len,
        RDB_exec_context *ecp)
{
    if (valp->kind != RDB_OB_INITIAL && valp->typ != &RDB_BINARY) {
        RDB_raise_invalid_argument("cannot turn RDB_object into binary", ecp);
        return RDB_ERROR;
    }

    /* If the value is newly initialized, allocate memory */
    if (valp->kind == RDB_OB_INITIAL) {
        valp->val.bin.len = pos + len;
        if (valp->val.bin.len > 0) {
            valp->val.bin.datap = RDB_alloc(valp->val.bin.len, ecp);
            if (valp->val.bin.datap == NULL) {
                return RDB_ERROR;
            }
        }
        valp->typ = &RDB_BINARY;
        valp->kind = RDB_OB_BIN;
    }

    /* If the memory block is to small, reallocate */
    if (valp->val.bin.len < pos + len) {
        void *datap;

        if (valp->val.bin.len > 0)
            datap = RDB_realloc(valp->val.bin.datap, pos + len, ecp);
        else
            datap = RDB_alloc(pos + len, ecp);
        if (datap == NULL) {
            return RDB_ERROR;
        }
        valp->val.bin.datap = datap;
        valp->val.bin.len = pos + len;
    }

    /* Copy data */
    if (len > 0)
        memcpy(((RDB_byte *)valp->val.bin.datap) + pos, srcp, len);
    return RDB_OK;
}

/**
 * RDB_binary_get obtains a pointer to <var>len</var> bytes starting at position
<var>pos</var> of the RDB_object pointed to by <var>objp</var>
and stores this pointer at the location pointed to by <var>pp</var>.
If the sum of <var>pos</var> and <var>len</var> exceeds the length of the
object, the length of the byte block will be lower than requested.

If <var>alenp</var> is not NULL, the actual length of the byte block is stored
at the location pointed to by <var>alenp</var>.

<var>valp</var> must point to a RDB_object of type BINARY.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_binary_get(const RDB_object *objp, size_t pos, size_t len,
        RDB_exec_context *ecp, void **pp, size_t *alenp)
{
    if (pos > objp->val.bin.len) {
        RDB_raise_invalid_argument("index out of range", ecp);
        return RDB_ERROR;
    }
    if (alenp != NULL) {
        if (pos + len > objp->val.bin.len)
            *alenp = objp->val.bin.len - pos;
        else
            *alenp = len;
    }
    *pp = (RDB_byte *)objp->val.bin.datap + pos;
    return RDB_OK;
}

/**
 * RDB_binary_length returns the number of bytes stored in the
RDB_object pointed to by <var>objp</var>. The RDB_object
must be of type BINARY.

@returns

The length of the RDB_object.
 */
size_t
RDB_binary_length(const RDB_object *objp)
{
    return objp->val.bin.len;
}

/**
 * RDB_binary_resize sets the returns the number of bytes stored in the
RDB_object pointed to by <var>objp</var> to <var>len</var>.
If <var>len</var> is greater than the original length, the value of the
bytes added is undefined.

@returns

The length of the RDB_object.
 */
int
RDB_binary_resize(RDB_object *objp, size_t len, RDB_exec_context *ecp)
{
    void *datap;

    if (len > 0) {
        datap = RDB_realloc(objp->val.bin.datap, len, ecp);
        if (datap == NULL)
            return RDB_ERROR;
        objp->val.bin.datap = datap;
    } else {
        RDB_free(objp->val.bin.datap);
    }
    objp->val.bin.len = len;
    return RDB_OK;
}

/**
 * RDB_obj_type returns a pointer to the type of *<var>objp</var>.

@returns

A pointer to the type of the RDB_object.
 */
RDB_type *
RDB_obj_type(const RDB_object *objp)
{
    return objp->typ;
}

/**
 * Set the type information for *<var>objp</var> to *<var>typ</var>.
 * This can be used only for tuples and arrays, which, unlike scalars
 * and tables, do not to carry explicit type information by default.
 * The caller must manage the type; it is not automatically destroyed
 * when *<var>objp</var> is destroyed (except if *<var>objp</var> is embedded in an
 * expression, in which case the expression takes responsibility for destroying the type).
 *
 * If *<var>typ</var> is a dummy type and the existing type is not a dummy
 * type, the existing type becomes the implemented type.
 */
void
RDB_obj_set_typeinfo(RDB_object *objp, RDB_type *typ)
{
    if (RDB_type_is_dummy(typ) && objp->typ != NULL
            && RDB_type_is_scalar(objp->typ) && !RDB_type_is_dummy(objp->typ)) {
        objp->impl_typ = objp->typ;
    }
    objp->typ = typ;
}

/*@}*/

RDB_type *
RDB_obj_impl_type(const RDB_object *objp)
{
    if (objp->typ == NULL)
        return NULL;
    return RDB_type_is_dummy(objp->typ) ? objp->impl_typ : objp->typ;
}

/* Works only for scalar types */
void
RDB_set_obj_type(RDB_object *objp, RDB_type *typ)
{
    objp->typ = typ;
    objp->kind = RDB_val_kind(typ);
    if (objp->kind == RDB_OB_BIN)
        objp->val.bin.datap = NULL;
}

static void
dfloat_to_str(RDB_float f, char *bufp)
{
    size_t len;

    len = sprintf(bufp, "%.15g", (double) f);

    /* Add decimal point */
    if (strchr(bufp, '.') == NULL) {
        bufp[len] = '.';
        bufp[len + 1] = '0';
        bufp[len + 2] = '\0';
    }
}

int
RDB_obj_to_string(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp)
{
    char buf[64];

    if (srcp->typ == &RDB_INTEGER) {
        sprintf(buf, "%d", RDB_obj_int(srcp));
        if (RDB_string_to_obj(dstp, buf, ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_BOOLEAN) {
        if (RDB_string_to_obj(dstp, RDB_obj_bool(srcp) ? "TRUE" : "FALSE",
                ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_FLOAT) {
        dfloat_to_str(RDB_obj_float(srcp), buf);
        if (RDB_string_to_obj(dstp, buf, ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_STRING) {
        if (RDB_string_to_obj(dstp, RDB_obj_string(srcp), ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (srcp->typ == &RDB_BINARY) {
        if (RDB_string_n_to_obj(dstp, srcp->val.bin.datap, srcp->val.bin.len,
                ecp) != RDB_OK)
            return RDB_ERROR;
    } else {
        RDB_raise_invalid_argument("type cannot be converted to string", ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

void
RDB_datetime_to_tm(struct tm *tm, const RDB_object *dt)
{
    tm->tm_year = dt->val.time.year - 1900;
    tm->tm_mon = dt->val.time.month - 1;
    tm->tm_mday = dt->val.time.day;
    tm->tm_hour = dt->val.time.hour;
    tm->tm_min = dt->val.time.minute;
    tm->tm_sec = dt->val.time.second;
    tm->tm_isdst = -1;
}

enum RDB_obj_kind
RDB_val_kind(const RDB_type *typ)
{
    switch (typ->kind) {
    case RDB_TP_SCALAR:
        if (typ == &RDB_BOOLEAN)
            return RDB_OB_BOOL;
        if (typ == &RDB_INTEGER)
            return RDB_OB_INT;
        if (typ == &RDB_FLOAT)
            return RDB_OB_FLOAT;
        if (typ == &RDB_DATETIME)
            return RDB_OB_TIME;
        if (typ->def.scalar.arep != NULL)
            return RDB_val_kind(typ->def.scalar.arep);
        return RDB_OB_BIN;
    case RDB_TP_TUPLE:
        return RDB_OB_TUPLE;
    case RDB_TP_RELATION:
        return RDB_OB_TABLE;
    case RDB_TP_ARRAY:
        return RDB_OB_ARRAY;
    }
    /* Should never be reached */
    abort();
}

RDB_bool
RDB_irep_is_string(const RDB_type *typ)
{
    if (typ == &RDB_STRING)
        return RDB_TRUE;
    return (RDB_bool) (typ->kind == RDB_TP_SCALAR && typ->def.scalar.arep == &RDB_STRING);
}
