/*
 * Array functions
 *
 * Copyright (C) 2003-2009, 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "array.h"
#include "type.h"

int
RDB_enlarge_array_buf(RDB_object *arrp, RDB_int len, RDB_exec_context *ecp)
{
    int i;
    void *vp = RDB_realloc(arrp->val.arr.elemv, sizeof (RDB_object) * len, ecp);
    if (vp == NULL)
        return RDB_ERROR;

    arrp->val.arr.elemv = vp;
    for (i = arrp->val.arr.capacity; i < len; i++)
        RDB_init_obj(&arrp->val.arr.elemv[i]);
    arrp->val.arr.capacity = len;
    return RDB_OK;
}

/**@addtogroup array
 * @{
 */

/**
 * RDB_array_get returns a pointer to the RDB_object at index <var>idx</var>.
This pointer may become invalid after the next invocation of RDB_array_get().
The pointer will become invalid when the array is destroyed.

@returns
A pointer to the array element, or NULL if an error occurred.

@par Errors:
<dl>
<dt>not_found_error
<dd><var>idx</var> exceeds the array length.
<dt>operator_not_found_error
<dd>The array was created from a table which refers to a non-existing
operator.
</dl>

The call may also fail for a @ref system-errors "system error".
*/
RDB_object *
RDB_array_get(const RDB_object *arrp, RDB_int idx, RDB_exec_context *ecp)
{
    if (idx < 0 || idx >= arrp->val.arr.length) {
        RDB_raise_not_found("out of range", ecp);
        return NULL;
    }
    return &arrp->val.arr.elemv[idx];
}

/**
 * RDB_array_length returns the length of an array.

@returns
The length of the array. A return code lower than zero
indicates an error.

@par Errors:
<dl>
<dt>operator_not_found_error
<dd>The array was created from a table which refers to a non-existing
operator.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
RDB_int
RDB_array_length(const RDB_object *arrp, RDB_exec_context *ecp)
{
    if (arrp->kind == RDB_OB_INITIAL)
        return 0;
    return arrp->val.arr.length;
}

/**
 * RDB_set_array_length sets the length of the array specified by
<var>arrp</var>.

@returns
RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may also fail for a @ref system-errors "system error".

 */
int
RDB_set_array_length(RDB_object *arrp, RDB_int len, RDB_exec_context *ecp)
{
    int i;
    int ret;

    if (arrp->kind == RDB_OB_INITIAL) {
        arrp->kind = RDB_OB_ARRAY;

        if (len > 0) {
            arrp->val.arr.elemv = RDB_alloc(sizeof(RDB_object) * len, ecp);
            if (arrp->val.arr.elemv == NULL) {
                return RDB_ERROR;
            }
            for (i = 0; i < len; i++)
                RDB_init_obj(&arrp->val.arr.elemv[i]);
        } else {
            arrp->val.arr.elemv = NULL;
        }

        arrp->val.arr.length = arrp->val.arr.capacity = len;

        return RDB_OK;
    }

    if (len < arrp->val.arr.length) {
        void *vp;
        /*
         * Shrink array
         */

        /* Set array length so the array is shrinked if an error occurs,
         * preventing the caller from accessing invalid entries */
        arrp->val.arr.length = len;

        for (i = len; i < arrp->val.arr.capacity; i++) {
            ret = RDB_destroy_obj(&arrp->val.arr.elemv[i], ecp);
            if (ret != RDB_OK) {
               arrp->val.arr.capacity = len;
               return ret;
            }
        }
        arrp->val.arr.capacity = len;
        if (len > 0) {
            vp = RDB_realloc(arrp->val.arr.elemv, sizeof (RDB_object) * len, ecp);
            if (vp == NULL)
                return RDB_ERROR;
        } else {
            RDB_free(arrp->val.arr.elemv);
            vp = NULL;
        }
        arrp->val.arr.elemv = vp;
    } else if (len > arrp->val.arr.length) {
        /* Enlarge array */
        if (RDB_enlarge_array_buf(arrp, len, ecp) != RDB_OK)
            return RDB_ERROR;
        arrp->val.arr.length = len;
    }

    return RDB_OK;
}

/**
 * RDB_array_set copies the RDB_object pointed to by tplp
into the RDB_object at index <var>idx</var>.

@returns
RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:
<dl>
<dt>not_found_error
<dd><var>idx</var> exceeds the array length.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_array_set(RDB_object *arrp, RDB_int idx, const RDB_object *objp,
        RDB_exec_context *ecp)
{
    if (idx >= arrp->val.arr.length) {
        RDB_raise_not_found("index out of bounds", ecp);
        return RDB_ERROR;
    }

    return RDB_copy_obj(&arrp->val.arr.elemv[idx], objp, ecp);
}

/**
 * Checks if *objp is an array.
 */
RDB_bool
RDB_is_array(const RDB_object *objp)
{
    if (objp->typ != NULL) {
        return RDB_type_is_array(objp->typ);
    }
    return (RDB_bool) (objp->kind == RDB_OB_ARRAY);
}

/*@}*/

int
RDB_copy_array(RDB_object *dstp, const RDB_object *srcp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_object *objp;
    int len = RDB_array_length((RDB_object *) srcp, ecp);

    if (len == -1) {
        RDB_raise_not_supported("invalid source array", ecp);
        return RDB_ERROR;
    }

    if (RDB_set_array_length(dstp, len, ecp) != RDB_OK)
        return RDB_ERROR;

    for (i = 0; i < len; i++) {
        objp = RDB_array_get((RDB_object *) srcp, (RDB_int) i, ecp);
        if (objp == NULL)
            return RDB_ERROR;
        if (RDB_array_set(dstp, (RDB_int) i, objp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    return RDB_OK;
}

