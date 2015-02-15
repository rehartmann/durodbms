/*
 * Tuple functions.
 *
 * Copyright (C) 2003-2009, 2011-2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "tuple.h"
#include "objinternal.h"
#include <gen/hashtabit.h>
#include <gen/strfns.h>

#include <string.h>
#include <stdlib.h>

enum {
    RDB_TUPLE_CAPACITY = 31
};

/*
 * A tuple is implemented using a hash table, taking advantage of
 * the fact that removing attributes is not supported.
 */

static unsigned
hash_entry(const void *ep, void *arg)
{
    return RDB_hash_str(((tuple_entry *) ep)->key);
}

static RDB_bool
entry_equals(const void *e1p, const void *e2p, void *arg)
{
     return (RDB_bool)
             strcmp(((tuple_entry *) e1p)->key, ((tuple_entry *) e2p)->key) == 0;
}

static void
init_tuple(RDB_object *tp)
{
    RDB_init_hashtable(&tp->val.tpl_tab, RDB_TUPLE_CAPACITY,
            &hash_entry, &entry_equals);
    tp->kind = RDB_OB_TUPLE;
}

static int
provide_entry(RDB_object *tplp, const char *attrname, RDB_exec_context *ecp,
        RDB_object **valpp)
{
    int ret;
    tuple_entry sentry;
    tuple_entry *entryp;

    if (tplp->kind == RDB_OB_INITIAL)
        init_tuple(tplp);

    sentry.key = (char *) attrname;

    /* Check if there is already a value for the key */
    entryp = RDB_hashtable_get(&tplp->val.tpl_tab, &sentry, NULL);
    if (entryp != NULL) {
        /* Return pointer to value */
    } else {
        /* Insert new entry */
        entryp = RDB_alloc(sizeof(tuple_entry), ecp);
        if (entryp == NULL) {
            return RDB_ERROR;
        }
        entryp->key = RDB_dup_str(attrname);
        if (entryp->key == NULL) {
            RDB_free(entryp);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        ret = RDB_hashtable_put(&tplp->val.tpl_tab, entryp, NULL);
        if (ret != RDB_OK) {
            RDB_free(entryp->key);
            RDB_free(entryp);
            return ret;
        }
        RDB_init_obj(&entryp->obj);
    }
    *valpp = &entryp->obj;
    return RDB_OK;
}

/** @defgroup tuple Tuple functions 
 * @{
 */

/**
 * RDB_tuple_set sets the attribute <var>name</var> of the tuple
variable specified by <var>tplp</var> to the value specified by
<var>objp</var>.

If <var>objp</var> is NULL, the attribute value is set to an object that
has been initialized using RDB_init_obj().

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_tuple_set(RDB_object *tplp, const char *attrname, const RDB_object *objp,
        RDB_exec_context *ecp)
{
    RDB_object *dstvalp;
    if (provide_entry(tplp, attrname, ecp, &dstvalp) != RDB_OK)
        return RDB_ERROR;

    RDB_destroy_obj(dstvalp, ecp);
    RDB_init_obj(dstvalp);
    if (objp == NULL)
        return RDB_OK;
    return RDB_copy_obj(dstvalp, objp, ecp);
}

/**
 * RDB_tuple_set_bool sets the attribute <var>name</var> of the tuple
variable specified by <var>tplp</var> to the boolean value specified by
<var>val</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_tuple_set_bool(RDB_object *tplp, const char *attrname, RDB_bool val,
        RDB_exec_context *ecp)
{
    RDB_object *dstvalp;
    if (provide_entry(tplp, attrname, ecp, &dstvalp) != RDB_OK)
        return RDB_ERROR;

    RDB_bool_to_obj(dstvalp, val);
    return RDB_OK;
} 

/**
 * RDB_tuple_set_int sets the attribute <var>name</var> of the tuple
variable specified by <var>tplp</var> to the integer value specified by
<var>val</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_tuple_set_int(RDB_object *tplp, const char *attrname, RDB_int val,
        RDB_exec_context *ecp)
{
    RDB_object *dstvalp;
    if (provide_entry(tplp, attrname, ecp, &dstvalp) != RDB_OK)
        return RDB_ERROR;

    RDB_int_to_obj(dstvalp, val);
    return RDB_OK;
}

/**
 * Set the attribute <var>name</var> of the tuple
variable specified by <var>tplp</var> to the value specified by
<var>val</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_tuple_set_float(RDB_object *tplp, const char *attrname, RDB_float val,
        RDB_exec_context *ecp)
{
    RDB_object *dstvalp;
    if (provide_entry(tplp, attrname, ecp, &dstvalp) != RDB_OK)
        return RDB_ERROR;

    RDB_float_to_obj(dstvalp, val);
    return RDB_OK;
}

/**
 * Set the attribute <var>name</var> of the tuple
variable specified by <var>tplp</var> to the value specified by
<var>str</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_tuple_set_string(RDB_object *tplp, const char *attrname, const char *str,
        RDB_exec_context *ecp)
{
    RDB_object *dstvalp;
    if (provide_entry(tplp, attrname, ecp, &dstvalp) != RDB_OK)
        return RDB_ERROR;

    return RDB_string_to_obj(dstvalp, str, ecp);
}

/**
 * RDB_tuple_get returns a pointer to a RDB_object structure
which contains the value for attribute <var>name</var>.
The value is not copied.

@returns

A pointer to the value of the attribute, or NULL if no
attribute with that name exists.
 */
RDB_object *
RDB_tuple_get(const RDB_object *tplp, const char *attrname)
{
    tuple_entry sentry;
    tuple_entry *entryp;

    if (tplp->kind == RDB_OB_INITIAL)
        return NULL;

    sentry.key = (char *) attrname;
    entryp = RDB_hashtable_get(&tplp->val.tpl_tab, &sentry, NULL);
    if (entryp == NULL)
        return NULL;
    return &entryp->obj;
}

/**
 * RDB_tuple_get_bool returns the value of attribute <var>name</var>
as a RDB_bool. The attribute must exist and it must be of
type boolean.

@returns

The attribute value.
 */
RDB_bool
RDB_tuple_get_bool(const RDB_object *tplp, const char *attrname)
{
    return RDB_tuple_get(tplp, attrname)->val.bool_val;
}

/**
 * RDB_tuple_get_int returns the value of attribute <var>name</var>
as a RDB_int. The attribute must exist and it must be of
type integer.

@returns

The attribute value.
 */
RDB_int
RDB_tuple_get_int(const RDB_object *tplp, const char *attrname)
{
    return RDB_tuple_get(tplp, attrname)->val.int_val;
}

/**
 * Return the value of attribute <var>name</var>
as a RDB_float. The attribute must exist and it must be of
type float.

@returns

The attribute value.
 */
RDB_float
RDB_tuple_get_float(const RDB_object *tplp, const char *attrname)
{
    return RDB_tuple_get(tplp, attrname)->val.float_val;
}

/**
 * RDB_tuple_get_string returns a pointer to the value of attribute
<var>name</var>. The attribute must exist and it must be of
type string.

@returns

A pointer to the attribute value.
 */
char *
RDB_tuple_get_string(const RDB_object *tplp, const char *attrname)
{
    return RDB_tuple_get(tplp, attrname)->val.bin.datap;
}

/**
 * RDB_tuple_size returns the number of attributes of the tuple
specified by <var>tplp</var>.

@returns

The number of attributes.
 */
RDB_int
RDB_tuple_size(const RDB_object *tplp)
{
    if (tplp->kind == RDB_OB_INITIAL)
        return (RDB_int) 0;
    return (RDB_int) RDB_hashtable_size(&tplp->val.tpl_tab);
}

/**
 * RDB_tuple_attr_names fills <var>namev</var> with pointers to
the attribute names of the tuple specified by <var>tplp</var>.

<var>namev</var> must be large enough for all attribute names.
The pointers must not be modified by the caller and will become invalid
when the tuple is destroyed.
 */
void
RDB_tuple_attr_names(const RDB_object *tplp, char **namev)
{
    int i = 0;
    tuple_entry *entryp;
    RDB_hashtable_iter hiter;

    RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tplp->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        namev[i++] = entryp->key;
    }
    RDB_destroy_hashtable_iter(&hiter);
}

/**
 * RDB_project_tuple creates a tuple which contains only the attributes
specified by <var>attrc</var> and <var>attrv</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>name_error
<dd>One of the attributes specified by <var>attrv</var> is not an attribute
of the original tuple.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_project_tuple(const RDB_object *tplp, int attrc, const char *attrv[],
                 RDB_exec_context *ecp, RDB_object *restplp)
{
    RDB_object *attrp;
    int i;

    if (tplp->kind != RDB_OB_TUPLE) {
        RDB_raise_invalid_argument("not a tuple", ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(restplp, ecp);
    RDB_init_obj(restplp);

    for (i = 0; i < attrc; i++) {
        attrp = RDB_tuple_get(tplp, attrv[i]);
        if (attrp == NULL) {
            RDB_raise_name(attrv[i], ecp);
            return RDB_ERROR;
        }

        RDB_tuple_set(restplp, attrv[i], attrp, ecp);
    }

    return RDB_OK;
}

int
RDB_remove_tuple(const RDB_object *tplp, int attrc, const char *attrv[],
                 RDB_exec_context *ecp, RDB_object *restplp)
{
    RDB_hashtable_iter hiter;
    tuple_entry *entryp;
    int i;

    if (tplp->kind != RDB_OB_TUPLE) {
        RDB_raise_invalid_argument("not a tuple", ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(restplp, ecp);
    RDB_init_obj(restplp);

    RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tplp->val.tpl_tab);
    for (;;) {
        /* Get next attribute */
        entryp = (tuple_entry *) RDB_hashtable_next(&hiter);
        if (entryp == NULL)
            break;

        /* Check if attribute is in attribute list */
        for (i = 0; i < attrc && strcmp(entryp->key, attrv[i]) != 0; i++);
        if (i >= attrc) {
            /* Not found, so copy attribute */
            RDB_tuple_set(restplp, entryp->key, &entryp->obj, ecp);
        }
    }
    RDB_destroy_hashtable_iter(&hiter);

    return RDB_OK;
}

/**
 * RDB_rename_tuple creates copies the tuple specified by <var>tplp</var>
to the tuple specified by <var>restplp</var>, renaming the attributes
specified by <var>renc</var> and <var>renv</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

The call may fail for a @ref system-errors "system error".
 */
int
RDB_rename_tuple(const RDB_object *tplp, int renc, const RDB_renaming renv[],
                 RDB_exec_context *ecp, RDB_object *restup)
{
    RDB_hashtable_iter it;
    tuple_entry *entryp;

    if (tplp->kind != RDB_OB_TUPLE) {
        RDB_raise_invalid_argument("not a tuple", ecp);
        return RDB_ERROR;
    }

    /* Copy attributes to tplp */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tplp->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        int ret;
        int ai = RDB_find_rename_from(renc, renv, entryp->key);

        if (ai >= 0) {
            ret = RDB_tuple_set(restup, renv[ai].to, &entryp->obj, ecp);
        } else {
            ret = RDB_tuple_set(restup, entryp->key, &entryp->obj, ecp);
        }

        if (ret != RDB_OK) {
            RDB_destroy_hashtable_iter(&it);
            return ret;
        }
    }

    RDB_destroy_hashtable_iter(&it);

    return RDB_OK;
}

/**
 * Check if *objp represents a tuple.
 * Calling it with a newly initialized object will return RDB_TRUE.
 */
RDB_bool
RDB_is_tuple(const RDB_object *objp)
{
    if (objp->typ != NULL) {
        return RDB_type_is_tuple(objp->typ);
    }
    return (RDB_bool) (objp->kind == RDB_OB_TUPLE
                       || objp->kind == RDB_OB_INITIAL);
}

/*@}*/

/* Copy all attributes from one tuple to another. */
int
RDB_copy_tuple(RDB_object *dstp, const RDB_object *srcp, RDB_exec_context *ecp)
{
    RDB_hashtable_iter it;
    tuple_entry *entryp;

    if (srcp->kind == RDB_OB_INITIAL)
        return RDB_OK;

    if (srcp->kind != RDB_OB_TUPLE) {
        RDB_raise_invalid_argument("not a tuple", ecp);
        return RDB_ERROR;
    }

    /* Copy attributes to tup */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&srcp->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        int ret = RDB_tuple_set(dstp, entryp->key, &entryp->obj, ecp);

        if (ret != RDB_OK) {
            RDB_destroy_hashtable_iter(&it);
            return ret;
        }
    }

    RDB_destroy_hashtable_iter(&it);

    return RDB_OK;
}
