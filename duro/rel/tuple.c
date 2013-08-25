/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "internal.h"
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
type BOOLEAN.

@returns

The attribute value.
 */
RDB_bool
RDB_tuple_get_bool(const RDB_object *tplp, const char *attrname)
{
    return ((RDB_object *) RDB_tuple_get(tplp, attrname))->val.bool_val;
}

/**
 * RDB_tuple_get_int returns the value of attribute <var>name</var>
as a RDB_int. The attribute must exist and it must be of
type INTEGER.

@returns

The attribute value.
 */
RDB_int
RDB_tuple_get_int(const RDB_object *tplp, const char *attrname)
{
    return ((RDB_object *) RDB_tuple_get(tplp, attrname))->val.int_val;
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
    return ((RDB_object *) RDB_tuple_get(tplp, attrname))->val.float_val;
}

/**
 * RDB_tuple_get_string returns a pointer to the value of attribute
<var>name</var>. The attribute must exist and it must be of
type STRING.

@returns

A pointer to the attribute value.
 */
char *
RDB_tuple_get_string(const RDB_object *tplp, const char *attrname)
{
    return ((RDB_object *) RDB_tuple_get(tplp, attrname))->val.bin.datap;
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
RDB_project_tuple(const RDB_object *tplp, int attrc, char *attrv[],
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
RDB_remove_tuple(const RDB_object *tplp, int attrc, char *attrv[],
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

int
RDB_add_tuple(RDB_object *tpl1p, const RDB_object *tpl2p,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_hashtable_iter hiter;

    RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tpl2p->val.tpl_tab);
    for (;;) {
        /* Get next attribute */
        RDB_object *dstattrp;
        tuple_entry *entryp = (tuple_entry *) RDB_hashtable_next(&hiter);
        if (entryp == NULL)
            break;

        /* Get corresponding attribute from tuple #1 */
        dstattrp = RDB_tuple_get(tpl1p, entryp->key);
        if (dstattrp != NULL) {
             RDB_bool b;
             RDB_type *typ = RDB_obj_type(dstattrp);
             
             /* Check attribute types for equality */
             if (typ != NULL && !RDB_type_equals(typ,
                     RDB_obj_type(&entryp->obj))) {
                 RDB_destroy_hashtable_iter(&hiter);
                 RDB_raise_type_mismatch("JOIN attribute types must be equal",
                         ecp);
                 return RDB_ERROR;
             }
             
             /* Check attribute values for equality */
             ret = RDB_obj_equals(dstattrp, &entryp->obj, ecp, txp, &b);
             if (ret != RDB_OK) {
                 RDB_destroy_hashtable_iter(&hiter);
                 return ret;
             }
             if (!b) {
                 RDB_destroy_hashtable_iter(&hiter);
                 RDB_raise_invalid_argument("tuples do not match", ecp);
                 return RDB_ERROR;
             }
        } else {
             ret = RDB_tuple_set(tpl1p, entryp->key, &entryp->obj, ecp);
             if (ret != RDB_OK)
             {
                 RDB_destroy_hashtable_iter(&hiter);
                 return RDB_ERROR;
             }
        }
    }
    RDB_destroy_hashtable_iter(&hiter);
    return RDB_OK;
}

/**
 * RDB_union_tuples creates a tuple which contains the attributes
of the two tuples specified by <var>tpl1p</var> and <var>tpl2p</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>type_mismatch_error
<dd>The two tuples have an attribute with the same name, but with
different types.
<dt>invalid_argument_error
<dd>The two tuples have an attribute with the same name, but with
different values.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_union_tuples(const RDB_object *tpl1p, const RDB_object *tpl2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *restplp)
{
    int ret = RDB_copy_obj(restplp, tpl1p, ecp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    return RDB_add_tuple(restplp, tpl2p, ecp, txp);
}

/**
 * RDB_extend_tuple extends the tuple specified by <var>tplp</var>
by the attributes specified by <var>attrc</var> and <var>attrv</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>name_error
<dd>One of the expressions specified in <var>updv</var> refers to an attribute
which does not exist in the tuple.
<dt>invalid_argument_error
<dd>One of the additional attributes already exists in the original table.
<dt>operator_not_found_error
<dd>One of the expressions specified in <var>updv</var> refers to an
operator which does not exist.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_extend_tuple(RDB_object *tplp, int attrc, const RDB_virtual_attr attrv[],
                RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object obj;

    for (i = 0; i < attrc; i++) {
        RDB_init_obj(&obj);
        if (RDB_evaluate(attrv[i].exp, &RDB_tpl_get, tplp, NULL, ecp, txp, &obj)
                != RDB_OK) {
            RDB_destroy_obj(&obj, ecp);
            return RDB_ERROR;
        }
        ret = RDB_tuple_set(tplp, attrv[i].name, &obj, ecp);
        RDB_destroy_obj(&obj, ecp);
        if (ret != RDB_OK)
            return RDB_ERROR;
    }
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

static RDB_expression *
find_rename_to(const RDB_expression *exp, const char *name)
{
    RDB_expression *argp = exp->def.op.args.firstp->nextp;

    while (argp != NULL) {
        if (strcmp(RDB_obj_string(&argp->nextp->def.obj), name) == 0)
            return argp;
        argp = argp->nextp->nextp;
    }
    /* not found */
    return NULL;
}

/**
 * RDB_wrap_tuple performs a tuple WRAP operator on the tuple pointed to by
<var>tplp</var> and stores the result in the variable pointed to by
<var>restplp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>name_error
<dd>One or more of the attributes specified by wrapv[i].attrv does not
exist.
</dl>
*/
int
RDB_wrap_tuple(const RDB_object *tplp, int wrapc, const RDB_wrapping wrapv[],
               RDB_exec_context *ecp, RDB_object *restplp)
{
    int i, j;
    int ret;
    RDB_object tpl;
    RDB_hashtable_iter it;
    tuple_entry *entryp;

    /* Wrap attributes */
    for (i = 0; i < wrapc; i++) {
        RDB_init_obj(&tpl);
        for (j = 0; j < wrapv[i].attrc; j++) {
            RDB_object *attrp = RDB_tuple_get(tplp, wrapv[i].attrv[j]);

            if (attrp == NULL) {
                RDB_destroy_obj(&tpl, ecp);
                RDB_raise_name(wrapv[i].attrv[j], ecp);
                return RDB_ERROR;
            }

            ret = RDB_tuple_set(&tpl, wrapv[i].attrv[j], attrp, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return ret;
            }
        }
        RDB_tuple_set(restplp, wrapv[i].attrname, &tpl, ecp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        RDB_destroy_obj(&tpl, ecp);
    }

    /* Copy attributes which have not been wrapped */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tplp->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        int i;

        for (i = 0; i < wrapc
                && RDB_find_str(wrapv[i].attrc, wrapv[i].attrv, entryp->key) == -1;
                i++);
        if (i == wrapc) {
            /* Attribute not found, copy */
            ret = RDB_tuple_set(restplp, entryp->key, &entryp->obj, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_hashtable_iter(&it);
                return RDB_ERROR;
            }
        }
    }
    RDB_destroy_hashtable_iter(&it);

    return RDB_OK;
}

/**
 * RDB_unwrap_tuple performs a tuple UNWRAP operator on the tuple pointed to by
<var>tplp</var> and stores the result in the variable pointed to by
<var>restplp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>name_error
<dd>An attribute specified by attrv does not exist.
<dt>invalid_argument_error
<dd>An attribute specified by attrv is not tuple-typed.
</dl>
 */
int
RDB_unwrap_tuple(const RDB_object *tplp, int attrc, char *attrv[],
        RDB_exec_context *ecp, RDB_object *restplp)
{
    int i;
    int ret;
    RDB_hashtable_iter it;
    tuple_entry *entryp;
    
    for (i = 0; i < attrc; i++) {
        RDB_object *wtplp = RDB_tuple_get(tplp, attrv[i]);

        if (wtplp == NULL) {
            RDB_raise_name(attrv[i], ecp);
            return RDB_ERROR;
        }
        if (wtplp->kind != RDB_OB_TUPLE) {
            RDB_raise_invalid_argument("attribute is not tuple", ecp);
            return RDB_ERROR;
        }

        ret = RDB_copy_tuple(restplp, wtplp, ecp);
        if (ret != RDB_OK)
            return ret;
    }
    
    /* Copy remaining attributes */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tplp->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        /* Copy attribute if it does not appear in attrv */
        if (RDB_find_str(attrc, attrv, entryp->key) == -1) {
            ret = RDB_tuple_set(restplp, entryp->key, &entryp->obj, ecp);
            if (ret != RDB_OK) {
                RDB_destroy_hashtable_iter(&it);
                return ret;
            }
        }
    }
    RDB_destroy_hashtable_iter(&it);

    return RDB_OK;
}

/*@}*/

int
RDB_invrename_tuple(const RDB_object *tup, const RDB_expression *exp,
                 RDB_exec_context *ecp, RDB_object *restup)
{
    RDB_hashtable_iter it;
    tuple_entry *entryp;

    if (tup->kind != RDB_OB_TUPLE) {
        RDB_raise_invalid_argument("not a tuple", ecp);
        return RDB_ERROR;
    }

    /* Copy attributes to tup */
    RDB_init_hashtable_iter(&it, (RDB_hashtable *)&tup->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&it)) != NULL) {
        int ret;
        RDB_expression *argp = find_rename_to(exp, entryp->key);

        if (argp != NULL) {
            ret = RDB_tuple_set(restup, RDB_obj_string(&argp->def.obj),
                    &entryp->obj, ecp);
        } else {
            ret = RDB_tuple_set(restup, entryp->key, &entryp->obj, ecp);
        }
        if (ret != RDB_OK) {
            RDB_destroy_hashtable_iter(&it);
            return RDB_ERROR;
        }
    }

    RDB_destroy_hashtable_iter(&it);

    return RDB_OK;
}

/*
 * Invert wrap operation on tuple
 */
int
RDB_invwrap_tuple(const RDB_object *tplp, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_object *restplp)
{
    int i;
    int ret;
    RDB_expression *argp;
    int wrapc = (RDB_expr_list_length(&exp->def.op.args) - 1) / 2;
    char **attrv = RDB_alloc(sizeof(char *) * wrapc, ecp);

    if (attrv == NULL) {
        return RDB_ERROR;
    }

    /*
     * Create unwrapped tuple
     */
    argp = exp->def.op.args.firstp;
    for (i = 0; i < wrapc; i++) {
        argp = argp->nextp->nextp;
        attrv[i] = RDB_obj_string(&argp->def.obj);
    }

    ret = RDB_unwrap_tuple(tplp, wrapc, attrv, ecp, restplp);
    RDB_free(attrv);
    return ret;
}

int
RDB_invunwrap_tuple(const RDB_object *tplp, RDB_expression *exp,
        RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *restplp)
{
    int ret;
    int i, j;
    RDB_expression *argp;
    int attrc = RDB_expr_list_length(&exp->def.op.args) - 1;
    RDB_type *srcreltyp = RDB_expr_type(exp->def.op.args.firstp, NULL, NULL,
            NULL, ecp, txp);
    RDB_type *srctuptyp = srcreltyp->def.basetyp;
    RDB_wrapping *wrapv = RDB_alloc(sizeof(RDB_wrapping) * attrc, ecp);
    if (wrapv == NULL) {
        return RDB_ERROR;
    }

    /*
     * Create wrapped tuple
     */

    argp = exp->def.op.args.firstp->nextp;
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_tuple_type_attr(srctuptyp,
                RDB_obj_string(&argp->def.obj))->typ;

        wrapv[i].attrc = tuptyp->def.tuple.attrc;
        wrapv[i].attrv = RDB_alloc(sizeof(char *) * tuptyp->def.tuple.attrc, ecp);
        if (wrapv[i].attrv == NULL) {
            return RDB_ERROR;
        }
        for (j = 0; j < wrapv[i].attrc; j++)
            wrapv[i].attrv[j] = tuptyp->def.tuple.attrv[j].name;

        wrapv[i].attrname = RDB_obj_string(&argp->def.obj);
        argp = argp->nextp;
    }

    ret = RDB_wrap_tuple(tplp, attrc, wrapv, ecp, restplp);

    for (i = 0; i < attrc; i++)
        RDB_free(wrapv[i].attrv);
    RDB_free(wrapv);
    return ret;
}

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

static int
set_tuple_attr_type(RDB_object *objp, RDB_type *tpltyp, const char *attrname,
        RDB_exec_context *ecp)
{
    RDB_type *attrtyp = RDB_type_attr_type(tpltyp, attrname);
    if (attrtyp == NULL) {
        RDB_raise_invalid_argument("cannot obtain tuple attribute type", ecp);
        return RDB_ERROR;
    }
    RDB_obj_set_typeinfo(objp, attrtyp);
    return RDB_OK;
}

int
RDB_tuple_equals(const RDB_object *tpl1p, const RDB_object *tpl2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    RDB_hashtable_iter hiter;
    tuple_entry *entryp;
    RDB_bool b;

    RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tpl1p->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_object *objp = RDB_tuple_get(tpl2p, entryp->key);
        if (objp == NULL) {
            RDB_destroy_hashtable_iter(&it);
            *resp = RDB_FALSE;
            return RDB_OK;
        }
        if (RDB_obj_type(&entryp->obj) == NULL) {
            if (RDB_obj_type(tpl1p) == NULL) {
                RDB_raise_invalid_argument("cannot obtain tuple attribute type", ecp);
                return RDB_ERROR;
            }
            if (set_tuple_attr_type(&entryp->obj, RDB_obj_type(tpl1p),
                    entryp->key, ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
        if (RDB_obj_type(objp) == NULL) {
            if (RDB_obj_type(tpl2p) == NULL) {
                RDB_raise_invalid_argument("cannot obtain tuple attribute type", ecp);
                return RDB_ERROR;
            }
            if (set_tuple_attr_type(objp, RDB_obj_type(tpl2p),
                    entryp->key, ecp) != RDB_OK) {
                return RDB_ERROR;
            }
        }

        if (RDB_obj_equals(&entryp->obj, objp, ecp, txp, &b) != RDB_OK) {
            RDB_destroy_hashtable_iter(&it);
            return RDB_ERROR;
        }
        if (!b) {
            RDB_destroy_hashtable_iter(&it);
            *resp = RDB_FALSE;
            return RDB_OK;
        }
    }
    RDB_destroy_hashtable_iter(&it);
    *resp = RDB_TRUE;
    return RDB_OK;
}

/*
 * Check if *tpl1p and *tpl2p match.
 * Only *tpl1p must carry type information.
 */
int
RDB_tuple_matches(const RDB_object *tpl1p, const RDB_object *tpl2p,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_hashtable_iter hiter;
    tuple_entry *entryp;
    RDB_bool b;

    /*
     * If one of the tuples is the empty tuple, the tuple matches
     */
    if (tpl1p->kind == RDB_OB_INITIAL || tpl2p->kind == RDB_OB_INITIAL) {
        *resp = RDB_TRUE;
        return RDB_OK;
    }

    RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tpl1p->val.tpl_tab);
    while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
        RDB_object *attrp = RDB_tuple_get(tpl2p, entryp->key);
        if (attrp != NULL) {
            if (RDB_obj_type(&entryp->obj) == NULL) {
                if (RDB_obj_type(tpl1p) == NULL) {
                    RDB_raise_invalid_argument("cannot obtain tuple attribute type", ecp);
                    return RDB_ERROR;
                }
                if (set_tuple_attr_type(&entryp->obj, RDB_obj_type(tpl1p),
                        entryp->key, ecp) != RDB_OK) {
                    return RDB_ERROR;
                }
            }
            if (RDB_obj_type(attrp) == NULL) {
                if (RDB_obj_type(tpl1p) == NULL) {
                    RDB_raise_invalid_argument("cannot obtain tuple attribute type", ecp);
                    return RDB_ERROR;
                }
                if (set_tuple_attr_type(attrp, RDB_obj_type(tpl1p),
                        entryp->key, ecp) != RDB_OK) {
                    return RDB_ERROR;
                }
            }

            ret = RDB_obj_equals(&entryp->obj, attrp, ecp, txp, &b);
            if (ret != RDB_OK) {
                RDB_destroy_hashtable_iter(&it);
                return ret;
            }
            if (!b) {
                RDB_destroy_hashtable_iter(&it);
                *resp = RDB_FALSE;
                return RDB_OK;
            }
        }
    }
    RDB_destroy_hashtable_iter(&it);
    *resp = RDB_TRUE;
    return RDB_OK;
}

/*
 * Generate type from tuple
 */
RDB_type *
RDB_tuple_type(const RDB_object *tplp, RDB_exec_context *ecp)
{
    int i;
    tuple_entry *entryp;
    RDB_hashtable_iter hiter;
    RDB_type *typ = RDB_alloc(sizeof (RDB_type), ecp);
    if (typ == NULL)
        return NULL;

    typ->kind = RDB_TP_TUPLE;
    typ->name = NULL;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->def.tuple.attrc = RDB_tuple_size(tplp);
    if (typ->def.tuple.attrc > 0) {
        typ->def.tuple.attrv = RDB_alloc(sizeof(RDB_attr) * typ->def.tuple.attrc, ecp);
        if (typ->def.tuple.attrv == NULL)
            goto error;

        for (i = 0; i < typ->def.tuple.attrc; i++)
            typ->def.tuple.attrv[i].name = NULL;

        RDB_init_hashtable_iter(&hiter, (RDB_hashtable *) &tplp->val.tpl_tab);
        i = 0;
        while ((entryp = RDB_hashtable_next(&hiter)) != NULL) {
            if (entryp->obj.kind != RDB_OB_TUPLE) {
                typ->def.tuple.attrv[i].typ = RDB_dup_nonscalar_type(
                        RDB_obj_type(&entryp->obj), ecp);
            } else {
                typ->def.tuple.attrv[i].typ = RDB_tuple_type(&entryp->obj,
                        ecp);
            }
            if (typ->def.tuple.attrv[i].typ == NULL)
                goto error;
            typ->def.tuple.attrv[i].name = RDB_dup_str(entryp->key);
            if (typ->def.tuple.attrv[i].name == NULL) {
                RDB_destroy_hashtable_iter(&it);
                goto error;
            }
            typ->def.tuple.attrv[i].defaultp = NULL;
            i++;
        }
        RDB_destroy_hashtable_iter(&it);
    }
    return typ;

error:
    if (typ->def.tuple.attrc > 0 && typ->def.tuple.attrv != NULL) {
        for (i = 0; i < typ->def.tuple.attrc; i++) {
            RDB_free(typ->def.tuple.attrv[i].name);
            if (typ->def.tuple.attrv[i].typ != NULL
                    && typ->def.tuple.attrv[i].typ->kind == RDB_TP_TUPLE)
                RDB_del_nonscalar_type(typ->def.tuple.attrv[i].typ, ecp);
        }
        RDB_free(typ->def.tuple.attrv);
    }
    RDB_free(typ);
    return NULL;
}
