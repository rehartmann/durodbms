/*
 * $Id$
 *
 * Copyright (C) 2003-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "catalog.h"
#include "stable.h"
#include "qresult.h"
#include "insert.h"
#include "optimize.h"
#include "internal.h"

#include <gen/strfns.h>

#include <string.h>

static RDB_string_vec *
dup_keyv(int keyc, const RDB_string_vec keyv[], RDB_exec_context *ecp)
{
    return RDB_dup_rename_keys(keyc, keyv, NULL, ecp);
}

static RDB_bool
strvec_is_subset(const RDB_string_vec *v1p, const RDB_string_vec *v2p)
{
    int i;

    for (i = 0; i < v1p->strc; i++) {
        if (RDB_find_str(v2p->strc, v2p->strv, v1p->strv[i]) == -1)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/**
 * Creates a stored table, but not the recmap and the indexes
 * and does not insert the table into the catalog.
 * reltyp is consumed on success (must not be freed by caller).
 */
RDB_object *
RDB_new_rtable(const char *name, RDB_bool persistent,
           RDB_type *reltyp,
           int keyc, const RDB_string_vec keyv[],
           int default_attrc, const RDB_attr *default_attrv,
           RDB_bool usr, RDB_exec_context *ecp)
{
    RDB_object *tbp = RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

    if (RDB_init_table_i(tbp, name, persistent, reltyp, keyc, keyv,
            default_attrc, default_attrv, usr, NULL, ecp) != RDB_OK) {
        RDB_free(tbp);
        return NULL;
    }
    return tbp;
}

int
RDB_init_table_i(RDB_object *tbp, const char *name, RDB_bool persistent,
        RDB_type *reltyp, int keyc, const RDB_string_vec keyv[],
        int default_attrc, const RDB_attr *default_attrv,
        RDB_bool usr, RDB_expression *exp, RDB_exec_context *ecp)
{
    int i;
    RDB_string_vec allkey; /* Used if keyv is NULL */
    int attrc;

    if (reltyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("relation type required", ecp);
        return RDB_ERROR;
    }

    attrc = reltyp->def.basetyp->def.tuple.attrc;

    if (keyv != NULL) {
        int j;

        /* At least one key is required */
        if (keyc < 1) {
            RDB_raise_invalid_argument("key is required", ecp);
            return RDB_ERROR;
        }

        /*
         * Check all keys
         */
        for (i = 0; i < keyc; i++) {
            /* Check if all the key attributes appear in the type */
            for (j = 0; j < keyv[i].strc; j++) {
                if (RDB_tuple_type_attr(reltyp->def.basetyp, keyv[i].strv[j])
                        == NULL) {
                    RDB_raise_invalid_argument("invalid key", ecp);
                    return RDB_ERROR;
                }
            }

            /* Check if an attribute appears twice in a key */
            for (j = 0; j < keyv[i].strc - 1; j++) {
                /* Search attribute name in the remaining key */
                if (RDB_find_str(keyv[i].strc - j - 1, keyv[i].strv + j + 1,
                        keyv[i].strv[j]) != -1) {
                    RDB_raise_invalid_argument("invalid key", ecp);
                    return RDB_ERROR;
                }
            }
        }

        /* Check if a key is a subset of another */
        for (i = 0; i < keyc - 1; i++) {
            for (j = i + 1; j < keyc; j++) {
                if (keyv[i].strc <= keyv[j].strc) {
                    if (strvec_is_subset(&keyv[i], &keyv[j])) {
                        RDB_raise_invalid_argument("invalid key", ecp);
                        return RDB_ERROR;
                    }
                } else {
                    if (strvec_is_subset(&keyv[j], &keyv[i])) {
                        RDB_raise_invalid_argument("invalid key", ecp);
                        return RDB_ERROR;
                    }
                }
            }
        }
    }

    tbp->kind = RDB_OB_TABLE;
    tbp->val.tb.is_user = usr;
    tbp->val.tb.is_persistent = persistent;
    tbp->val.tb.keyv = NULL;
    tbp->val.tb.default_tplp = NULL;
    tbp->val.tb.stp = NULL;

    if (name != NULL) {
        tbp->val.tb.name = RDB_dup_str(name);
        if (tbp->val.tb.name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        tbp->val.tb.name = NULL;
    }

    allkey.strv = NULL;
    if (exp != NULL) {
        /* Key is inferred from exp 'on demand' */
        tbp->val.tb.keyv = NULL;
    } else {
        if (keyv == NULL) {
            /* Create key for all-key table */
            allkey.strc = attrc;
            allkey.strv = RDB_alloc(sizeof (char *) * attrc, ecp);
            if (allkey.strv == NULL) {
                goto error;
            }
            for (i = 0; i < attrc; i++)
                allkey.strv[i] = reltyp->def.basetyp->def.tuple.attrv[i].name;
            keyc = 1;
            keyv = &allkey;
        }

        /* Copy candidate keys */
        tbp->val.tb.keyv = dup_keyv(keyc, keyv, ecp);
        if (tbp->val.tb.keyv == NULL) {
            goto error;
        }
        tbp->val.tb.keyc = keyc;
    }
    tbp->val.tb.exp = exp;

    tbp->typ = reltyp;

    if (RDB_set_defvals(tbp, default_attrc, default_attrv, ecp) != RDB_OK)
        goto error;

    RDB_free(allkey.strv);

    return RDB_OK;

error:
    /* Clean up */
    if (tbp != NULL) {
        RDB_free(tbp->val.tb.name);
        if (tbp->val.tb.keyv != NULL)
            RDB_free_keys(tbp->val.tb.keyc, tbp->val.tb.keyv);
    }
    RDB_free(allkey.strv);
    return RDB_ERROR;
}

/** @addtogroup table
 * @{
 */

/**
 * Copy all tuples from source table into the destination table.
 * The destination table must be a real table.
 *
 * @returns the number of tuples copied on success, RDB_ERROR on failure
 */
RDB_int
RDB_move_tuples(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object tpl;
    int ret;
    int count = 0;
    RDB_qresult *qrp = NULL;
    RDB_expression *texp = RDB_optimize(srcp, 0, NULL, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);

    qrp = RDB_expr_qresult(texp, ecp, txp);
    if (qrp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Eliminate duplicates, if necessary */
    if (RDB_duprem(qrp, ecp, txp) != RDB_OK)
        goto cleanup;

    while ((ret = RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        if (!dstp->val.tb.is_persistent)
            ret = RDB_insert_real(dstp, &tpl, ecp, NULL);
        else
            ret = RDB_insert_real(dstp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
        count++;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = count;
    }

cleanup:
    if (qrp != NULL)
        RDB_del_qresult(qrp, ecp, txp);
    RDB_del_expr(texp, ecp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

/**
 * Like RDB_init_table(), but uses a RDB_type argument
 * instead of attribute arguments.

If <var>default_attrc</var> is greater than zero,
<var>default_attrv</var> must point to an array of length <var>default_attrc</var>
where <var>name</var> is the attribute name and <var>defaultp</var>
points to the default value for that attribute.
Entries with a <var>defaultp</var> of NULL are ignored.
Other fields of RDB_attr are ignored, but <var>options</var> should be set to zero
for compatibility with future versions.
 *
 * If it returns with RDB_OK, <var>rtyp</var> is consumed.
 */
int
RDB_init_table_from_type(RDB_object *tbp, const char *name, RDB_type *reltyp,
        int keyc, const RDB_string_vec keyv[],
        int default_attrc, const RDB_attr *default_attrv,
        RDB_exec_context *ecp)
{
    return RDB_init_table_i(tbp, name, RDB_FALSE, reltyp,
            keyc, keyv, default_attrc, default_attrv,
            RDB_TRUE, NULL, ecp);
}

/**
 * Turn *<var>tbp</var> into a transient table.
 * <var>tbp</var> should have been initialized using RDB_init_obj().

For <var>name</var>, <var>attrc</var>, <var>attrv</var>,
<var>keyc</var>, <var>keyv</var>,
and <var>ecp</var>, the same rules apply as for
RDB_create_table().

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>type_mismatch_error
<dd>The type of a default value does not match the type of the corresponding
attribute.
<dt>invalid_argument_error
<dd>One or more of the arguments are incorrect. For example, a key attribute
does not appear in *<var>attrv</var>, etc.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_init_table(RDB_object *tbp, const char *name,
        int attrc, const RDB_attr attrv[],
        int keyc, const RDB_string_vec keyv[],
        RDB_exec_context *ecp)
{
    RDB_type *reltyp = RDB_new_relation_type(attrc, attrv, ecp);
    if (reltyp == NULL)
        return RDB_ERROR;

    if (RDB_init_table_from_type(tbp, name, reltyp, keyc, keyv,
            attrc, attrv, ecp) != RDB_OK) {
        RDB_del_nonscalar_type(reltyp, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;   
}

int
RDB_table_keys(RDB_object *tbp, RDB_exec_context *ecp, RDB_string_vec **keyvp)
{
    RDB_bool freekey;

    if (tbp->kind != RDB_OB_TABLE) {
        RDB_raise_invalid_argument("no table", ecp);
        return RDB_ERROR;
    }

    if (tbp->val.tb.keyv == NULL) {
        int keyc;
        RDB_string_vec *keyv;

        keyc = RDB_infer_keys(tbp->val.tb.exp, NULL, NULL, ecp, NULL,
                &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;
        if (freekey) {
            tbp->val.tb.keyv = keyv;
        } else {
            tbp->val.tb.keyv = dup_keyv(keyc, keyv, ecp);
            if (tbp->val.tb.keyv == NULL) {
                return RDB_ERROR;
            }
        }
        tbp->val.tb.keyc = keyc;
    }

    if (keyvp != NULL)
        *keyvp = tbp->val.tb.keyv;

    return tbp->val.tb.keyc;
}

/**
 * RDB_table_name returns a pointer to the name of a table.

@returns

A pointer to the name of the table, or NULL if the table has no name.
 */
char *
RDB_table_name(const RDB_object *tbp)
{
    return tbp->val.tb.name;
}

/**
RDB_copy_table assigns the table specified
by <var>srcp</var> to the value of the table specified by <var>dstp</var>.
The two tables must have the same heading.

Currently, virtual target tables are not supported.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>type_mismatch_error
<dd>The types of the two tables differ.
<dt>operator_not_found_error
<dd>The definition of the table specified by <var>srcp</var>
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
*/
int
RDB_copy_table(RDB_object *dstp, RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_ma_copy cpy;

    cpy.dstp = dstp;
    cpy.srcp = srcp;

    return RDB_multi_assign(0, NULL, 0, NULL, 0, NULL, 1, &cpy, ecp, txp);
}

/**
 * RDB_extract_tuple extracts a single tuple from a table which contains
only one tuple and stores its value in the variable pointed to by <var>tplp</var>.

If an error occurs, the tuple value of the variable pointed to by <var>tplp</var>
is undefined and an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>not_found_error
<dd>The table is empty.
<dt>invalid_argument_error
<dd>The table contains more than one tuple.
<dt>operator_not_found_error
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_extract_tuple(RDB_object *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tplp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_type *errtyp;
    RDB_expression *texp = RDB_optimize(tbp, 0, NULL, ecp, txp);
    if (texp == NULL)
        return RDB_ERROR;

    qrp = RDB_expr_qresult(texp, ecp, txp);
    if (qrp == NULL) {
        RDB_del_expr(texp, ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    /* Get tuple */
    ret = RDB_next_tuple(qrp, tplp, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if there are more tuples */
    for(;;) {
        RDB_bool is_equal;
    
        ret = RDB_next_tuple(qrp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            errtyp = RDB_obj_type(RDB_get_err(ecp));
            if (errtyp == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                ret = RDB_OK;
            }
            break;
        }

        ret = RDB_tuple_equals(tplp, &tpl, ecp, txp, &is_equal);
        if (ret != RDB_OK)
            goto cleanup;

        if (!is_equal) {
            RDB_raise_invalid_argument("table contains more than one tuple", ecp);
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

cleanup:
    RDB_destroy_obj(&tpl, ecp);

    RDB_del_qresult(qrp, ecp, txp);
    RDB_del_expr(texp, ecp);
    return RDB_get_err(ecp) == NULL ? RDB_OK : RDB_ERROR;
}

/**
 * RDB_table_is_persistent returns if the table *<var>tbp</var>
is persistent.

@returns

RDB_TRUE if *<var>tbp</var> is persistent, RDB_FALSE if it
is transient.
 */
RDB_bool
RDB_table_is_persistent(const RDB_object *tbp)
{
	return tbp->val.tb.is_persistent;
}

/**
 * RDB_table_is_real returns if the table *<var>tbp</var>
is real.

@returns

RDB_TRUE if *<var>tbp</var> is real, RDB_FALSE if it
is virtual.
 */
RDB_bool
RDB_table_is_real(const RDB_object *tbp)
{
    return (RDB_bool) (tbp->val.tb.exp == NULL);
}

/**
 * RDB_subset checks if the table specified by <var>tb1p</var> is a subset
of the table specified by <var>tb2p</var> and stores the result at the
location pointed to by <var>resultp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>type_mismatch_error
<dd>The types of the two tables differ.
<dt>operator_not_found_error
<dd>The definition of one of the tables
refers to a non-existing operator.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_subset(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("argument types must be equal", ecp);
        return RDB_ERROR;
    }

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    qrp = RDB_table_qresult(tb1p, ecp, txp);
    if (qrp == NULL) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    *resultp = RDB_TRUE;
    while ((ret = RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, ecp, txp, resultp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            RDB_del_qresult(qrp, ecp, txp);
            goto error;
        }
        if (!*resultp) {
            break;
        }
    }

    RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
            RDB_del_qresult(qrp, ecp, txp);
            goto error;
        }
        RDB_clear_err(ecp);
    }
    ret = RDB_del_qresult(qrp, ecp, txp);
    if (ret != RDB_OK)
        goto error;
    return RDB_OK;

error:
    return RDB_ERROR;
}

/**
 * Return TRUE if *srctbp depends on *dsttbp, FALSE otherwise.
 */
RDB_bool
RDB_table_refers(const RDB_object *srctbp, const RDB_object *dsttbp)
{
    RDB_expression *exp;

    if (srctbp == dsttbp)
        return RDB_TRUE;

    exp = RDB_vtable_expr(srctbp);
    if (exp == NULL)
        return RDB_FALSE;
    return RDB_expr_refers(exp, dsttbp);
}

/**
 * Returns a pointer to an array of RDB_attr structs
describing the attributes of table *<var>tbp</var> and
stores the number of attributes in *<var>attrcp</var>.

The pointer returned must no longer be used if the table has been destroyed.

@returns

A pointer to an array of RDB_attr structs or NULL if *tbp is not a table.
 */
RDB_attr *
RDB_table_attrs(const RDB_object *tbp, int *attrcp)
{
    RDB_attr *attrv;
    int i;

    if (tbp->kind != RDB_OB_TABLE) {
        return NULL;
    }

    attrv = RDB_type_attrs(RDB_obj_type(tbp), attrcp);
    if (tbp->val.tb.default_tplp != NULL) {
        for (i = 0; i < *attrcp; i++) {
            attrv[i].defaultp = RDB_tuple_get(tbp->val.tb.default_tplp,
                    attrv[i].name);
        }
    } else {
        for (i = 0; i < *attrcp; i++) {
            attrv[i].defaultp = NULL;
        }
    }
    return attrv;
}

/*@}*/

int
RDB_table_equals(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    int cnt;

    /* Check if types of the two tables match */
    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("argument types must be equal", ecp);
        return RDB_ERROR;
    }

    /*
     * Check if both tables have same cardinality
     */
    cnt = RDB_cardinality(tb1p, ecp, txp);
    if (cnt < 0)
        return cnt;

    ret = RDB_cardinality(tb2p, ecp, txp);
    if (ret < 0)
        return ret;
    if (ret != cnt) {
        *resp = RDB_FALSE;
        return RDB_OK;
    }

    /*
     * Check if all tuples from table #1 are in table #2
     * (The implementation is quite inefficient if table #2
     * is a SUMMARIZE PER or GROUP table)
     */
    qrp = RDB_table_qresult(tb1p, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    while ((ret = RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, ecp, txp, resp);
        if (ret != RDB_OK) {
            goto error;
        }
        if (!*resp) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_del_qresult(qrp, ecp, txp);
        }
    }
    RDB_clear_err(ecp);

    *resp = RDB_TRUE;
    RDB_destroy_obj(&tpl, ecp);
    return RDB_del_qresult(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_del_qresult(qrp, ecp, txp);
    return ret;
}

/**
 * If the tuples are sorted by an ordered index when read using
 * a table qresult, return the index, otherwise NULL.
 */
RDB_tbindex *
RDB_expr_sortindex (RDB_expression *exp)
{
    if (exp->kind == RDB_EX_TBP) {
        if (exp->def.tbref.tbp->val.tb.exp != NULL)
            return RDB_expr_sortindex(exp->def.tbref.tbp->val.tb.exp);
        return exp->def.tbref.indexp;
    }
    if (exp->kind != RDB_EX_RO_OP)
        return NULL;
    if (strcmp(exp->def.op.name, "where") == 0) {
        return RDB_expr_sortindex(exp->def.op.args.firstp);
    }
    if (strcmp(exp->def.op.name, "project") == 0) {
        return RDB_expr_sortindex(exp->def.op.args.firstp);
    }
    if (strcmp(exp->def.op.name, "semiminus") == 0
            || strcmp(exp->def.op.name, "minus") == 0
            || strcmp(exp->def.op.name, "semijoin") == 0
            || strcmp(exp->def.op.name, "intersect") == 0
            || strcmp(exp->def.op.name, "join") == 0
            || strcmp(exp->def.op.name, "extend") == 0
            || strcmp(exp->def.op.name, "divide") == 0) {
        return RDB_expr_sortindex(exp->def.op.args.firstp);
    }
    /* !! RENAME, SUMMARIZE, WRAP, UNWRAP, GROUP, UNGROUP */

    return NULL;
}

int
RDB_set_defvals(RDB_object *tbp, int attrc, const RDB_attr attrv[],
        RDB_exec_context *ecp)
{
    RDB_bool defvals = RDB_FALSE;
    RDB_object *tplp;
    int i;

    /* Check if there are actually any default values */
    for (i = 0; i < attrc; i++) {
        if (attrv[i].defaultp != NULL) {
            defvals = RDB_TRUE;
        }
    }

    if (!defvals)
        return RDB_OK;

    tplp = RDB_new_obj(ecp);
    if (tplp == NULL)
        return RDB_ERROR;

    for (i = 0; i < attrc; i++) {
        if (attrv[i].defaultp != NULL) {
            if (RDB_tuple_set(tplp, attrv[i].name,
                    attrv[i].defaultp, ecp) != RDB_OK)
                goto error;
        }
    }
    tbp->val.tb.default_tplp = tplp;
    return RDB_OK;

error:
    RDB_free_obj(tplp, ecp);
    return RDB_ERROR;;
}

/**
 * @defgroup index Index functions
 * \#include <rel/rdb.h>
 * @{
 */

/**
 * RDB_create_table_index creates an index with name <var>name</var>
for the table specified by <var>tbp</var> over the attributes
specified by <var>idxcompc</var> and <var>idxcompv</var>.
The <var>flags</var> argument must be either 0 or RDB_ORDERED.
If <var>flags</var> is 0, a hash index is created.
If <var>flags</var> is RDB_ORDERED, a B-tree index is created.

Functions which read tuples from a table (like
@ref RDB_table_to_array
and @ref RDB_extract_tuple) try to use available
indexes to achieve better performance.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>invalid_argument_error
<dd><var>name</var> is not a valid index name.
<dt>element_exist_error
<dd>An index with name <var>name</var> already exists.
<dt>name_error
<dd>An attribute specified by <var>idxcompv</var> does not appear in the
table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_create_table_index(const char *name, RDB_object *tbp, int idxcompc,
        const RDB_seq_item idxcompv[], int flags, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_tbindex *indexp;

    if (!RDB_legal_name(name)) {
        RDB_raise_invalid_argument("invalid index name", ecp);
        return RDB_ERROR;
    }

    if (tbp->val.tb.is_persistent) {
        /* Insert index into catalog */
        ret = RDB_cat_insert_index(name, idxcompc, idxcompv,
                RDB_FALSE,
                (RDB_bool) ((RDB_ORDERED & flags) != 0), RDB_table_name(tbp),
                ecp, txp);
        if (ret != RDB_OK)
            goto error;
    }

    /*
     * If the stored table has not been created, don't create the physical index -
     * it will be created later from the catalog when the stored table is created
     */
    if (tbp->val.tb.stp != NULL) {
        tbp->val.tb.stp->indexv = RDB_realloc(tbp->val.tb.stp->indexv,
                (tbp->val.tb.stp->indexc + 1) * sizeof (RDB_tbindex), ecp);
        if (tbp->val.tb.stp->indexv == NULL) {
            return RDB_ERROR;
        }

        indexp = &tbp->val.tb.stp->indexv[tbp->val.tb.stp->indexc++];

        indexp->name = RDB_dup_str(name);
        if (indexp->name == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        indexp->attrc = idxcompc;
        indexp->attrv = RDB_alloc(sizeof (RDB_seq_item) * idxcompc, ecp);
        if (indexp->attrv == NULL) {
            RDB_free(indexp->name);
            return RDB_ERROR;
        }

        for (i = 0; i < idxcompc; i++) {
            indexp->attrv[i].attrname = RDB_dup_str(idxcompv[i].attrname);
            if (indexp->attrv[i].attrname == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            indexp->attrv[i].asc = idxcompv[i].asc;
        }
        indexp->unique = RDB_FALSE;
        indexp->ordered = (RDB_bool) (RDB_ORDERED & flags);

        /* Create index */
        if (RDB_create_tbindex(tbp, RDB_db_env(RDB_tx_db(txp)), ecp, txp,
                indexp, flags) != RDB_OK) {
            goto error;
        }
    }

    return RDB_OK;

error:
    if (tbp->val.tb.stp != NULL) {
        /* Remove index entry */
        void *ivp = RDB_realloc(tbp->val.tb.stp->indexv,
                (--tbp->val.tb.stp->indexc) * sizeof (RDB_tbindex), ecp);
        if (ivp != NULL)
            tbp->val.tb.stp->indexv = ivp;
    }
    return RDB_ERROR;
}

/**
 * RDB_drop_table_index drops the index specified by <var>name</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>not_found_error
<dd>An index with name <var>name</var> does not exist.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_drop_table_index(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    int xi;
    char *tbname;
    RDB_object *tbp;
    void *p;

    if (!RDB_legal_name(name)) {
        RDB_raise_not_found("invalid index name", ecp);
        return RDB_ERROR;
    }

    ret = RDB_cat_index_tablename(name, &tbname, ecp, txp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    tbp = RDB_get_table(tbname, ecp, txp);
    RDB_free(tbname);
    if (tbp == NULL)
        return RDB_ERROR;

    /* Delete index from catalog */
    ret = RDB_cat_delete_index(name, ecp, txp);
    if (ret != RDB_OK)
        return ret;

    if (tbp->val.tb.stp != NULL) {
        /*
         * Delete index from the stored table
         */
        for (i = 0; i < tbp->val.tb.stp->indexc
                && strcmp(tbp->val.tb.stp->indexv[i].name, name) != 0;
                i++);
        if (i >= tbp->val.tb.stp->indexc) {
            /* Index not found, so reread indexes */
            for (i = 0; i < tbp->val.tb.stp->indexc; i++)
                RDB_free_tbindex(&tbp->val.tb.stp->indexv[i]);
            RDB_free(tbp->val.tb.stp->indexv);
            ret = RDB_cat_get_indexes(tbp->val.tb.name, txp->dbp->dbrootp, ecp, txp,
                    &tbp->val.tb.stp->indexv);
            if (ret != RDB_OK)
                return RDB_ERROR;

            /* Search again */
            for (i = 0; i < tbp->val.tb.stp->indexc
                    && strcmp(tbp->val.tb.stp->indexv[i].name, name) != 0;
                    i++);
            if (i >= tbp->val.tb.stp->indexc) {
                RDB_raise_internal("invalid index", ecp);
                return RDB_ERROR;
            }
        }
        xi = i;

        /* Destroy index */
        ret = RDB_add_del_index(txp, tbp->val.tb.stp->indexv[i].idxp, ecp);
        if (ret != RDB_OK)
            return ret;

        /*
         * Delete index entry
         */

        RDB_free_tbindex(&tbp->val.tb.stp->indexv[xi]);

        tbp->val.tb.stp->indexc--;
        for (i = xi; i < tbp->val.tb.stp->indexc; i++) {
            tbp->val.tb.stp->indexv[i] = tbp->val.tb.stp->indexv[i + 1];
        }

        p = RDB_realloc(tbp->val.tb.stp->indexv,
                sizeof(RDB_tbindex) * tbp->val.tb.stp->indexc, ecp);
        if (p == NULL)
            return RDB_ERROR;
        tbp->val.tb.stp->indexv = p;
    }

    return RDB_OK;
}

/*@}*/
