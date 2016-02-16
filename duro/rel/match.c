/*
 * Copyright (C) 2008, 2011-2005 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "qresult.h"
#include "optimize.h"
#include "internal.h"
#include "stable.h"
#include <obj/objinternal.h>

#include <string.h>

/*
 * Check if the *tplp matches one of the tuples in *qrp.
 */
static int
qr_matching_tuple(RDB_qresult *qrp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_object tpl;

    *resultp = RDB_FALSE;
    RDB_init_obj(&tpl);

    while ((ret = RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        if (RDB_tuple_matches(tplp, &tpl, ecp, txp, resultp) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        if (*resultp) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_OK;
        }
    }
    if (RDB_obj_type(&ecp->error) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    return RDB_destroy_obj(&tpl, ecp);
}

static int
project_matching(RDB_expression *texp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_expression *argp;
    RDB_object tpl;

    /*
     * Pick attributes which are attributes of the table
     */
    RDB_init_obj(&tpl);
    argp = texp->def.op.args.firstp->nextp;
    while (argp != NULL) {
        char *attrname = RDB_obj_string(&argp->def.obj);
        RDB_object *attrp = RDB_tuple_get(tplp, attrname);
        if (attrp != NULL) {
            if (RDB_tuple_set(&tpl, attrname, attrp, ecp) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
        }
        argp = argp->nextp;
    }
    if (RDB_expr_matching_tuple(texp->def.op.args.firstp, &tpl, ecp, txp,
            resultp) != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    return RDB_destroy_obj(&tpl, ecp);
}

static int
union_matching(RDB_expression *texp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    /*
     * Check if one of the argument expressions contain the tuple
     */
    if (RDB_expr_matching_tuple(texp->def.op.args.firstp, tplp,
            ecp, txp, resultp) != RDB_OK)
        return RDB_ERROR;
    if (*resultp)
        return RDB_OK;
    return RDB_expr_matching_tuple(texp->def.op.args.firstp->nextp, tplp,
            ecp, txp, resultp);
}

static int
join_matching(RDB_expression *texp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    /*
     * Check if both argument expressions contain the tuple
     */
    if (RDB_expr_matching_tuple(texp->def.op.args.firstp, tplp,
            ecp, txp, resultp) != RDB_OK)
        return RDB_ERROR;
    if (!*resultp)
        return RDB_OK;
    return RDB_expr_matching_tuple(texp->def.op.args.firstp->nextp, tplp,
            ecp, txp, resultp);
}

/*
 * Check if one of the tuples in *tbp matches *tplp
 */
int
stored_matching(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp = RDB_table_qresult(tbp, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    ret = qr_matching_tuple(qrp, tplp, ecp, txp, resultp);
    if (ret != RDB_OK) {
        RDB_del_qresult(qrp, ecp, txp);
        return ret;
    }
    return RDB_del_qresult(qrp, ecp, txp);
}

static int
stored_matching_uindex(RDB_object *tbp, const RDB_object *tplp, RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int i;
    int ret;
    RDB_object tpl;
    RDB_object **objpv;

    objpv = RDB_alloc(sizeof(RDB_object *) * indexp->attrc, ecp);
    if (objpv == NULL) {
        return RDB_ERROR;
    }
    for (i = 0; i < indexp->attrc; i++) {
        objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
        objpv[i]->store_typ = objpv[i]->typ;
    }
    RDB_init_obj(&tpl);
    ret = RDB_get_by_uindex(tbp, objpv, indexp, tbp->typ->def.basetyp, ecp,
            txp, &tpl);
    if (ret == RDB_ERROR) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            RDB_clear_err(ecp);
            *resultp = RDB_FALSE;
            ret = RDB_OK;
        }
        goto cleanup;
    }
    if (ret != RDB_OK) {
        goto cleanup;
    }
    if (indexp->attrc < tbp->typ->def.basetyp->def.tuple.attrc) {
        /* Check if non-index attributes match */
        ret = RDB_tuple_matches(tplp, &tpl, ecp, txp, resultp);
    } else {
        *resultp = RDB_TRUE;
    }

cleanup:
    RDB_free(objpv);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

static int
stored_matching_nuindex(RDB_object *tbp, const RDB_object *tplp, RDB_tbindex *indexp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_object tpl;
    RDB_qresult *qrp = RDB_index_qresult(tbp, indexp, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    if (RDB_seek_index_qresult(qrp, indexp, tplp, ecp, txp) != RDB_OK)
        goto error;

    if (RDB_next_tuple(qrp, &tpl, ecp, txp) != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            goto error;
        }

        /* End of qresult reached */
        *resultp = RDB_FALSE;
        RDB_clear_err(ecp);
        return RDB_OK;
    }

    /*
     * If the tuple matches, the result is TRUE.
     * If the tuple does not match, the following tuples from *qrp
     * will not match either, so the result is FALSE.
     */
    if (RDB_tuple_matches(&tpl, tplp, ecp, txp, resultp) != RDB_OK)
        goto error;

    RDB_destroy_obj(&tpl, ecp);
    return RDB_del_qresult(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_del_qresult(qrp, ecp, txp);
    return RDB_ERROR;
}

/*
 * Check if one of the tuples in *exp matches *tplp
 * (The expression must be relation-valued)
 */
int
RDB_expr_matching_tuple(RDB_expression *exp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp;

    switch (exp->kind) {
    case RDB_EX_OBJ:
        return RDB_table_matching_tuple(&exp->def.obj, tplp, ecp, txp, resultp);
    case RDB_EX_TBP:
        if (exp->def.tbref.tbp->val.tbp->stp != NULL
                && exp->def.tbref.indexp != NULL) {
            if (exp->def.tbref.indexp->unique) {
                /* Use unique index */
                return stored_matching_uindex(exp->def.tbref.tbp, tplp,
                        exp->def.tbref.indexp, ecp, txp, resultp);
            }
            return stored_matching_nuindex(exp->def.tbref.tbp, tplp,
                    exp->def.tbref.indexp, ecp, txp, resultp);
        }
        if (exp->def.tbref.tbp->val.tbp->exp != NULL) {
            RDB_expr_matching_tuple(exp->def.tbref.tbp->val.tbp->exp, tplp,
                    ecp, txp, resultp);
        }
        if (exp->def.tbref.tbp->val.tbp->stp == NULL) {
            /*
             * The stored table may have been created by another process,
             * so try to open it
             */
            if (RDB_provide_stored_table(exp->def.tbref.tbp,
                    RDB_FALSE, ecp, txp) != RDB_OK) {
                return RDB_ERROR;
            }

            if (exp->def.tbref.tbp->val.tbp->stp == NULL) {
                /* Physical table representation has not been created, so table is empty */
                *resultp = RDB_FALSE;
                return RDB_OK;
            }
        }
        return stored_matching(exp->def.tbref.tbp, tplp, ecp, txp, resultp);
    case RDB_EX_RO_OP:
        if (strcmp (exp->def.op.name, "project") == 0
                && exp->def.op.args.firstp != NULL) {
            RDB_expression *chexp = exp->def.op.args.firstp;
            if (chexp->kind == RDB_EX_TBP
                    && chexp->def.tbref.tbp->val.tbp->stp != NULL
                    && chexp->def.tbref.indexp != NULL) {
                if (chexp->def.tbref.indexp->unique) {
                    return stored_matching_uindex(chexp->def.tbref.tbp,
                            tplp, chexp->def.tbref.indexp,
                            ecp, txp, resultp);
                }
                return stored_matching_nuindex(chexp->def.tbref.tbp,
                        tplp, chexp->def.tbref.indexp, ecp, txp, resultp);
            }
            return project_matching(exp, tplp, ecp, txp, resultp);
        }
        if ((strcmp (exp->def.op.name, "union") == 0
                || strcmp (exp->def.op.name, "d_union") == 0)
                && exp->def.op.args.firstp != NULL
                && exp->def.op.args.firstp->nextp != NULL) {
            return union_matching(exp, tplp, ecp, txp, resultp);
        }
        if ((strcmp (exp->def.op.name, "join") == 0
                || strcmp (exp->def.op.name, "semijoin") == 0
                || strcmp (exp->def.op.name, "intersect") == 0)
                && exp->def.op.args.firstp != NULL
                && exp->def.op.args.firstp->nextp != NULL) {
            return join_matching(exp, tplp, ecp, txp, resultp);
        }
        break;
    default:
        break;
    }

    qrp = RDB_expr_qresult(exp, ecp, txp);
    if (qrp == NULL)
        goto error;

    ret = qr_matching_tuple(qrp, tplp, ecp, txp, resultp);
    if (ret != RDB_OK) {
        goto error;
    }
    return RDB_del_qresult(qrp, ecp, txp);

error:
    if (qrp != NULL)
        RDB_del_qresult(qrp, ecp, txp);
    return RDB_ERROR;
}

static RDB_bool
index_covers_tuple(RDB_tbindex *indexp, const RDB_object *tplp)
{
    int i;

    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_tuple_get(tplp, indexp->attrv[i].attrname) == NULL)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

/** @addtogroup table
 * @{
 */

/**
 * Check if *<var>tbp</var> contains a tuple that matches *<var>tplp</var>
and store the result at *<var>resultp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

On success, RDB_OK is returned.
If an error occurred, RDB_ERROR is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>operator_not_found_error
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
<dt>invalid_argument_error
<dd>*<var>srcp</var> or *<var>dstp</var> is a table that does not exist.
(e.g. after a rollback)
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_table_matching_tuple(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
	if (tbp->val.tbp->exp == NULL) {
	    int i;

	    if (tbp->val.tbp->stp == NULL) {
            if (RDB_provide_stored_table(tbp,
                    RDB_FALSE, ecp, txp) != RDB_OK) {
                return RDB_ERROR;
            }

            if (tbp->val.tbp->stp == NULL) {
                /* Physical table representation has not been created, so table is empty */
                *resultp = RDB_FALSE;
                return RDB_OK;
            }
	    }

	    /*
	     * Search for a unique index that covers the tuple
	     */
	    for (i = 0;
	            i < tbp->val.tbp->stp->indexc
	                    && !(tbp->val.tbp->stp->indexv[i].unique
	                        && index_covers_tuple(&tbp->val.tbp->stp->indexv[i], tplp));
	            i++);
	    if (i >= tbp->val.tbp->stp->indexc) {
	        /* Not found - scan *tbp for a matching tuple */
	        return stored_matching(tbp, tplp, ecp, txp, resultp);
	    }
		return stored_matching_uindex(tbp, tplp, &tbp->val.tbp->stp->indexv[i],
		        ecp, txp, resultp);
	}
	/* Virtual table */
	return RDB_expr_matching_tuple(tbp->val.tbp->exp, tplp, ecp, txp, resultp);
}

/*@}*/
