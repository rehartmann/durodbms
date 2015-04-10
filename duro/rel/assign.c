/*
 * $Id$
 *
 * Copyright (C) 2005, 2012, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Functions for assignment operations (insert, update, delete, copy),
 * including multiple assignment and DB/table constraint checking
 */

#include "rdb.h"
#include "update.h"
#include "delete.h"
#include "insert.h"
#include "optimize.h"
#include "internal.h"
#include "stable.h"
#include "transform.h"
#include <gen/strfns.h>
#include <obj/objinternal.h>

#include <string.h>

typedef struct insert_node {
    RDB_ma_insert ins;
    struct insert_node *nextp;
} insert_node;

typedef struct update_node {
    RDB_ma_update upd;
    struct update_node *nextp;
} update_node;

typedef struct delete_node {
    RDB_ma_delete del;
    struct delete_node *nextp;
} delete_node;

typedef struct vdelete_node {
    RDB_ma_vdelete del;
    struct vdelete_node *nextp;
} vdelete_node;

typedef struct copy_node {
    RDB_ma_copy cpy;
    struct copy_node *nextp;
} copy_node;

static void
concat_inslists(insert_node **dstpp, insert_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        insert_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static void
concat_updlists(update_node **dstpp, update_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        update_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static void
concat_dellists(delete_node **dstpp, delete_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        delete_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static void
concat_vdellists(vdelete_node **dstpp, vdelete_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        vdelete_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

static void
concat_copylists(copy_node **dstpp, copy_node *srcp)
{
    if (*dstpp == NULL) {
        *dstpp = srcp;
    } else {
        copy_node *lastp = *dstpp;

        /* Find last node */
        while (lastp->nextp != NULL)
            lastp = lastp->nextp;

        /* Concat lists */
        lastp->nextp = srcp;
    }
}

/*
 * Compute the attribute values added by EXTEND of tuple *tplp
 * and compare them with the actual values
 * If they don't match, raise not_found_error.
 *
 */
static int
check_extend_tuple(const RDB_object *tplp, const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *argp = exp->def.op.args.firstp->nextp;

    /* Check the additional attributes */
    while (argp != NULL) {
        RDB_object val;
        RDB_object *valp;
        RDB_bool iseq;

        valp = RDB_tuple_get(tplp, RDB_obj_string(&argp->nextp->def.obj));
        if (valp == NULL) {
            RDB_raise_invalid_argument("invalid EXTEND attribute", ecp);
            return RDB_ERROR;
        }
        RDB_init_obj(&val);
        ret = RDB_evaluate(argp, &RDB_tpl_get, (void *) tplp,
                NULL, ecp, txp, &val);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&val, ecp);
            return ret;
        }
        ret = RDB_obj_equals(&val, valp, ecp, txp, &iseq);
        RDB_destroy_obj(&val, ecp);
        if (ret != RDB_OK)
            return ret;
        if (!iseq) {
            RDB_raise_predicate_violation("EXTEND predicate violation", ecp);
            return RDB_ERROR;
        }
        argp = argp->nextp->nextp;
    }
    return RDB_OK;
}

static int
check_extend(const RDB_object *objp, const RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_qresult *qrp;
    RDB_object tpl;

    if (RDB_is_tuple(objp))
        return check_extend_tuple(objp, exp, ecp, txp);

    /*
     * Check tuples in *objp
     */

    qrp = RDB_table_iterator((RDB_object *) objp, 0, NULL, ecp, NULL);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        if (check_extend_tuple (&tpl, exp, ecp, txp) != RDB_OK)
            goto error;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        goto error;
    RDB_destroy_obj(&tpl, ecp);
    return RDB_del_table_iterator(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    RDB_del_table_iterator(qrp, ecp, txp);
    return RDB_ERROR;
}

static insert_node *
new_insert_node(RDB_object *tbp, const RDB_object *srcp, int flags, RDB_exec_context *ecp)
{
    int ret;

    insert_node *insnp = RDB_alloc(sizeof (insert_node), ecp);
    if (insnp == NULL)
        return NULL;
    insnp->ins.objp = RDB_alloc(sizeof(RDB_object), ecp);
    if (insnp->ins.objp == NULL) {
        RDB_free(insnp);
        return NULL;
    }
    RDB_init_obj(insnp->ins.objp);
    ret = RDB_copy_obj_data(insnp->ins.objp, srcp, ecp, NULL);
    if (ret != RDB_OK) {
        RDB_free_obj(insnp->ins.objp, ecp);
        RDB_free(insnp);
        return NULL;
    }
    insnp->ins.tbp = tbp;
    insnp->ins.flags = flags;
    insnp->nextp = NULL;
    return insnp;
}

static vdelete_node *
new_vdelete_node(RDB_object *tbp, const RDB_object *srcp, RDB_exec_context *ecp)
{
    int ret;

    vdelete_node *vdelnp = RDB_alloc(sizeof (vdelete_node), ecp);
    if (vdelnp == NULL)
        return NULL;
    vdelnp->del.objp = RDB_alloc(sizeof(RDB_object), ecp);
    if (vdelnp->del.objp == NULL) {
        RDB_free(vdelnp);
        return NULL;
    }
    RDB_init_obj(vdelnp->del.objp);
    ret = RDB_copy_obj_data(vdelnp->del.objp, srcp, ecp, NULL);
    if (ret != RDB_OK) {
        RDB_destroy_obj(vdelnp->del.objp, ecp);
        RDB_free(vdelnp->del.objp);
        RDB_free(vdelnp);
        return NULL;
    }
    vdelnp->del.tbp = tbp;
    vdelnp->nextp = NULL;
    return vdelnp;
}

static copy_node *
new_copy_node(RDB_object *tbp, const RDB_object *srcp, RDB_exec_context *ecp)
{
    int ret;

    copy_node *copynp = RDB_alloc(sizeof (copy_node), ecp);
    if (copynp == NULL)
        return NULL;
    copynp->cpy.srcp = RDB_alloc(sizeof(RDB_object), ecp);
    if (copynp->cpy.srcp == NULL) {
        RDB_free(copynp);
        return NULL;
    }
    RDB_init_obj(copynp->cpy.srcp);
    ret = RDB_copy_obj_data(copynp->cpy.srcp, srcp, ecp, NULL);
    if (ret != RDB_OK) {
        RDB_free_obj(copynp->cpy.srcp, ecp);
        RDB_free(copynp);
        return NULL;
    }
    copynp->cpy.dstp = tbp;
    copynp->nextp = NULL;
    return copynp;
}

static void
del_inslist(insert_node *insnp, RDB_exec_context *ecp)
{
    insert_node *hinsnp;

    while (insnp != NULL) {
        hinsnp = insnp->nextp;

        if (insnp->ins.objp != NULL) {
            RDB_free_obj(insnp->ins.objp, ecp);
        }
        RDB_free(insnp);
        insnp = hinsnp;
    }    
}

static void
del_updlist(update_node *updnp, RDB_exec_context *ecp)
{
    update_node *hupdnp;

    while (updnp != NULL) {
        hupdnp = updnp->nextp;

        if (updnp->upd.condp != NULL) {
            RDB_del_expr(updnp->upd.condp, ecp);
        }
        RDB_free(updnp);
        updnp = hupdnp;
    }    
}

static void
del_dellist(delete_node *delnp, RDB_exec_context *ecp)
{
    delete_node *hdelnp;

    while (delnp != NULL) {
        hdelnp = delnp->nextp;

        if (delnp->del.condp != NULL) {
            RDB_del_expr(delnp->del.condp, ecp);
        }
        RDB_free(delnp);
        delnp = hdelnp;
    }    
}

static void
del_vdellist(vdelete_node *delnp, RDB_exec_context *ecp)
{
    vdelete_node *hdelnp;

    while (delnp != NULL) {
        hdelnp = delnp->nextp;

        if (delnp->del.objp != NULL) {
            RDB_destroy_obj(delnp->del.objp, ecp);
            RDB_free(delnp->del.objp);
        }
        RDB_free(delnp);
        delnp = hdelnp;
    }
}

static void
del_copylist(copy_node *copynp, RDB_exec_context *ecp)
{
    copy_node *hcpynp;

    while (copynp != NULL) {
        hcpynp = copynp->nextp;

        if (copynp->cpy.srcp != NULL) {
            RDB_free_obj(copynp->cpy.srcp, ecp);
        }
        RDB_free(copynp);
        copynp = hcpynp;
    }
}

static int
resolve_insert(RDB_object *tbp, const RDB_object *srcp, int flags, insert_node **insnpp,
               RDB_exec_context *, RDB_transaction *);

static int
resolve_insert_expr(RDB_expression *, const RDB_object *, int,
    insert_node **, RDB_exec_context *, RDB_transaction *);

static int
src_matches_condition(RDB_expression *condp, const RDB_object *srcp,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_object tpl;
    RDB_qresult *qrp;

    if (RDB_is_tuple(srcp)) {
        return RDB_evaluate_bool(condp, &RDB_tpl_get,
                (void *) srcp, NULL, ecp, txp, resultp);
    }
    /* *srcp is a table */

    qrp = RDB_table_iterator((RDB_object *) srcp, 0, NULL, ecp, NULL);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    while (RDB_next_tuple(qrp, &tpl, ecp, txp) == RDB_OK) {
        if (RDB_evaluate_bool(condp, &RDB_tpl_get,
                (void *) &tpl, NULL, ecp, txp, resultp) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            RDB_del_table_iterator(qrp, ecp, NULL);
            return RDB_ERROR;
        }
        if (!*resultp) {
            RDB_destroy_obj(&tpl, ecp);
            RDB_del_table_iterator(qrp, ecp, NULL);
            return RDB_OK;
        }
    }
    RDB_destroy_obj(&tpl, ecp);
    RDB_del_table_iterator(qrp, ecp, txp);
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
resolve_insert_expr(RDB_expression *exp, const RDB_object *srcp, int flags,
    insert_node **insnpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tbp;

    switch (exp->kind) {
        case RDB_EX_TBP:
            return resolve_insert(exp->def.tbref.tbp, srcp, flags, insnpp, ecp, txp);
        case RDB_EX_OBJ:
            return resolve_insert(&exp->def.obj, srcp, flags, insnpp, ecp, txp);
        case RDB_EX_RO_OP:
            break;
        case RDB_EX_VAR:
            /* Resolve variable */
            tbp = RDB_get_table(exp->def.varname, ecp, txp);
            if (tbp == NULL)
                return RDB_ERROR;
            return resolve_insert(tbp, srcp, flags, insnpp, ecp, txp);
        default:
            RDB_raise_invalid_argument("invalid target table", ecp);
            return RDB_ERROR;
    }

    if (strcmp(exp->def.op.name, "where") == 0) {
        RDB_bool b;
        if (src_matches_condition(exp->def.op.args.firstp->nextp, srcp, ecp, txp, &b)
                != RDB_OK)
            return RDB_ERROR;

        if (!b) {
           RDB_raise_predicate_violation("where predicate violation", ecp);
           return RDB_ERROR;
        }
        return resolve_insert_expr(exp->def.op.args.firstp, srcp, flags, insnpp,
                ecp, txp);
    }
    if (strcmp(exp->def.op.name, "project") == 0
        || strcmp(exp->def.op.name, "remove") == 0) {
        return resolve_insert_expr(exp->def.op.args.firstp, srcp, flags, insnpp,
                ecp, txp);
    }
    if (strcmp(exp->def.op.name, "rename") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        if (RDB_invrename_tuple_ex(srcp, exp, ecp, &tpl) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = resolve_insert_expr(exp->def.op.args.firstp, &tpl, flags,
                insnpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    if (strcmp(exp->def.op.name, "extend") == 0) {
        ret = check_extend(srcp, exp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        return resolve_insert_expr(exp->def.op.args.firstp, srcp, flags, insnpp,
                ecp, txp);
    }
    if (strcmp(exp->def.op.name, "unwrap") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        ret = RDB_invunwrap_tuple(srcp, exp, ecp, txp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = resolve_insert_expr(exp->def.op.args.firstp, &tpl, flags, insnpp,
                ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    if (strcmp(exp->def.op.name, "wrap") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        ret = RDB_invwrap_tuple(srcp, exp, ecp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = resolve_insert_expr(exp->def.op.args.firstp, &tpl, flags,
                insnpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    RDB_raise_not_supported("insert is not supported for this kind of table",
            ecp);
    return RDB_ERROR;
}

static int
resolve_insert(RDB_object *tbp, const RDB_object *srcp, int flags, insert_node **insnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
  	if (tbp->val.tb.exp == NULL) {
        *insnpp = new_insert_node(tbp, srcp, flags, ecp);
        if (*insnpp == NULL)
            return RDB_ERROR;
        return RDB_OK;
    }

    return resolve_insert_expr(tbp->val.tb.exp, srcp, flags, insnpp, ecp, txp);
}

static int
resolve_update(RDB_object *tbp, RDB_expression *condp,
        int updc, RDB_attr_update *updv, update_node **updnpp,
        RDB_exec_context *ecp, RDB_transaction *txp);

static update_node *
new_update_node(RDB_object *tbp, RDB_expression *condp,
        int updc, RDB_attr_update *updv, RDB_exec_context *ecp)
{
    update_node *nupdnp = RDB_alloc(sizeof (update_node), ecp);
    if (nupdnp == NULL)
        return NULL;
    if (condp == NULL) {
        nupdnp->upd.condp = NULL;
    } else {
        nupdnp->upd.condp = RDB_dup_expr(condp, ecp);
        if (nupdnp->upd.condp == NULL) {
            RDB_free(nupdnp);
            return NULL;
        }
    }
    nupdnp->upd.tbp = tbp;
    nupdnp->upd.updc = updc;
    nupdnp->upd.updv = updv;
    nupdnp->nextp = NULL;
    return nupdnp;
}

static int
resolve_update_expr(RDB_expression *texp, RDB_expression *condp,
    int updc, RDB_attr_update *updv, update_node **updnpp,
    RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *tbp;

    switch (texp->kind) {
        case RDB_EX_TBP:
            return resolve_update(texp->def.tbref.tbp, condp, updc, updv,
                    updnpp, ecp, txp);
        case RDB_EX_OBJ:
            return resolve_update(&texp->def.obj, condp, updc, updv, updnpp,
                    ecp, txp);
        case RDB_EX_VAR:
            /* Resolve variable */
            tbp = RDB_get_table(texp->def.varname, ecp, txp);
            if (tbp == NULL)
                return RDB_ERROR;
            return resolve_update(tbp, condp, updc, updv, updnpp, ecp, txp);
        case RDB_EX_RO_OP:
            break;
        default:
            RDB_raise_invalid_argument("invalid target table", ecp);
            return RDB_ERROR;
    }

    if (strcmp(texp->def.op.name, "project") == 0
            || strcmp(texp->def.op.name, "remove") == 0) {
        return resolve_update_expr(texp->def.op.args.firstp,
                condp, updc, updv, updnpp, ecp, txp);
    }

    RDB_raise_not_supported(
            "Update is not supported for this virtual table", ecp);
    return RDB_ERROR;   
}

static int
resolve_update(RDB_object *tbp, RDB_expression *condp,
        int updc, RDB_attr_update *updv, update_node **updnpp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_expression *exp = tbp->val.tb.exp;

   	if (exp == NULL) {
        *updnpp = new_update_node(tbp, condp, updc, updv, ecp);
        if (*updnpp == NULL)
            return RDB_ERROR;
        return RDB_OK;
    }
    return resolve_update_expr(exp, condp, updc, updv, updnpp, ecp, txp);
}

static delete_node *
new_delete_node(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *ecp)
{
    delete_node *ndelnp = RDB_alloc(sizeof (delete_node), ecp);
    if (ndelnp == NULL)
        return NULL;
    if (condp == NULL) {
        ndelnp->del.condp = NULL;
    } else {
        ndelnp->del.condp = RDB_dup_expr(condp, ecp);
        if (ndelnp->del.condp == NULL) {
            RDB_free(ndelnp);
            return NULL;
        }
    }
    ndelnp->del.tbp = tbp;
    ndelnp->nextp = NULL;
    return ndelnp;
}

static int
resolve_delete(RDB_object *tbp, RDB_expression *condp, delete_node **,
               RDB_exec_context *, RDB_transaction *);

static int
resolve_delete_expr(RDB_expression *exp, RDB_expression *condp,
        delete_node **delnpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    delete_node *delnp;
    RDB_object *tbp;

    switch (exp->kind) {
        case RDB_EX_TBP:
            return resolve_delete(exp->def.tbref.tbp, condp, delnpp, ecp, txp);
        case RDB_EX_OBJ:
            return resolve_delete(&exp->def.obj, condp, delnpp, ecp, txp);
        case RDB_EX_RO_OP:
            break;
        case RDB_EX_VAR:
            /* Resolve variable */
            tbp = RDB_get_table(exp->def.varname, ecp, txp);
            if (tbp == NULL)
                return RDB_ERROR;
            return resolve_delete(tbp, condp, delnpp, ecp, txp);
        default:
            RDB_raise_invalid_argument("invalid target table", ecp);
            return RDB_ERROR;
    }

    if (strcmp(exp->def.op.name, "project") == 0
            || strcmp(exp->def.op.name, "remove") == 0) {
        return resolve_delete_expr(exp->def.op.args.firstp,
                condp, delnpp, ecp, txp);
    }

    /*
     * Add WHERE condition to all deletes
     */
    if (strcmp(exp->def.op.name, "where") == 0) {
        if (resolve_delete_expr(exp->def.op.args.firstp, condp, &delnp,
                ecp, txp) != RDB_OK)
            return RDB_ERROR;

        *delnpp = delnp;
        while (delnp != NULL) {
            if (delnp->del.condp == NULL) {
                delnp->del.condp = RDB_dup_expr(
                        exp->def.op.args.firstp->nextp, ecp);
                if (delnp->del.condp == NULL) {
                    del_dellist(*delnpp, ecp);
                    return RDB_ERROR;
                }
            } else {
                RDB_expression *ncondp;
                RDB_expression *hcondp = RDB_dup_expr(
                        exp->def.op.args.firstp->nextp, ecp);
                if (hcondp == NULL) {
                    del_dellist(delnp, ecp);
                    return RDB_ERROR;
                }
                ncondp = RDB_ro_op("and", ecp);
                if (ncondp == NULL) {
                    RDB_del_expr(hcondp, ecp);
                    del_dellist(delnp, ecp);
                    return RDB_ERROR;
                }
                RDB_add_arg(ncondp, hcondp);
                RDB_add_arg(ncondp, delnp->del.condp);
                delnp->del.condp = ncondp;
            }
            delnp = delnp->nextp;
        }
        return RDB_OK;
    }

    if (strcmp(exp->def.op.name, "rename") == 0) {
        if (resolve_delete_expr(exp->def.op.args.firstp, condp, &delnp,
                ecp, txp) != RDB_OK)
            return RDB_ERROR;

        *delnpp = delnp;
        while (delnp != NULL) {
            if (delnp->del.condp != NULL) {
                if (RDB_invrename_expr(delnp->del.condp, exp, ecp) != RDB_OK) {
                    del_dellist(*delnpp, ecp);
                    return RDB_ERROR;
                }
            }
            delnp = delnp->nextp;
        }
        return RDB_OK;
    }

    if (strcmp(exp->def.op.name, "extend") == 0) {
        if (resolve_delete_expr(exp->def.op.args.firstp, condp, &delnp,
                ecp, txp) != RDB_OK)
            return RDB_ERROR;

        *delnpp = delnp;
        while (delnp != NULL) {
            if (delnp->del.condp != NULL) {
                if (RDB_resolve_exprnames(&delnp->del.condp,
                        exp->def.op.args.firstp->nextp, ecp) != RDB_OK) {
                    del_dellist(*delnpp, ecp);
                    return RDB_ERROR;
                }
            }
            delnp = delnp->nextp;
        }
        return RDB_OK;
    }

    RDB_raise_not_supported("delete is not supported for this kind of table",
            ecp);
    return RDB_ERROR;   
}

static int
resolve_delete(RDB_object *tbp, RDB_expression *condp, delete_node **delnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
	if (tbp->val.tb.exp == NULL) {
        *delnpp = new_delete_node(tbp, condp, ecp);
        if (*delnpp == NULL)
            return RDB_ERROR;
        return RDB_OK;
	}

    return resolve_delete_expr(tbp->val.tb.exp, condp, delnpp, ecp, txp);
}

static int
resolve_vdelete_expr(RDB_expression *, const RDB_object *,
    vdelete_node **, RDB_exec_context *, RDB_transaction *);

static int
resolve_vdelete(RDB_object *tbp, const RDB_object *srcp, vdelete_node **vdelnpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (tbp->val.tb.exp == NULL) {
        *vdelnpp = new_vdelete_node(tbp, srcp, ecp);
        if (*vdelnpp == NULL)
            return RDB_ERROR;
        return RDB_OK;
    }

    return resolve_vdelete_expr(tbp->val.tb.exp, srcp, vdelnpp, ecp, txp);
}

static int
resolve_vdelete_expr(RDB_expression *exp, const RDB_object *srcp,
    vdelete_node **vdelnpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tbp;

    switch (exp->kind) {
        case RDB_EX_TBP:
            return resolve_vdelete(exp->def.tbref.tbp, srcp, vdelnpp, ecp, txp);
        case RDB_EX_OBJ:
            return resolve_vdelete(&exp->def.obj, srcp, vdelnpp, ecp, txp);
        case RDB_EX_RO_OP:
            break;
        case RDB_EX_VAR:
            /* Resolve variable */
            tbp = RDB_get_table(exp->def.varname, ecp, txp);
            if (tbp == NULL)
                return RDB_ERROR;
            return resolve_vdelete(tbp, srcp, vdelnpp, ecp, txp);
        default:
            RDB_raise_invalid_argument("invalid target table", ecp);
            return RDB_ERROR;
    }

    if (strcmp(exp->def.op.name, "where") == 0) {
        RDB_bool b;

        if (src_matches_condition(exp->def.op.args.firstp->nextp, srcp, ecp, txp, &b)
                != RDB_OK)
            return RDB_ERROR;

        if (!b) {
            /* The tuple cannot be an element of the WHERE table */
            RDB_raise_not_found("tuple not found in WHERE table", ecp);
            return RDB_ERROR;
        }
        return resolve_vdelete_expr(exp->def.op.args.firstp, srcp, vdelnpp,
                ecp, txp);
    }
    if (strcmp(exp->def.op.name, "rename") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        if (RDB_invrename_tuple_ex(srcp, exp, ecp, &tpl) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = resolve_vdelete_expr(exp->def.op.args.firstp, &tpl, vdelnpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    if (strcmp(exp->def.op.name, "extend") == 0) {
        if (check_extend(srcp, exp, ecp, txp) != RDB_OK) {
            /* Convert predicate_violation_error to not_found_error */
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_PREDICATE_VIOLATION_ERROR) {
                RDB_raise_not_found("tuple does not match EXTEND definition", ecp);
            }
            return RDB_ERROR;
        }
        return resolve_vdelete_expr(exp->def.op.args.firstp, srcp, vdelnpp,
                ecp, txp);
    }
    RDB_raise_not_supported("insert is not supported for this kind of table",
            ecp);
    return RDB_ERROR;
}

static int
resolve_copy(RDB_object *, const RDB_object *, copy_node **,
               RDB_exec_context *, RDB_transaction *);

static int
resolve_copy_expr(RDB_expression *exp, const RDB_object *srcp,
    copy_node **copynpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_object *tbp;

    switch (exp->kind) {
        case RDB_EX_TBP:
            return resolve_copy(exp->def.tbref.tbp, srcp, copynpp, ecp, txp);
        case RDB_EX_OBJ:
            return resolve_copy(&exp->def.obj, srcp, copynpp, ecp, txp);
        case RDB_EX_RO_OP:
            break;
        case RDB_EX_VAR:
            /* Resolve variable */
            tbp = RDB_get_table(exp->def.varname, ecp, txp);
            if (tbp == NULL)
                return RDB_ERROR;
            return resolve_copy(tbp, srcp, copynpp, ecp, txp);
        default:
            RDB_raise_invalid_argument("invalid target table", ecp);
            return RDB_ERROR;
    }

    if (strcmp(exp->def.op.name, "where") == 0) {
        RDB_bool b;
        if (src_matches_condition(exp->def.op.args.firstp->nextp, srcp, ecp, txp, &b)
                != RDB_OK)
            return RDB_ERROR;

        if (!b) {
           RDB_raise_predicate_violation("where predicate violation", ecp);
           return RDB_ERROR;
        }
        return resolve_copy_expr(exp->def.op.args.firstp, srcp, copynpp,
                ecp, txp);
    }
    if (strcmp(exp->def.op.name, "project") == 0
        || strcmp(exp->def.op.name, "remove") == 0) {
        return resolve_copy_expr(exp->def.op.args.firstp, srcp, copynpp, ecp,
                txp);
    }
    if (strcmp(exp->def.op.name, "rename") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        if (RDB_invrename_tuple_ex(srcp, exp, ecp, &tpl) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = resolve_copy_expr(exp->def.op.args.firstp, &tpl, copynpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    if (strcmp(exp->def.op.name, "extend") == 0) {
        ret = check_extend(srcp, exp, ecp, txp);
        if (ret != RDB_OK)
            return RDB_ERROR;
        return resolve_copy_expr(exp->def.op.args.firstp, srcp, copynpp, ecp, txp);
    }
    if (strcmp(exp->def.op.name, "unwrap") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        ret = RDB_invunwrap_tuple(srcp, exp, ecp, txp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return RDB_ERROR;
        }
        ret = resolve_copy_expr(exp->def.op.args.firstp, &tpl, copynpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    if (strcmp(exp->def.op.name, "wrap") == 0) {
        RDB_object tpl;

        RDB_init_obj(&tpl);
        ret = RDB_invwrap_tuple(srcp, exp, ecp, &tpl);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }
        ret = resolve_copy_expr(exp->def.op.args.firstp, &tpl, copynpp, ecp, txp);
        RDB_destroy_obj(&tpl, ecp);
        return ret;
    }
    RDB_raise_not_supported("copy is not supported for this kind of table",
            ecp);
    return RDB_ERROR;
}

static int
resolve_copy(RDB_object *dstp, const RDB_object *srcp, copy_node **copynpp,
               RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (dstp->val.tb.exp == NULL) {
        *copynpp = new_copy_node(dstp, srcp, ecp);
        return *copynpp == NULL ? RDB_ERROR : RDB_OK;
    }

    return resolve_copy_expr(dstp->val.tb.exp, srcp, copynpp, ecp, txp);
}

/* Check if the source relation type matches the destination table */
static RDB_bool
source_table_type_matches(const RDB_object *dsttbp, const RDB_type *srctyp) {
    int i;
    RDB_attr *attrp;
    RDB_type *dsttptyp = RDB_base_type(RDB_obj_type(dsttbp));

    /* Every source attribute must appear in the destination */
    for (i = 0; i < srctyp->def.basetyp->def.tuple.attrc; i++) {
        attrp = RDB_tuple_type_attr(dsttptyp,
                srctyp->def.basetyp->def.tuple.attrv[i].name);
        if (attrp == NULL || !RDB_type_equals(attrp->typ,
                srctyp->def.basetyp->def.tuple.attrv[i].typ))
            return RDB_FALSE;
    }

    /*
     * Every destination attribute must either appear in the source
     * or in the default attributes
     */
    for (i = 0; i < dsttptyp->def.tuple.attrc; i++) {
        attrp = RDB_tuple_type_attr(srctyp->def.basetyp,
                dsttptyp->def.tuple.attrv[i].name);
        if (attrp != NULL) {
            /*
             * If the attribute was found in the source table,
             * types must match
             */
            if (!RDB_type_equals(attrp->typ, dsttptyp->def.tuple.attrv[i].typ))
                return RDB_FALSE;
        } else {
            /* Attribute must appear in default attributes */
            if (dsttbp->val.tb.default_map == NULL
                    || RDB_hashmap_get(dsttbp->val.tb.default_map,
                            dsttptyp->def.tuple.attrv[i].name) == NULL) {
                return RDB_FALSE;
            }
        }
    }
    return RDB_TRUE;
}

static RDB_int
do_insert(const RDB_ma_insert *insp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_int rc;

    switch (insp->objp->kind) {
    case RDB_OB_INITIAL:
    case RDB_OB_TUPLE:
        if (RDB_insert_real(insp->tbp, insp->objp, ecp, txp)
                != RDB_OK) {
            if ((insp->flags & RDB_DISTINCT) != 0
                    || RDB_obj_type(RDB_get_err(ecp)) != &RDB_ELEMENT_EXISTS_ERROR) {
                return RDB_ERROR;
            }
        }
        return (RDB_int) 1;
    case RDB_OB_TABLE:
        rc = RDB_move_tuples(insp->tbp, insp->objp, insp->flags, ecp,
                txp);
        if (rc == RDB_ERROR) {
            return (RDB_int) RDB_ERROR;
        }
        if (rc == 0) {
            /*
             * If the source table was empty, check types
             * (Otherwise the type is checked during insertion of the tuples)
             */
            if (!source_table_type_matches(insp->tbp,
                    RDB_obj_type(insp->objp))) {
                RDB_raise_type_mismatch(
                        "Source table type does not match destination", ecp);
                return (RDB_int) RDB_ERROR;
            }
        }
        return rc;
    default:
        RDB_raise_invalid_argument(
                "INSERT requires tuple or relation argument", ecp);
        return (RDB_int) RDB_ERROR;
    }
}

/*
 * Perform update. *updp->tbp must be a real table.
 */
static RDB_int
do_update(const RDB_ma_update *updp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *tbexp, *nexp;
    RDB_expression *exp = NULL;

    if (updp->condp == NULL) {
        return RDB_update_real(updp->tbp, NULL, updp->updc, updp->updv,
                ecp, txp);
    }

    tbexp = RDB_table_ref(updp->tbp, ecp);
    if (tbexp == NULL)
        return RDB_ERROR;

    /*
     * Build WHERE expression
     */
    exp = RDB_ro_op("where", ecp);
    if (exp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, tbexp);
    RDB_add_arg(exp, updp->condp);

    nexp = RDB_optimize_expr(exp, 0, NULL, NULL, ecp, txp);
    exp->def.op.args.firstp = NULL;
    RDB_del_expr(exp, ecp);
    RDB_del_expr(tbexp, ecp);
    if (nexp == NULL)
        return RDB_ERROR;

    if (nexp->kind == RDB_EX_TBP) {
        ret = RDB_update_real(nexp->def.tbref.tbp, NULL, updp->updc, updp->updv,
                ecp, txp);
        RDB_del_expr(nexp, ecp);
        return ret;
    }
    if (nexp->kind == RDB_EX_RO_OP
            && strcmp (nexp->def.op.name, "where") == 0) {
        if (nexp->def.op.optinfo.objc > 0
                || nexp->def.op.optinfo.stopexp != NULL) {
            ret = RDB_update_where_index(nexp, NULL, updp->updc, updp->updv,
                    ecp, txp);
            RDB_del_expr(nexp, ecp);
            return ret;
        }

        if (nexp->def.op.args.firstp->kind == RDB_EX_RO_OP
                && strcmp(nexp->def.op.args.firstp->def.op.name, "where") == 0
                && (nexp->def.op.args.firstp->def.op.optinfo.objc > 0
                        || nexp->def.op.args.firstp->def.op.optinfo.stopexp != NULL)) {
            ret = RDB_update_where_index(nexp->def.op.args.firstp,
                    nexp->def.op.args.firstp->nextp, updp->updc, updp->updv,
                    ecp, txp);
            RDB_del_expr(nexp, ecp);
            return ret;
        }

        if (nexp->def.op.args.firstp->kind == RDB_EX_TBP) {
            ret = RDB_update_real(nexp->def.op.args.firstp->def.tbref.tbp,
                    nexp->def.op.args.firstp->nextp, updp->updc, updp->updv,
                    ecp, txp);
            RDB_del_expr(nexp, ecp);
            return ret;
        }
    }
    RDB_del_expr(nexp, ecp);
    RDB_raise_not_supported("unsupported update", ecp);
    return RDB_ERROR;
}

/*
 * Perform a delete. delp->tbp must be a real table.
 */
static RDB_int
do_delete(const RDB_ma_delete *delp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_expression *tbexp, *exp, *nexp;

    if (delp->condp == NULL) {
        return RDB_delete_real(delp->tbp, NULL, ecp, txp);
    }

    tbexp = RDB_table_ref(delp->tbp, ecp);
    if (tbexp == NULL)
        return RDB_ERROR;

    exp = RDB_ro_op("where", ecp);
    if (exp == NULL)
        return RDB_ERROR;
    RDB_add_arg(exp, tbexp);
    RDB_add_arg(exp, delp->condp);

    nexp = RDB_optimize_expr(exp, 0, NULL, NULL, ecp, txp);
    if (nexp == NULL)
        return RDB_ERROR;

    exp->def.op.args.firstp = NULL;
    RDB_del_expr(exp, ecp);
    RDB_del_expr(tbexp, ecp);

    if (nexp->kind == RDB_EX_TBP) {
        ret = RDB_delete_real(nexp->def.tbref.tbp, NULL, ecp, txp);
        RDB_del_expr(nexp, ecp);
        return ret;
    }
    if (nexp->kind == RDB_EX_RO_OP && strcmp (nexp->def.op.name, "where") == 0) {
        if (nexp->def.op.optinfo.objc > 0
                || nexp->def.op.optinfo.stopexp != NULL) {
            ret = RDB_delete_where_index(nexp, NULL, ecp, txp);
            RDB_del_expr(nexp, ecp);
            return ret;
        }

        if (nexp->def.op.args.firstp->kind == RDB_EX_RO_OP
                && strcmp(nexp->def.op.args.firstp->def.op.name, "where") == 0
                && (nexp->def.op.args.firstp->def.op.optinfo.objc > 0
                   || nexp->def.op.optinfo.stopexp != NULL)) {
            ret = RDB_delete_where_index(nexp->def.op.args.firstp,
                    nexp->def.op.args.firstp->nextp, ecp, txp);
            RDB_del_expr(nexp, ecp);
            return ret;
        }

        if (nexp->def.op.args.firstp->kind == RDB_EX_TBP) {
            ret = RDB_delete_real(nexp->def.op.args.firstp->def.tbref.tbp,
                    nexp->def.op.args.firstp->nextp, ecp, txp);
            RDB_del_expr(nexp, ecp);
            return ret;
        }
    }
    RDB_del_expr(nexp, ecp);
    RDB_raise_not_supported("Unsupported delete", ecp);
    return RDB_ERROR;
}

static RDB_int
do_vdelete_rel(RDB_object *destp, RDB_object *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_int delcount = 0;

    RDB_init_obj(&tpl);
    qrp = RDB_table_iterator(srcp, 0, NULL, ecp, txp);
    if (qrp == NULL)
        goto error;
    while (RDB_next_tuple(qrp,  &tpl, ecp, txp) == RDB_OK) {
        if (RDB_delete_real_tuple(destp, &tpl, ecp, txp)
                == (RDB_int) RDB_ERROR) {
            goto error;
        }
        delcount++;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
        goto error;

    RDB_destroy_obj(&tpl, ecp);
    if (RDB_del_table_iterator(qrp, ecp, txp) != RDB_OK)
        return RDB_ERROR;
    return delcount;

error:
    RDB_destroy_obj(&tpl, ecp);
    if (qrp != NULL) {
        RDB_del_table_iterator(qrp, ecp, txp);
    }
    return (RDB_int) RDB_ERROR;
}

/*
 * Perform a delete of tuples. delp->tbp must be a real table.
 */
static RDB_int
do_vdelete(const RDB_ma_vdelete *delp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    switch (delp->objp->kind) {
        case RDB_OB_INITIAL:
        case RDB_OB_TUPLE:
            return RDB_delete_real_tuple(delp->tbp, delp->objp, ecp, txp);
        case RDB_OB_TABLE:
            return do_vdelete_rel(delp->tbp, delp->objp, ecp, txp);
        default: ;
    }
    RDB_raise_invalid_argument(
            "DELETE requires tuple or relation argument", ecp);
    return RDB_ERROR;
}

static int
copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *srctyp = RDB_obj_type(srcvalp);

    if (RDB_copy_obj_data(dstvalp, srcvalp, ecp, txp) != RDB_OK)
        return RDB_ERROR;

    /*
     * Copy type information so copying works even if the destination
     * has been newly initialized usind RDB_init_obj().
     */
    if (srctyp != NULL && RDB_type_is_scalar(srctyp)) {
        dstvalp->typ = srctyp;
    }
    return RDB_OK;
} 

/*
 * Convert inserts into virtual tables to inserts into real tables.
 * *geninspp will contain a pointer to the list of generated table inserts or
 * NULL if there were no virtual tables to be resolved.
 */
static int
resolve_inserts(int insc, const RDB_ma_insert *insv, RDB_ma_insert **ninsvp,
        insert_node **geninspp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int ninsc = 0;
    int llen = 0;

    /* list of generated inserts */
    insert_node *geninsp = NULL;

    insert_node *insnp;

    for (i = 0; i < insc; i++) {
        if (RDB_TB_CHECK & insv[i].tbp->val.tb.flags) {
            if (RDB_check_table(insv[i].tbp, ecp, txp) != RDB_OK) {
                goto error;
            }
        }

        if (insv[i].tbp->val.tb.exp != NULL) {
            if (resolve_insert(insv[i].tbp, insv[i].objp, insv[i].flags, &insnp,
                    ecp, txp) != RDB_OK)
                goto error;

            /* Add inserts to list */
            concat_inslists(&geninsp, insnp);
            llen++;
        } else {
            ninsc++;
        }
    }

    if (llen == 0) {
        /* No tables had to be resolved, simply return insv and insc */
        *ninsvp = (RDB_ma_insert *) insv;
        *geninspp = NULL;
        return insc;
    }

    ninsc += llen;

    /*
     * If inserts have been generated, allocate new insert list
     * which consists of old and new inserts
     */

    *ninsvp = RDB_alloc(sizeof (RDB_ma_insert) * ninsc, ecp);
    if (*ninsvp == NULL)
        goto error;

    j = 0;
    for (i = 0; i < insc; i++) {
        if (insv[i].tbp->val.tb.exp == NULL) {
            (*ninsvp)[j].tbp = insv[i].tbp;
            (*ninsvp)[j++].objp = insv[i].objp;
        }
    }

    insnp = geninsp;
    while (insnp != NULL) {
        (*ninsvp)[j].tbp = insnp->ins.tbp;
        (*ninsvp)[j].objp = insnp->ins.objp;

        insnp = insnp->nextp;
        j++;
    }
    *geninspp = geninsp;
    return ninsc;

error:
    if (geninsp != NULL)
        del_inslist(geninsp, ecp);

    return RDB_ERROR;
}

/*
 * Convert updates into virtual tables to updates of real tables.
 *
 * *genupdpp will contain a pointer to the list of generated table updates or
 * NULL if there were no virtual tables to be resolved.
 */
static int
resolve_updates(int updc, const RDB_ma_update *updv, RDB_ma_update **nupdvp,
        update_node **genupdpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int ret;
    int nupdc = 0;
    int llen = 0;

    /* list of generated updates */
    update_node *genupdp = NULL;

    update_node *updnp;

    for (i = 0; i < updc; i++) {
        if (RDB_TB_CHECK & updv[i].tbp->val.tb.flags) {
            if (RDB_check_table(updv[i].tbp, ecp, txp) != RDB_OK) {
                ret = RDB_ERROR;
                goto error;
            }
        }

        if (updv[i].tbp->val.tb.exp != NULL) {
            /* Convert virtual table updates to real table updates */
            ret = resolve_update(updv[i].tbp, updv[i].condp,
                    updv[i].updc, updv[i].updv, &updnp, ecp, txp);
            if (ret != RDB_OK)
                goto error;

            /* Add updates to list */
            concat_updlists(&genupdp, updnp);
            llen++;
        } else {
            nupdc++;
        }
    }

    if (llen == 0) {
        *nupdvp = (RDB_ma_update *) updv;
        *genupdpp = NULL;
        return updc;
    }

    /*
     * Allocate new list which consists of old stored table updates
     * and new updates
     */

    nupdc += llen;

    (*nupdvp) = RDB_alloc(sizeof (RDB_ma_update) * nupdc, ecp);
    if (*nupdvp == NULL)
        goto error;

    j = 0;
    for (i = 0; i < updc; i++) {
        if (updv[i].tbp->val.tb.exp == NULL) {
            (*nupdvp)[j].tbp = updv[i].tbp;
            (*nupdvp)[j].condp = updv[i].condp;
            (*nupdvp)[j].updc = updv[i].updc;
            (*nupdvp)[j++].updv = updv[i].updv;
        }
    }

    updnp = genupdp;
    while (updnp != NULL) {
        (*nupdvp)[j].tbp = updnp->upd.tbp;
        (*nupdvp)[j].condp = updnp->upd.condp;
        (*nupdvp)[j].updc = updnp->upd.updc;
        (*nupdvp)[j].updv = updnp->upd.updv;

        updnp = updnp->nextp;
        j++;
    }
    *genupdpp = genupdp;
    return nupdc;

error:
    if (genupdp != NULL)
        del_updlist(genupdp, ecp);

    return RDB_ERROR;
}

/*
 * Convert deletes of the form DELETE <table> [WHERE expr]
 * of virtual tables to deletes of real tables.
 *
 * *gendelpp will contain a pointer to the list of generated table deletes or
 * NULL if there were no virtual tables to be resolved.
 */
static int
resolve_deletes(int delc, const RDB_ma_delete *delv, RDB_ma_delete **ndelvp,
        delete_node **gendelpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int ret;
    int ndelc = 0;
    int llen = 0;

    /* list of generated deletes */
    delete_node *gendelp = NULL;

    delete_node *delnp;

    for (i = 0; i < delc; i++) {
        if (RDB_TB_CHECK & delv[i].tbp->val.tb.flags) {
            if (RDB_check_table(delv[i].tbp, ecp, txp) != RDB_OK) {
                goto error;
            }
        }

        if (delv[i].tbp->val.tb.exp != NULL) {
            /* Convert virtual table deletes to real table deletes */
            ret = resolve_delete(delv[i].tbp, delv[i].condp, &delnp, ecp, txp);
            if (ret != RDB_OK)
                goto error;

            /* Add deletes to list */
            concat_dellists(&gendelp, delnp);
            llen++;
        } else {
            ndelc++;
        }
    }

    if (llen == 0) {
        *ndelvp = (RDB_ma_delete *) delv;
        *gendelpp = NULL;
        return delc;
    }

    /*
     * If deletes have been generated, allocate new list
     * which consists of old and new deletes
     */

    ndelc += llen;

    (*ndelvp) = RDB_alloc(sizeof (RDB_ma_delete) * ndelc, ecp);
    if (*ndelvp == NULL) {
        goto error;
    }

    j = 0;
    for (i = 0; i < delc; i++) {
        if (delv[i].tbp->val.tb.exp == NULL) {
            (*ndelvp)[j].tbp = delv[i].tbp;
            (*ndelvp)[j++].condp = delv[i].condp;
        }
    }

    delnp = gendelp;
    while (delnp != NULL) {
        (*ndelvp)[j].tbp = delnp->del.tbp;
        (*ndelvp)[j].condp = delnp->del.condp;

        delnp = delnp->nextp;
        j++;
    }

    *gendelpp = gendelp;
    return ndelc;
    
error:
    if (gendelp != NULL)
        del_dellist(gendelp, ecp);

    return RDB_ERROR;
}

/*
 * Convert deletes of the form DELETE <table> <tuple or table>
 * of virtual tables to deletes of real tables.
 *
 * *gendelpp will contain a pointer to the list of generated table deletes
 * or NULL if there were no virtual tables to be resolved.
 */
static int
resolve_vdeletes(int delc, const RDB_ma_vdelete *delv, RDB_ma_vdelete **ndelvp,
        vdelete_node **gendelpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int ret;
    int ndelc = 0;
    int llen = 0;

    /* List of generated deletes */
    vdelete_node *gendelp = NULL;

    vdelete_node *delnp;

    for (i = 0; i < delc; i++) {
        if (RDB_TB_CHECK & delv[i].tbp->val.tb.flags) {
            if (RDB_check_table(delv[i].tbp, ecp, txp) != RDB_OK) {
                goto error;
            }
        }

        if (delv[i].tbp->val.tb.exp != NULL) {
            /* Convert virtual table deletes to real table deletes */
            ret = resolve_vdelete(delv[i].tbp, delv[i].objp, &delnp, ecp, txp);
            if (ret != RDB_OK)
                goto error;

            /* Add deletes to list */
            concat_vdellists(&gendelp, delnp);
            llen++;
        } else {
            ndelc++;
        }
    }

    if (llen == 0) {
        /* No tables had to be resolved, simply return delv and delc */
        *ndelvp = (RDB_ma_vdelete *) delv;
        *gendelpp = NULL;
        return delc;
    }

    /*
     * If deletes have been generated, allocate new list
     * which consists of old and new deletes
     */

    ndelc += llen;

    (*ndelvp) = RDB_alloc(sizeof (RDB_ma_vdelete) * ndelc, ecp);
    if (*ndelvp == NULL) {
        goto error;
    }

    j = 0;
    for (i = 0; i < delc; i++) {
        if (delv[i].tbp->val.tb.exp == NULL) {
            (*ndelvp)[j].tbp = delv[i].tbp;
            (*ndelvp)[j++].objp = delv[i].objp;
        }
    }

    delnp = gendelp;
    while (delnp != NULL) {
        (*ndelvp)[j].tbp = delnp->del.tbp;
        (*ndelvp)[j].objp = delnp->del.objp;

        delnp = delnp->nextp;
        j++;
    }

    *gendelpp = gendelp;
    return ndelc;

error:
    if (gendelp != NULL)
        del_vdellist(gendelp, ecp);

    return RDB_ERROR;
}

/*
 * Convert copy operations to virtual tables to copy operations to real tables.
 * *gencopypp will contain a pointer to the list of generated table inserts or
 * NULL if there were no virtual tables to be resolved.
 */
static int
resolve_copies(int copyc, const RDB_ma_copy *copyv, RDB_ma_copy **ncopyvp,
        copy_node **gencopypp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;
    int ncopyc = 0;
    int llen = 0;

    /* list of generated inserts */
    copy_node *gencopyp = NULL;

    copy_node *copynp;

    for (i = 0; i < copyc; i++) {
        if (copyv[i].dstp->kind == RDB_OB_TABLE) {
            if (RDB_TB_CHECK & copyv[i].dstp->val.tb.flags) {
                if (RDB_check_table(copyv[i].dstp, ecp, txp) != RDB_OK) {
                    goto error;
                }
            }

            if (copyv[i].dstp->val.tb.exp != NULL) {
                if (resolve_copy(copyv[i].dstp, copyv[i].srcp, &copynp, ecp, txp)
                        != RDB_OK)
                    goto error;

                /* Add copies to list */
                concat_copylists(&gencopyp, copynp);
                llen++;
            } else {
                ncopyc++;
            }
        } else {
            ncopyc++;
        }
    }

    if (llen == 0) {
        /* No tables had to be resolved, simply return copyv and copyc */
        *ncopyvp = (RDB_ma_copy *) copyv;
        *gencopypp = NULL;
        return copyc;
    }

    ncopyc += llen;

    /*
     * If copies have been generated, allocate new copy list
     * which consists of old and new copies
     */

    *ncopyvp = RDB_alloc(sizeof (RDB_ma_copy) * ncopyc, ecp);
    if (*ncopyvp == NULL)
        goto error;

    j = 0;
    for (i = 0; i < copyc; i++) {
        if (copyv[i].dstp->val.tb.exp == NULL) {
            (*ncopyvp)[j].dstp = copyv[i].dstp;
            (*ncopyvp)[j++].srcp = copyv[i].srcp;
        }
    }

    copynp = gencopyp;
    while (copynp != NULL) {
        (*ncopyvp)[j].dstp = copynp->cpy.dstp;
        (*ncopyvp)[j].srcp = copynp->cpy.srcp;

        copynp = copynp->nextp;
        j++;
    }
    *gencopypp = gencopyp;
    return ncopyc;

error:
    if (gencopyp != NULL)
        del_copylist(gencopyp, ecp);

    return RDB_ERROR;
}

static RDB_bool
copy_needs_tx(const RDB_object *dstp, const RDB_object *srcp)
{
    return (RDB_bool) ((dstp->kind == RDB_OB_TABLE && RDB_table_is_persistent(dstp))
            || (srcp->kind == RDB_OB_TABLE && RDB_table_is_persistent(srcp)));
}

static int
check_assign_types(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;

    /*
     * Check if conditions are of type BOOLEAN
     */

    for (i = 0; i < updc; i++) {
        if (updv[i].condp != NULL) {
            if (RDB_check_expr_type(updv[i].condp,
                    updv[i].tbp->typ->def.basetyp,
                    &RDB_BOOLEAN, NULL, ecp, txp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }

    for (i = 0; i < delc; i++) {
        if (delv[i].condp != NULL) {
            if (RDB_check_expr_type(delv[i].condp,
                    delv[i].tbp->typ->def.basetyp,
                    &RDB_BOOLEAN, NULL, ecp, txp) != RDB_OK) {
                return RDB_ERROR;
            }
        }
    }

    /*
     * Check types of updated attributes
     */
    for (i = 0; i < updc; i++) {
        for (j = 0; j < updv[i].updc; j++) {
            RDB_attr *attrp = RDB_tuple_type_attr(
                    updv[i].tbp->typ->def.basetyp, updv[i].updv[j].name);
            if (attrp == NULL) {
                RDB_raise_name(updv[i].updv[j].name, ecp);
                return RDB_ERROR;
            }

            if (RDB_type_is_scalar(attrp->typ)) {
                if (RDB_check_expr_type(updv[i].updv[j].exp,
                        updv[i].tbp->typ->def.basetyp, attrp->typ,
                        NULL, ecp, txp) != RDB_OK) {
                    return RDB_ERROR;
                }
            }
        }
    }

    /*
     * Check types of copied values
     */
    for (i = 0; i < copyc; i++) {
        /* If destination carries a value, types must match */
        if (copyv[i].dstp->kind != RDB_OB_INITIAL
                && copyv[i].dstp->typ != NULL) {
            if (!RDB_obj_matches_type(copyv[i].srcp, copyv[i].dstp->typ)) {
                RDB_raise_type_mismatch("source does not match destination",
                        ecp);
                return RDB_ERROR;
            }
        }
    }
    return RDB_OK;
}

/*
 * Check if the same target is assigned twice
 * or if there is a later assignment that depends on a target
 */
static int
check_conflicts_deps(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i, j;

    for (i = 0; i < insc; i++) {
        for (j = i + 1; j < insc; j++) {
            if (insv[i].tbp == insv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < updc; j++) {
            if (insv[i].tbp == updv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (updv[j].condp != NULL
                    && RDB_expr_refers(updv[j].condp, insv[i].tbp)) {
                RDB_raise_not_supported("update condition depends on target", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < delc; j++) {
            if (insv[i].tbp == delv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (delv[j].condp != NULL
                    && RDB_expr_refers(delv[j].condp, insv[i].tbp)) {
                RDB_raise_not_supported("delete condition depends on target", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < vdelc; j++) {
            if (insv[i].tbp == vdelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && insv[i].tbp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }

            /*
             * Check if a presviously modified table is source of a copy
             */
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && RDB_table_refers(copyv[j].srcp, insv[i].tbp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }
    for (i = 0; i < updc; i++) {
        for (j = i + 1; j < updc; j++) {
            if (updv[i].tbp == updv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (updv[j].condp != NULL
                    && RDB_expr_refers(updv[j].condp, updv[i].tbp)) {
                RDB_raise_not_supported("update condition depends on target", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < delc; j++) {
            if (updv[i].tbp == delv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (delv[j].condp != NULL
                    && RDB_expr_refers(delv[j].condp, updv[i].tbp)) {
                RDB_raise_not_supported("delete condition depends on target", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < vdelc; j++) {
            if (updv[i].tbp == vdelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && updv[i].tbp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && RDB_table_refers(copyv[j].srcp, updv[i].tbp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }
    for (i = 0; i < delc; i++) {
        for (j = i + 1; j < delc; j++) {
            if (delv[i].tbp == delv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (delv[j].condp != NULL
                    && RDB_expr_refers(delv[j].condp, delv[i].tbp)) {
                RDB_raise_not_supported("delete condition depends on target", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < vdelc; j++) {
            if (delv[i].tbp == vdelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && delv[i].tbp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && RDB_table_refers(copyv[j].srcp, delv[i].tbp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }

    for (i = 0; i < vdelc; i++) {
        for (j = 0; j < delc; j++) {
            if (vdelv[i].tbp == delv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (delv[j].condp != NULL
                    && RDB_expr_refers(delv[j].condp, vdelv[i].tbp)) {
                RDB_raise_not_supported("delete condition depends on target", ecp);
                return RDB_ERROR;
            }
        }
        for (j = i + 1; j < vdelc; j++) {
            if (vdelv[i].tbp == vdelv[j].tbp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
        }
        for (j = 0; j < copyc; j++) {
            if (copyv[j].dstp->kind == RDB_OB_TABLE
                    && vdelv[i].tbp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && RDB_table_refers(copyv[j].srcp, vdelv[i].tbp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }

    for (i = 0; i < copyc; i++) {
        for (j = i + 1; j < copyc; j++) {
            if (copyv[i].dstp == copyv[j].dstp) {
                RDB_raise_invalid_argument("target is assigned twice", ecp);
                return RDB_ERROR;
            }
            if (copyv[j].srcp->kind == RDB_OB_TABLE
                    && copyv[i].dstp->kind == RDB_OB_TABLE
                    && copyv[j].srcp->kind == RDB_OB_TABLE
                    && RDB_table_refers(copyv[j].srcp, copyv[i].dstp)) {
                RDB_raise_not_supported(
                        "Table is both source and target of assignment", ecp);
                return RDB_ERROR;
            }
        }
    }
    return RDB_OK;
}

static int
eval_constraint(RDB_expression *exp, const char *name,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_bool b;
    int ret = RDB_evaluate_bool(exp, NULL, NULL, NULL, ecp, txp, &b);
    if (ret != RDB_OK) {
        return ret;
    }
    if (!b) {
        RDB_raise_predicate_violation(name, ecp);
        return RDB_ERROR;
    }
    if (RDB_env_trace(RDB_db_env(RDB_tx_db(txp))) > 0) {
        fputs("Constraint check successful.\n", stderr);
    }
    return RDB_OK;
}

/*
 * Check if the assignments violate a constraint
 */
static int
check_assign_constraints(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    return RDB_apply_constraints_i(insc, insv, updc, updv, delc, delv,
            vdelc, vdelv, copyc, copyv, &eval_constraint, ecp, txp);
}

/** @addtogroup generic
 * @{
 */

/*
 * Check if a multiple assignment requires a running transaction.
 */
static RDB_bool
assign_needs_tx(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[])
{
    int i;
    RDB_bool need_tx;

    /* Check if a persistent table is involved */
    for (i = 0; i < delc && !RDB_table_is_persistent(delv[i].tbp); i++);
    need_tx = (RDB_bool) (i < delc);
    if (!need_tx) {
        for (i = 0; i < vdelc && !RDB_table_is_persistent(vdelv[i].tbp); i++);
        need_tx = (RDB_bool) (i < vdelc);
    }
    if (!need_tx) {
        for (i = 0; i < updc && !RDB_table_is_persistent(updv[i].tbp); i++);
        need_tx = (RDB_bool) (i < updc);
    }
    if (!need_tx) {
        for (i = 0; i < copyc && !copy_needs_tx(copyv[i].dstp, copyv[i].srcp); i++);
        need_tx = (RDB_bool) (i < copyc);
    }
    if (!need_tx) {
        for (i = 0;
             i < insc && !RDB_table_is_persistent(insv[i].tbp);
             i++);
        need_tx = (RDB_bool) (i < insc);
    }

    return need_tx;
}

/**
 * Perform a number of insert, update, delete,
and copy operations in a single call.

For each of the RDB_ma_insert elements given by <var>insc</var> and <var>insv</var>,
the tuple or relation *<var>insv</var>[i]->objp is inserted into *<var>insv</var>[i]->tbp.
If *<var>insv</var>[i].flags is zero, inserting a tuple which is already an element of
the table succeeds. If *<var>insv</var>[i].flags is RDB_DISTINCT, such an insert will
result in an element_exists_error.

For each of the RDB_ma_update elements given by <var>updc</var> and <var>updv</var>,
the attributes given by <var>updv</var>[i]->updc and <var>updv</var>[i]->updv
of the tuples of *<var>updv</var>[i]->tbp for which *<var>updv</var>[i]->condp
evaluates to RDB_TRUE are updated.

For each of the RDB_ma_delete elements given by <var>delc</var> and <var>delv</var>,
the tuples for which *<var>delv</var>[i]->condp evaluates to RDB_TRUE
are deleted from *<var>delv</var>[i]->tbp.

For each of the RDB_ma_vdelete elements given by <var>vdelc</var> and <var>vdelv</var>,
the tuples given by *<var>insv</var>[i]->objp (may be a tuple or a table)
are deleted from *<var>vdelv</var>[i]->tbp.

For each of the RDB_ma_copy elements given by <var>copyc</var> and <var>copyv</var>,
*<var>copyv</var>[i]->scrp is copied to *<var>copyv</var>[i]->dstp.

A <strong>RDB_multi_assign</strong> call is atomic with respect to
constraint checking; a constraint violation error can only occur
if the result of <em>all</em> operations violates a constraint.

A table may not appear twice as a target in the arguments to
<strong>RDB_multi_assign()</strong>, and it may not appear
as a source if it appears as a target in a previous assignment.

This means that an assignment like the following:</p>

<code>
UPDATE S WHERE S# = S#('S2') STATUS := 15,
UPDATE S WHERE S# = S#('S3') STATUS := 25;
</code>

(taken from <em>TTM</em>, chapter 6)
cannot be performed directly. It can, however, be converted to an
equivalent form like the following:</p>

<code>
UPDATE S WHERE (S# = S#('S2')) OR (S# = S#('S3'))
        STATUS := IF (S# = S#('S2')) THEN 15 ELSE 25;
</code>

The restrictions of RDB_insert(), RDB_update(), RDB_delete(),
and RDB_copy_obj()
regarding virtual target tables apply to RDB_multi_assign,
too.

<var>txp</var> must point to a running transaction 
if a persistent table is involved.

@returns

On success, the number of tuples inserted, deleted, and updated as specified by the
arguments <var>insc</var>, <var>insv</var>, <var>updc</var>, <var>updv</var>,
<var>delc</var>, <var>delv</var>, <var>vdelc</var> and <var>vdelv</var>,
plus <var>objc</var>. If an error occurred, (RDB_int) RDB_ERROR is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> must point to a running transaction (see above)
but does not.
<dt>invalid_argument_error
<dd>A table appears twice as a target.
<dt>
<dd>A table does not exist. (e.g. after a rollback)
<dt>not_supported_error
<dd>A table is both source and target.
<dd>A virtual table appears as a target in <var>copyv</var>.
<dt>predicate_violation_error
<dd>A constraint has been violated.
<dt>not_found_error
<dd>A tuple given by *<var>vdelv</var>[i]->objp was not found.
</dl>

The errors that can be raised by RDB_insert(),
RDB_update(), RDB_delete() and RDB_copy_obj() can also be raised.
 */
RDB_int
RDB_multi_assign(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_int rcount, cnt;
    int ninsc;
    int nupdc;
    int ndelc;
    int nvdelc;
    int ncopyc;
    RDB_bool need_tx;
    RDB_transaction subtx;
    RDB_transaction *atxp = NULL;
    RDB_ma_insert *ninsv = NULL;
    RDB_ma_update *nupdv = NULL;
    RDB_ma_delete *ndelv = NULL;
    RDB_ma_vdelete *nvdelv = NULL;
    RDB_ma_copy *ncopyv = NULL;
    insert_node *geninsp = NULL;
    update_node *genupdp = NULL;
    delete_node *gendelp = NULL;
    vdelete_node *genvdelp = NULL;
    copy_node *gencopyp = NULL;

    if (check_assign_types(insc, insv, updc, updv, delc, delv,
            copyc, copyv, ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }

    /*
     * Resolve virtual table assignment
     */

    if (insc > 0) {
        ninsc = resolve_inserts(insc, insv, &ninsv, &geninsp, ecp, txp);
        if (ninsc == RDB_ERROR) {
            rcount = RDB_ERROR;
            ninsv = NULL;
            goto cleanup;
        }
    } else {
        ninsc = 0;
        ninsv = NULL;
    }

    if (updc > 0) {
        nupdc = resolve_updates(updc, updv, &nupdv, &genupdp, ecp, txp);
        if (nupdc == RDB_ERROR) {
            rcount = RDB_ERROR;
            nupdv = NULL;
            goto cleanup;
        }
    } else {
            nupdc = 0;
            nupdv = NULL;
    }

    if (delc > 0) {
        ndelc = resolve_deletes(delc, delv, &ndelv, &gendelp, ecp, txp);
        if (ndelc == RDB_ERROR) {
            rcount = RDB_ERROR;
            ndelv = NULL;
            goto cleanup;
        }
    } else {
        ndelc = 0;
        ndelv = NULL;
    }

    if (vdelc > 0) {
        nvdelc = resolve_vdeletes(vdelc, vdelv, &nvdelv, &genvdelp, ecp, txp);
        if (nvdelc == RDB_ERROR) {
            rcount = RDB_ERROR;
            ndelv = NULL;
            goto cleanup;
        }
    } else {
        nvdelc = 0;
        nvdelv = NULL;
    }

    if (copyc > 0) {
        ncopyc = resolve_copies(copyc, copyv, &ncopyv, &gencopyp, ecp, txp);
        if (ncopyc == RDB_ERROR) {
            rcount = RDB_ERROR;
            ncopyv = NULL;
            goto cleanup;
        }
    } else {
        ncopyc = 0;
        ncopyv = NULL;
    }

    /*
     * A running transaction is required if a persistent table is involved.
     */
    need_tx = assign_needs_tx(ninsc, ninsv, nupdc, nupdv, ndelc, ndelv,
            nvdelc, nvdelv, ncopyc, ncopyv);
    if (need_tx && !RDB_tx_is_running(txp)) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    rcount = check_conflicts_deps(ninsc, ninsv, nupdc, nupdv,
            ndelc, ndelv, nvdelc, nvdelv, ncopyc, ncopyv, ecp, txp);
    if (rcount != RDB_OK)
        goto cleanup;

    /*
     * Check constraints
     */

    /* No constraint checking for transient tables */
    if (need_tx) {
        if (check_assign_constraints(ninsc, ninsv, nupdc, nupdv, ndelc, ndelv,
                nvdelc, nvdelv, ncopyc, ncopyv, ecp, txp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
    }

    /*
     * Execute assignments
     */

    /*
     * Start subtransaction, if there is more than one assignment
     * or if the source of an insert or copy is a table.
     * A subtransaction is also needed for an insert into a table
     * with secondary indexes, because the insert is not atomic
     * in Berkeley DB 4.5.
     * A subtx is also needed if a table is inserted.
     * (It is not checked if there is actually more than tuple in it)
     */
    if (need_tx) {
        if ((ninsc + nupdc + ndelc + nvdelc + ncopyc > 1)
                || (ninsc == 1
                    && ((ninsv[0].tbp->val.tb.stp != NULL
                        && ninsv[0].tbp->val.tb.stp->indexc > 1)
                        || ninsv[0].objp->kind == RDB_OB_TABLE))
                || (ncopyc == 1
                        && ncopyv[0].dstp->kind == RDB_OB_TABLE
                        && ((ncopyv[0].dstp->val.tb.stp != NULL
                            && ncopyv[0].dstp->val.tb.stp->indexc > 1)
                            || ncopyv[0].srcp->kind == RDB_OB_TABLE))) {
            if (RDB_begin_tx(ecp, &subtx, RDB_tx_db(txp), txp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            atxp = &subtx;
        } else {
            atxp = txp;
        }
    } else {
        atxp = txp;
    }

    rcount = 0;
    for (i = 0; i < ninsc; i++) {
        if (RDB_TB_CHECK & ninsv[i].tbp->val.tb.flags) {
            if (RDB_check_table(ninsv[i].tbp, ecp, txp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        cnt = do_insert(&ninsv[i], ecp, atxp);
        if (cnt == RDB_ERROR) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        rcount += cnt;
    }
    for (i = 0; i < nupdc; i++) {
        if (RDB_TB_CHECK & nupdv[i].tbp->val.tb.flags) {
            if (RDB_check_table(nupdv[i].tbp, ecp, txp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (nupdv[i].tbp->val.tb.exp == NULL) {
            cnt = do_update(&nupdv[i], ecp, atxp);
            if (cnt == RDB_ERROR) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount += cnt;
        }
    }
    for (i = 0; i < ndelc; i++) {
        if (RDB_TB_CHECK & ndelv[i].tbp->val.tb.flags) {
            if (RDB_check_table(ndelv[i].tbp, ecp, txp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (ndelv[i].tbp->val.tb.exp == NULL) {
            cnt = do_delete(&ndelv[i], ecp, atxp);
            if (cnt == RDB_ERROR) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount += cnt;
        }
    }
    for (i = 0; i < nvdelc; i++) {
        if (RDB_TB_CHECK & nvdelv[i].tbp->val.tb.flags) {
            if (RDB_check_table(nvdelv[i].tbp, ecp, txp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (nvdelv[i].tbp->val.tb.exp == NULL) {
            cnt = do_vdelete(&nvdelv[i], ecp, atxp);
            if (cnt == (RDB_int) RDB_ERROR) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
            rcount += cnt;
        }
    }
    for (i = 0; i < ncopyc; i++) {
        RDB_object *dstp = ncopyv[i].dstp;

        if (dstp->kind == RDB_OB_TABLE
                && (RDB_TB_CHECK & dstp->val.tb.flags)) {
            if (RDB_check_table(dstp, ecp, txp) != RDB_OK) {
                rcount = RDB_ERROR;
                goto cleanup;
            }
        }

        if (copy_obj(dstp, ncopyv[i].srcp, ecp, atxp) != RDB_OK) {
            rcount = RDB_ERROR;
            goto cleanup;
        }
        rcount++;
    }

    /* Commit subtx, if it has been started */
    if (need_tx && (atxp == &subtx)) {
        if (RDB_commit(ecp, &subtx) != RDB_OK)
            rcount = RDB_ERROR;
        atxp = NULL;
    }

cleanup:
    if (ninsv != NULL && ninsv != insv) {
        RDB_free(ninsv);
    }
    if (geninsp != NULL)
        del_inslist(geninsp, ecp);

    if (nupdv != NULL && nupdv != updv) {
        RDB_free(nupdv);
    }
    if (genupdp != NULL)
        del_updlist(genupdp, ecp);

    if (ndelv != NULL && ndelv != delv) {
        RDB_free(ndelv);
    }
    if (gendelp != NULL)
        del_dellist(gendelp, ecp);

    if (nvdelv != NULL && nvdelv != vdelv) {
        RDB_free(nvdelv);
    }
    if (genvdelp != NULL)
        del_vdellist(genvdelp, ecp);

    if (ncopyv != NULL && ncopyv != copyv) {
        RDB_free(ncopyv);
    }
    if (gencopyp != NULL)
        del_copylist(gencopyp, ecp);

    /* Abort subtx, if necessary */
    if (rcount == RDB_ERROR) {
        if (atxp == &subtx) {
            RDB_rollback(ecp, &subtx);
        }
    }

    return rcount;
}

/**
 * RDB_copy_obj copies the value of the RDB_object pointed to
by <var>srcvalp</var> to the RDB_object pointed to by <var>dstvalp</var>.

The source RDB_object must either be newly initialized or of the same type
as the destination.

If both the source and the target are a table, the tuples
are copied from the source to the target. In this case, both tables
must be local, because otherwise a transaction would be required.
RDB_copy_table() or RDB_multi_assign() can be used to copy
global tables.

Currently, RDB_copy_obj is not supported for targets which hold
a virtual table.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>rdb_type_mismatch_error
<dd>*<var>dstvalp</var> is not newly initialized
and its type does not match the type of the RDB_object specified by
<var>srcvalp</var>.
</dl>

The call may also fail for a @ref system-errors "system error".
 */
int
RDB_copy_obj(RDB_object *dstvalp, const RDB_object *srcvalp,
        RDB_exec_context *ecp)
{
    RDB_ma_copy cpy;

    cpy.dstp = dstvalp;
    cpy.srcp = (RDB_object *) srcvalp;

    return RDB_multi_assign(0, NULL, 0, NULL, 0, NULL, 0, NULL, 1, &cpy,
            ecp, NULL) != (RDB_int) RDB_ERROR ? RDB_OK : RDB_ERROR ;
} 

/*@}*/

/**@addtogroup table
 * @{*/

/**
 * RDB_insert inserts the tuple or relation specified by <var>objp</var>
into the table specified by <var>tbp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

Calling RDB_insert on virtual tables is only supported for tables defined using
the following operators: WHERE, project, remove, RENAME, EXTEND, WRAP, and UNWRAP.

@returns

RDB_OK on success, RDB_ERROR if an error occurred.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd>The table given by <var>tbp</var> is global and *<var>txp</var>
is not a running transaction.
<dt>invalid_argument_error
<dd>A table attribute is missing in the tuple and no default value
was specified for that attribute.
<dt>element_exist_error
<dd>The tuple was already an element of the table.
<dt>key_violation_error
<dd>Inserting the tuple would result in a table which contains
a key value twice.
<dt>predicate_violation_error
<dd>Inserting the tuple would result in a table which violates its
predicate.
<dt>type_mismatch_error
<dd>The type of a tuple attribute does not match the type of the
corresponding table attribute.
<dt>operator_not_found_error
<dd>The definition of *<var>tbp</var>
refers to a non-existing operator.
<dt>not_supported_error
<dd>RDB_insert is not supported for this type of table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
int
RDB_insert(RDB_object *tbp, const RDB_object *objp, RDB_exec_context *ecp,
           RDB_transaction *txp)
{
    RDB_ma_insert ins;

    ins.tbp = tbp;
    ins.objp = (RDB_object *) objp;
    ins.flags = RDB_DISTINCT;
    if (RDB_multi_assign(1, &ins, 0, NULL, 0, NULL, 0, NULL, 0, NULL,
            ecp, txp) == (RDB_int) RDB_ERROR)
        return RDB_ERROR;
    return RDB_OK;
}

/**
 * RDB_update updates all tuples from the table specified by <var>tbp</var>
for which the expression specified by <var>exp</var> evaluates to true.
If <var>exp</var> is NULL, all tuples are updated.

If an error occurs, an error value is left in *<var>ecp</var>.

The attributes to be updated are specified by the <var>updv</var> array.
The attribute specified by the field name is set to the value
obtained by evaluating the expression specified by the field exp.

Currently, RDB_update is not supported for virtual tables except PROJECT.

@returns

The number of updated tuples in real tables if the call was successful.
A call which did not modify any tuple because no tuple matched the condition
is considered a successful call and returns zero.
If an error occurred, (RDB_int)RDB_ERROR is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.

<dt>name_error
<dd>One of the attributes in <var>updv</var> does not exist in the table.
<dd>One of the expressions specified in <var>updv</var> refers to an attribute
which does not exist in the table.

<dt>element_exist_error
<dd>The update operation would update a tuple so that it would be equal
to a tuple which is already an element of the table.

<dt>key_violation_error
<dd>The update operation would result in a table which contains
a key value twice.

<dt>predicate_violation_error
<dd>The update operation would result in a table which violates its
predicate.

<dt>type_mismatch_error
<dd>The type of one of the expressions in <var>updv</var> is not the same
as the type of the corresponding table attribute.

<dt>operator_not_found_error
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.
<dd>The expression specified by <var>exp</var>
refers to a non-existing operator.
<dd>One of the expressions specified in <var>updv</var>
refers to a non-existing operator.

<dt>not_supported_error
<dd>RDB_update is not supported for this type of table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
RDB_int
RDB_update(RDB_object *tbp, RDB_expression *condp, int updc,
           const RDB_attr_update updv[], RDB_exec_context *ecp,
           RDB_transaction *txp)
{
    RDB_ma_update upd;

    upd.tbp = tbp;
    upd.condp = condp;
    upd.updc = updc;
    upd.updv = (RDB_attr_update *) updv;
    return RDB_multi_assign(0, NULL, 1, &upd, 0, NULL, 0, NULL, 0, NULL,
            ecp, txp);
}

/**
 * RDB_delete deletes all tuples from the table specified by <var>tbp</var>
for which the expression specified by <var>exp</var> evaluates to true.
If <var>exp</var> is NULL, all tuples are deleted.

If an error occurs, an error value is left in *<var>ecp</var>.

Currently, RDB_delete is not supported for virtual tables except
PROJECT, WHERE, RENAME, and EXTEND.

@returns

The number of deleted tuples, if no error occurred.
A call which did not delete any tuple because no tuple matched the condition
is considered a successful call and returns zero.
If an error occurred, (RDB_int)RDB_ERROR is returned.

@par Errors:

<dl>
<dt>no_running_tx_error
<dd><var>txp</var> does not point to a running transaction.
<dt>invalid_argument_error
<dd><var>exp</var> refers to an attribute which does not exist in the table.
<dt>predicate_violation_error
<dd>Deleting the tuples would result in a table which violates its
predicate.
<dt>operator_not_found_error</dt>
<dd>The definition of the table specified by <var>tbp</var>
refers to a non-existing operator.</dd>
<dd>The expression specified by <var>exp</var> refers to a non-existing operator.</dd>
<dt>not_supported_error
<dd>RDB_delete is not supported for this type of table.
</dl>

The call may also fail for a @ref system-errors "system error",
in which case the transaction may be implicitly rolled back.
 */
RDB_int
RDB_delete(RDB_object *tbp, RDB_expression *condp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_ma_delete del;

    del.tbp = tbp;
    del.condp = condp;
    return RDB_multi_assign(0, NULL, 0, NULL, 1, &del, 0, NULL, 0, NULL,
            ecp, txp);
}

/*@}*/

/**@addtogroup constr
 * @{*/

/**
 * Get expressions that would have been evaluated
 * by an invocation of RDB_multi_assign() and invoke *applyfn
 * with the optimized expression as argument.
 * Stop if the invocation returns a value different from RDB_OK.
 */
int
RDB_apply_constraints(int insc, const RDB_ma_insert insv[],
        int updc, const RDB_ma_update updv[],
        int delc, const RDB_ma_delete delv[],
        int vdelc, const RDB_ma_vdelete vdelv[],
        int copyc, const RDB_ma_copy copyv[],
        RDB_apply_constraint_fn *applyfnp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int ninsc;
    int nupdc;
    int ndelc;
    int nvdelc;
    RDB_ma_insert *ninsv = NULL;
    RDB_ma_update *nupdv = NULL;
    RDB_ma_delete *ndelv = NULL;
    RDB_ma_vdelete *nvdelv = NULL;
    insert_node *geninsp = NULL;
    update_node *genupdp = NULL;
    delete_node *gendelp = NULL;
    vdelete_node *genvdelp = NULL;

    if (check_assign_types(insc, insv, updc, updv, delc, delv,
            copyc, copyv, ecp, txp) != RDB_OK) {
        return RDB_ERROR;
    }

    /*
     * Resolve virtual table assignments
     */

    if (insc > 0) {
        ninsc = resolve_inserts(insc, insv, &ninsv, &geninsp, ecp, txp);
        if (ninsc == RDB_ERROR) {
            ret = RDB_ERROR;
            ninsv = NULL;
            goto cleanup;
        }
    } else {
        ninsc = 0;
        ninsv = NULL;
    }

    if (updc > 0) {
        nupdc = resolve_updates(updc, updv, &nupdv, &genupdp, ecp, txp);
        if (nupdc == RDB_ERROR) {
            ret = RDB_ERROR;
            nupdv = NULL;
            goto cleanup;
        }
    } else {
        nupdc = 0;
        nupdv = NULL;
    }

    if (delc > 0) {
        ndelc = resolve_deletes(delc, delv, &ndelv, &gendelp, ecp, txp);
        if (ndelc == RDB_ERROR) {
            ret = RDB_ERROR;
            ndelv = NULL;
            goto cleanup;
        }
    } else {
        ndelc = 0;
        ndelv = NULL;
    }

    if (vdelc > 0) {
        nvdelc = resolve_vdeletes(vdelc, vdelv, &nvdelv, &genvdelp, ecp, txp);
        if (ndelc == RDB_ERROR) {
            ret = RDB_ERROR;
            nvdelv = NULL;
            goto cleanup;
        }
    } else {
        nvdelc = 0;
        nvdelv = NULL;
    }

    ret = check_conflicts_deps(ninsc, ninsv, nupdc, nupdv,
            ndelc, ndelv, nvdelc, nvdelv, copyc, copyv, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /*
     * Check constraints
     */

    ret = RDB_apply_constraints_i(ninsc, ninsv,
            nupdc, nupdv, ndelc, ndelv, nvdelc, nvdelv,
            copyc, copyv, applyfnp, ecp, txp);

cleanup:
    /*
     * Free generated inserts, updates, deletes
     */
    if (ninsv != NULL && ninsv != insv) {
        RDB_free(ninsv);
    }
    if (geninsp != NULL)
        del_inslist(geninsp, ecp);

    if (nupdv != NULL && nupdv != updv) {
        RDB_free(nupdv);
    }
    if (genupdp != NULL)
        del_updlist(genupdp, ecp);

    if (ndelv != NULL && ndelv != delv) {
        RDB_free(ndelv);
    }
    if (gendelp != NULL)
        del_dellist(gendelp, ecp);

    if (nvdelv != NULL && nvdelv != vdelv) {
        RDB_free(nvdelv);
    }
    if (genvdelp != NULL)
        del_vdellist(genvdelp, ecp);

    return ret;
}

/*@}*/
