/*
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "rdb.h"
#include "internal.h"

int
matching_ts(RDB_table *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_object tpl;
    RDB_qresult *qrp;

    qrp = _RDB_table_qresult(tbp, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        if (_RDB_tuple_matches(tplp, &tpl, ecp, txp, resultp) != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            _RDB_drop_qresult(qrp, ecp, txp);
            return RDB_ERROR;
        }
        if (*resultp) {
            RDB_destroy_obj(&tpl, ecp);
            _RDB_drop_qresult(qrp, ecp, txp);
            return RDB_OK;
        }
    }
    RDB_destroy_obj(&tpl, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
    } else {
        _RDB_drop_qresult(qrp, ecp, txp);
        return RDB_ERROR;
    }
    *resultp = RDB_FALSE;
    return _RDB_drop_qresult(qrp, ecp, txp);  
}

static RDB_bool
index_covers_tuple(_RDB_tbindex *indexp, const RDB_object *tplp)
{
    int i;

    for (i = 0; i < indexp->attrc; i++) {
        if (RDB_tuple_get(tplp, indexp->attrv[i].attrname) == NULL)
            return RDB_FALSE;
    }
    return RDB_TRUE;
}

static int
stored_matching(RDB_table *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int i;
    _RDB_tbindex *indexp;
    int ret;
    RDB_object tpl;
    RDB_object **objpv;

    if (tbp->stp == NULL) {
        /* Physical table representation has not been created, so table is empty */
        *resultp = RDB_FALSE;
        return RDB_OK;
    }

    /*
     * Check if the tuple fully covers the table
     */
     

    /*
     * Search for a unique index that covers the tuple
     */
    for (i = 0;
            i < tbp->stp->indexc
                    && !(tbp->stp->indexv[i].unique
                        && index_covers_tuple(&tbp->stp->indexv[i], tplp));
            i++);
    if (i >= tbp->stp->indexc) {
        return matching_ts(tbp, tplp, ecp, txp, resultp);
    }
    indexp = &tbp->stp->indexv[i];
    objpv = malloc(sizeof(RDB_object *) * indexp->attrc);
    if (objpv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    for (i = 0; i < indexp->attrc; i++) {
        objpv[i] = RDB_tuple_get(tplp, indexp->attrv[i].attrname);
    }
    RDB_init_obj(&tpl);
    ret = _RDB_get_by_uindex(tbp, objpv, indexp, tbp->typ->var.basetyp, ecp,
            txp, &tpl);
    if (ret == DB_NOTFOUND) {
        *resultp = RDB_FALSE;
        ret = RDB_OK;
        goto cleanup;
    }
    if (ret != RDB_OK) {
        goto cleanup;
    }
    if (indexp->attrc < tbp->typ->var.basetyp->var.tuple.attrc) {
        ret = RDB_obj_equals(tplp, &tpl, ecp, txp, resultp);
    } else {
        *resultp = RDB_TRUE;
    }

cleanup:
    free(objpv);
    RDB_destroy_obj(&tpl, ecp);
    return ret;   
}

static int
project_matching(RDB_table *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int i;
    int ret;
    RDB_object tpl;
    RDB_type *tpltyp = tbp->var.project.tbp->typ->var.basetyp;

    /*
     * Pick attributes which are attributes of the table
     */
    RDB_init_obj(&tpl);
    for (i = 0; i < tpltyp->var.tuple.attrc; i++) {
        char *attrname = tpltyp->var.tuple.attrv[i].name;
        RDB_object *attrp = RDB_tuple_get(tplp, attrname);
        if (attrp != NULL) {
            if (RDB_tuple_set(&tpl, attrname, attrp, ecp) != RDB_OK) {
                RDB_destroy_obj(&tpl, ecp);
                return RDB_ERROR;
            }
        }
    }
    ret = _RDB_matching_tuple(tbp->var.project.tbp, &tpl, ecp, txp, resultp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&tpl, ecp);
        return RDB_ERROR;
    }
    return RDB_destroy_obj(&tpl, ecp);
}

int
_RDB_matching_tuple(RDB_table *tbp, const RDB_object *tplp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    switch (tbp->kind) {
        case RDB_TB_REAL:
            return stored_matching(tbp, tplp, ecp, txp, resultp);
       case RDB_TB_PROJECT:
            return project_matching(tbp, tplp, ecp, txp, resultp);
       case RDB_TB_SELECT:
       case RDB_TB_UNION:
       case RDB_TB_SEMIMINUS:
       case RDB_TB_SEMIJOIN:
       case RDB_TB_JOIN:
       case RDB_TB_EXTEND:
       case RDB_TB_SUMMARIZE:
       case RDB_TB_RENAME:
       case RDB_TB_WRAP:
       case RDB_TB_UNWRAP:
       case RDB_TB_SDIVIDE:
       case RDB_TB_GROUP:
       case RDB_TB_UNGROUP:
           return matching_ts(tbp, tplp, ecp, txp, resultp);
    }
    abort();
}
