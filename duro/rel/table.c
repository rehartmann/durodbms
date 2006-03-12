/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "catalog.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>
#include <dli/tabletostr.h>
#include <stdio.h>
#include <assert.h>

void
_RDB_print_table(RDB_table *tbp, RDB_transaction *txp, FILE *fp,
        RDB_exec_context *ecp)
{
    RDB_object obj;

    RDB_init_obj(&obj);
    _RDB_table_to_str(&obj, tbp, ecp, txp, RDB_SHOW_INDEX);
    fputs(RDB_obj_string(&obj), fp);
    RDB_destroy_obj(&obj, ecp);
}

RDB_table *
_RDB_new_table(RDB_exec_context *ecp)
{
    RDB_table *tbp = malloc(sizeof (RDB_table));
    if (tbp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    tbp->name = NULL;
    tbp->keyv = NULL;
    tbp->stp = NULL;
    return tbp;
}

/*
 * Creates a stored table, but not the recmap and the indexes
 * and does not insert the table into the catalog.
 * reltyp is consumed on success (must not be freed by caller).
 */
static RDB_table *
new_rtable(const char *name, RDB_bool persistent,
           RDB_type *reltyp,
           int keyc, const RDB_string_vec keyv[], RDB_bool usr,
           RDB_exec_context *ecp)
{
    int i;
    RDB_table *tbp = _RDB_new_table(ecp);
    if (tbp == NULL)
        return NULL;

    tbp->is_user = usr;
    tbp->is_persistent = persistent;

    tbp->kind = RDB_TB_REAL;

    if (name != NULL) {
        tbp->name = RDB_dup_str(name);
        if (tbp->name == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }

    /* copy candidate keys */
    tbp->keyc = keyc;
    tbp->keyv = malloc(sizeof(RDB_attr) * keyc);
    if (tbp->keyv == NULL) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].strv = NULL;
    }
    for (i = 0; i < keyc; i++) {
        tbp->keyv[i].strc = keyv[i].strc;
        tbp->keyv[i].strv = RDB_dup_strvec(keyv[i].strc, keyv[i].strv);
        if (tbp->keyv[i].strv == NULL) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }

    tbp->typ = reltyp;

    return tbp;

error:
    /* clean up */
    if (tbp != NULL) {
        free(tbp->name);
        for (i = 0; i < keyc; i++) {
            if (tbp->keyv[i].strv != NULL) {
                RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
            }
        }
        free(tbp->keyv);
        free(tbp);
    }
    return NULL;
}

/*
 * Like new_rtable(), but uses attrc and heading instead of reltype.
 */
RDB_table *
_RDB_new_rtable(const char *name, RDB_bool persistent,
                int attrc, const RDB_attr heading[],
                int keyc, const RDB_string_vec keyv[], RDB_bool usr,
                RDB_exec_context *ecp)
{
    int ret;
    int i;
    RDB_table *tbp;
    RDB_type *reltyp = RDB_create_relation_type(attrc, heading, ecp);
    if (reltyp == NULL) {
        return NULL;
    }

    for (i = 0; i < attrc; i++) {
        if (heading[i].defaultp != NULL) {
            RDB_type *tuptyp = reltyp->var.basetyp;

            tuptyp->var.tuple.attrv[i].defaultp = malloc(sizeof (RDB_object));
            if (tuptyp->var.tuple.attrv[i].defaultp == NULL) {
                RDB_drop_type(reltyp, ecp, NULL);
                RDB_raise_no_memory(ecp);
                return NULL;
            }
            RDB_init_obj(tuptyp->var.tuple.attrv[i].defaultp);
            ret = RDB_copy_obj(tuptyp->var.tuple.attrv[i].defaultp,
                    heading[i].defaultp, ecp);
            if (ret != RDB_OK) {
                RDB_drop_type(reltyp, ecp, NULL);
                return NULL;
            }                
        }
    }

    tbp = new_rtable(name, persistent, reltyp, keyc, keyv, usr, ecp);
    if (tbp == NULL) {
        RDB_drop_type(reltyp, ecp, NULL);
        return NULL;
    }
    return tbp;
}

static int
drop_anon_table(RDB_table *tbp, RDB_exec_context *ecp)
{
    assert((tbp->kind >= RDB_TB_REAL) && (tbp->kind <= RDB_TB_UNGROUP));

    if (RDB_table_name(tbp) == NULL)
        return RDB_drop_table(tbp, ecp, NULL);
    return RDB_OK;
}

void
_RDB_free_table(RDB_table *tbp, RDB_exec_context *ecp)
{
    int i;

    if (tbp->keyv != NULL) {
        /* Delete candidate keys */
        for (i = 0; i < tbp->keyc; i++) {
            RDB_free_strvec(tbp->keyv[i].strc, tbp->keyv[i].strv);
        }
        free(tbp->keyv);
    }

    RDB_drop_type(tbp->typ, NULL, NULL);
    free(tbp->name);
    tbp->kind = -1; /* !! For debugging */
    free(tbp);
}

int
RDB_table_keys(RDB_table *tbp, RDB_exec_context *ecp, RDB_string_vec **keyvp)
{
    if (tbp->keyv == NULL) {
        if (_RDB_infer_keys(tbp, ecp) != RDB_OK)
            return RDB_ERROR;
    }

    if (keyvp != NULL)
        *keyvp = tbp->keyv;
        
    return tbp->keyc;
}

int
RDB_drop_table_index(const char *name, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    int xi;
    char *tbname;
    RDB_table *tbp;

    if (!_RDB_legal_name(name)) {
        RDB_raise_not_found("invalid index name", ecp);
        return RDB_ERROR;
    }

    ret = _RDB_cat_index_tablename(name, &tbname, ecp, txp);
    if (ret != RDB_OK)
        return RDB_ERROR;

    tbp = RDB_get_table(tbname, ecp, txp);
    free(tbname);
    if (tbp == NULL)
        return RDB_ERROR;

    for (i = 0; i < tbp->stp->indexc
            && strcmp(tbp->stp->indexv[i].name, name) != 0;
            i++);
    if (i >= tbp->stp->indexc) {
        /* Index not found, so reread indexes */
        for (i = 0; i < tbp->stp->indexc; i++)
            _RDB_free_tbindex(&tbp->stp->indexv[i]);
        free(tbp->stp->indexv);
        ret = _RDB_cat_get_indexes(tbp->name, txp->dbp->dbrootp, ecp, txp,
                &tbp->stp->indexv);
        if (ret != RDB_OK)
            return RDB_ERROR;

        /* Search again */
        for (i = 0; i < tbp->stp->indexc
                && strcmp(tbp->stp->indexv[i].name, name) != 0;
                i++);
        if (i >= tbp->stp->indexc) {
            RDB_raise_internal("invalid index", ecp);
            return RDB_ERROR;
        }
    }        
    xi = i;

    /* Destroy index */
    ret = _RDB_del_index(txp, tbp->stp->indexv[i].idxp, ecp);
    if (ret != RDB_OK)
        return ret;

    /* Delete index from catalog */
    ret = _RDB_cat_delete_index(name, ecp, txp);
    if (ret != RDB_OK)
        return ret;

    /*
     * Delete index entry
     */

    _RDB_free_tbindex(&tbp->stp->indexv[xi]);

    tbp->stp->indexc--;
    for (i = xi; i < tbp->stp->indexc; i++) {
        tbp->stp->indexv[i] = tbp->stp->indexv[i + 1];
    }

    realloc(tbp->stp->indexv,
            sizeof(_RDB_tbindex) * tbp->stp->indexc);

    return RDB_OK;
}

int
_RDB_drop_table(RDB_table *tbp, RDB_bool rec, RDB_exec_context *ecp)
{
    int i;
    int ret;

    switch (tbp->kind) {
        case RDB_TB_REAL:
            break;
        case RDB_TB_SELECT:
            if (tbp->var.select.objpc > 0)
                free(tbp->var.select.objpv);
            RDB_drop_expr(tbp->var.select.exp, ecp);
            if (rec) {
                ret = drop_anon_table(tbp->var.select.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_UNION:
            if (rec) {
                ret = drop_anon_table(tbp->var._union.tb1p, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var._union.tb2p, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_SEMIMINUS:
            if (rec) {
                ret = drop_anon_table(tbp->var.semiminus.tb1p, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.semiminus.tb2p, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_SEMIJOIN:
            if (rec) {
                ret = drop_anon_table(tbp->var.semijoin.tb1p, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.semijoin.tb2p, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_JOIN:
            if (rec) {
                ret = drop_anon_table(tbp->var.join.tb1p, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.join.tb2p, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_EXTEND:
            if (rec) {
                ret = drop_anon_table(tbp->var.extend.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.extend.attrc; i++) {
                free(tbp->var.extend.attrv[i].name);
                RDB_drop_expr(tbp->var.extend.attrv[i].exp, ecp);
            }
            break;
        case RDB_TB_PROJECT:
            if (rec) {
                ret = drop_anon_table(tbp->var.project.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_RENAME:
            if (rec) {
                ret = drop_anon_table(tbp->var.rename.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.rename.renc; i++) {
                free(tbp->var.rename.renv[i].from);
                free(tbp->var.rename.renv[i].to);
            }
            break;
        case RDB_TB_SUMMARIZE:
            if (rec) {
                ret = drop_anon_table(tbp->var.summarize.tb1p, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.summarize.tb2p, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.summarize.addc; i++) {
                if (tbp->var.summarize.addv[i].op != RDB_COUNT
                        && tbp->var.summarize.addv[i].op != RDB_COUNTD)
                    RDB_drop_expr(tbp->var.summarize.addv[i].exp, ecp);
                free(tbp->var.summarize.addv[i].name);
            }
            break;
        case RDB_TB_WRAP:
            if (rec) {
                ret = drop_anon_table(tbp->var.wrap.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.wrap.wrapc; i++) {
                free(tbp->var.wrap.wrapv[i].attrname);
                RDB_free_strvec(tbp->var.wrap.wrapv[i].attrc,
                        tbp->var.wrap.wrapv[i].attrv);
            }
            break;
        case RDB_TB_UNWRAP:
            if (rec) {
                ret = drop_anon_table(tbp->var.unwrap.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            RDB_free_strvec(tbp->var.unwrap.attrc, tbp->var.unwrap.attrv);
            break;
        case RDB_TB_SDIVIDE:
            if (rec) {
                ret = drop_anon_table(tbp->var.sdivide.tb1p, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.sdivide.tb2p, ecp);
                if (ret != RDB_OK)
                    return ret;
                ret = drop_anon_table(tbp->var.sdivide.tb3p, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            break;
        case RDB_TB_GROUP:
            if (rec) {
                ret = drop_anon_table(tbp->var.group.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            for (i = 0; i < tbp->var.group.attrc; i++) {
                free(tbp->var.group.attrv[i]);
            }
            free(tbp->var.group.gattr);
            break;
        case RDB_TB_UNGROUP:
            if (rec) {
                ret = drop_anon_table(tbp->var.ungroup.tbp, ecp);
                if (ret != RDB_OK)
                    return ret;
            }
            free(tbp->var.ungroup.attr);
            break;
        default:
            abort();
    }
    _RDB_free_table(tbp, ecp);
    return RDB_OK;
}

/*
 * Return the type of the table tbp.
 */
RDB_type *
RDB_table_type(const RDB_table *tbp)
{
    return tbp->typ;
}

/*
 * Copy all tuples from source table into the destination table.
 * The destination table must be a real table.
 */
int
_RDB_move_tuples(RDB_table *dstp, RDB_table *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_qresult *qrp = NULL;
    RDB_object tpl;
    int ret;

    /*
     * Copy all tuples from source table to destination table
     */
    qrp = _RDB_table_qresult(srcp, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        if (dstp->kind == RDB_TB_REAL && !dstp->is_persistent)
            ret = _RDB_insert_real(dstp, &tpl, ecp, NULL);
        else
            ret = _RDB_insert_real(dstp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            goto cleanup;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
        RDB_clear_err(ecp);
        ret = RDB_OK;
    }

cleanup:
    _RDB_drop_qresult(qrp, ecp, txp);
    RDB_destroy_obj(&tpl, ecp);
    return ret;
}

int
RDB_copy_table(RDB_table *dstp, RDB_table *srcp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_ma_copy cpy;
    RDB_object srcobj, dstobj;

    RDB_init_obj(&dstobj);
    RDB_table_to_obj(&dstobj, dstp, ecp);
    RDB_init_obj(&srcobj);
    RDB_table_to_obj(&srcobj, srcp, ecp);

    cpy.dstp = &dstobj;
    cpy.srcp = &srcobj;

    ret = RDB_multi_assign(0, NULL, 0, NULL, 0, NULL, 1, &cpy, ecp, txp);

    /*
     * Destroy the objects, but not the tables
     */

    dstobj.var.tbp = NULL;
    srcobj.var.tbp = NULL;
    RDB_destroy_obj(&dstobj, ecp);
    RDB_destroy_obj(&srcobj, ecp);

    return ret;
}

int
RDB_all(RDB_table *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    /* initialize result */
    *resultp = RDB_TRUE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (!RDB_tuple_get_bool(tplp, attrname))
            *resultp = RDB_FALSE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_any(RDB_table *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    /* initialize result */
    *resultp = RDB_FALSE;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (RDB_tuple_get_bool(tplp, attrname))
            *resultp = RDB_TRUE;
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_max(RDB_table *tbp, const char *attrname, RDB_exec_context *ecp,
       RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MIN;
    else if (attrtyp == &RDB_FLOAT)
        resultp->var.float_val = RDB_FLOAT_MIN;
    else if (attrtyp == &RDB_DOUBLE)
        resultp->var.double_val = RDB_DOUBLE_MIN;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(tplp, attrname);
             
            if (val > resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_double val = RDB_tuple_get_double(tplp, attrname);
             
            if (val > resultp->var.double_val)
                resultp->var.double_val = val;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_min(RDB_table *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = RDB_INT_MIN;
    else if (attrtyp == &RDB_FLOAT)
        resultp->var.double_val = RDB_FLOAT_MIN;
    else if (attrtyp == &RDB_DOUBLE)
        resultp->var.double_val = RDB_DOUBLE_MIN;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER) {
            RDB_int val = RDB_tuple_get_int(tplp, attrname);
             
            if (val < resultp->var.int_val)
                 resultp->var.int_val = val;
        } else {
            RDB_double val = RDB_tuple_get_double(tplp, attrname);
             
            if (val < resultp->var.double_val)
                resultp->var.double_val = val;
        }
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_sum(RDB_table *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;

    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    _RDB_set_obj_type(resultp, attrtyp);

    /* initialize result */
    if (attrtyp == &RDB_INTEGER)
        resultp->var.int_val = 0;
    else if (attrtyp == &RDB_FLOAT)
        resultp->var.float_val = 0.0;
    else if (attrtyp == &RDB_DOUBLE)
        resultp->var.double_val = 0.0;
    else {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        if (attrtyp == &RDB_INTEGER)
            resultp->var.int_val += RDB_tuple_get_int(tplp, attrname);
        else
            resultp->var.double_val
                            += RDB_tuple_get_double(tplp, attrname);
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);

    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_avg(RDB_table *tbp, const char *attrname, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_double *resultp)
{
    RDB_type *attrtyp;
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;
    unsigned long count;

    /* attrname may only be NULL if table is unary */
    if (attrname == NULL) {
        if (tbp->typ->var.basetyp->var.tuple.attrc != 1) {
            RDB_raise_invalid_argument("attribute name is required", ecp);
            return RDB_ERROR;
        }
        attrname = tbp->typ->var.basetyp->var.tuple.attrv[0].name;
    }

    attrtyp = _RDB_tuple_type_attr(tbp->typ->var.basetyp, attrname)->typ;
    if (attrtyp == NULL) {
        RDB_raise_attribute_not_found(attrname, ecp);
        return RDB_ERROR;
    }

    if (!RDB_type_is_numeric(attrtyp)) {
        RDB_raise_type_mismatch("argument must be numeric", ecp);
        return RDB_ERROR;
    }
    count = 0;

    /*
     * Perform aggregation
     */

    RDB_init_obj(&arr);

    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&arr, ecp);
        return ret;
    }

    i = 0;
    while ((tplp = RDB_array_get(&arr, (RDB_int) i++, ecp)) != NULL) {
        count++;
        if (attrtyp == &RDB_INTEGER)
            *resultp += RDB_tuple_get_int(tplp, attrname);
        else
            *resultp += RDB_tuple_get_double(tplp, attrname);
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);

    if (count == 0) {
        RDB_raise_aggregate_undefined(ecp);
        return RDB_ERROR;
    }
    *resultp /= count;

    return RDB_destroy_obj(&arr, ecp);
}

int
RDB_extract_tuple(RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tplp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_table *ntbp;
    RDB_type *errtyp;

    ret = _RDB_optimize(tbp, 0, NULL, ecp, txp, &ntbp);
    if (ret != RDB_OK)
        return ret;

    qrp = _RDB_table_qresult(ntbp, ecp, txp);
    if (qrp == NULL) {
        if (ntbp->kind != RDB_TB_REAL)
            RDB_drop_table(ntbp, ecp, txp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    /* Get tuple */
    ret = _RDB_next_tuple(qrp, tplp, ecp, txp);
    if (ret != RDB_OK)
        goto cleanup;

    /* Check if there are more tuples */
    for(;;) {
        RDB_bool is_equal;
    
        ret = _RDB_next_tuple(qrp, &tpl, ecp, txp);
        if (ret != RDB_OK) {
            errtyp = RDB_obj_type(RDB_get_err(ecp));
            if (errtyp == &RDB_NOT_FOUND_ERROR) {
                RDB_clear_err(ecp);
                ret = RDB_OK;
            }
            break;
        }

        ret = _RDB_tuple_equals(tplp, &tpl, ecp, txp, &is_equal);
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

    _RDB_drop_qresult(qrp, ecp, txp);
    if (ntbp->kind != RDB_TB_REAL) {
        RDB_drop_table(ntbp, ecp, txp);
    }
    return RDB_get_err(ecp) == NULL ? RDB_OK : RDB_ERROR;
}

int
RDB_table_is_empty(RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    int ret;
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_table *ptbp;
    RDB_table *ntbp;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    /*
     * Project all attributes away, then optimize
     */

    ptbp = RDB_project(tbp, 0, NULL, ecp);
    if (ptbp == NULL) {
        RDB_raise_no_memory(ecp);
    }

    ret = _RDB_optimize(ptbp, 0, NULL, ecp, txp, &ntbp);

    /* Remove projection */
    _RDB_free_table(ptbp, ecp);

    if (ret != RDB_OK)
        return ret;

    qrp = _RDB_table_qresult(ntbp, ecp, txp);
    if (qrp == NULL) {
        RDB_drop_table(ntbp, ecp, txp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    /*
     * Read first tuple
     */
    if (_RDB_next_tuple(qrp, &tpl, ecp, txp) != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
            RDB_destroy_obj(&tpl, ecp);
            _RDB_drop_qresult(qrp, ecp, txp);
            RDB_drop_table(ntbp, ecp, txp);
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);
        *resultp = RDB_TRUE;
    } else {
        *resultp = RDB_FALSE;
    }

    RDB_destroy_obj(&tpl, ecp);
    ret = _RDB_drop_qresult(qrp, ecp, txp);
    if (ret != RDB_OK) {
        return ret;
    }
    return RDB_drop_table(ntbp, ecp, txp);
}

RDB_int
RDB_cardinality(RDB_table *tbp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_int count;
    RDB_qresult *qrp;
    RDB_object tpl;
    RDB_table *ntbp;

    if (txp != NULL && !RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    ret = _RDB_optimize(tbp, 0, NULL, ecp, txp, &ntbp);
    if (ret != RDB_OK)
        return ret;

    qrp = _RDB_table_qresult(ntbp, ecp, txp);
    if (qrp == NULL) {
        if (ntbp->kind != RDB_TB_REAL)
            RDB_drop_table(ntbp, ecp, txp);
        return RDB_ERROR;
    }

    /* Duplicates must be removed */
    ret = _RDB_duprem(qrp, ecp);
    if (ret != RDB_OK) {
        if (ntbp->kind != RDB_TB_REAL)
            RDB_drop_table(ntbp, ecp, txp);
        _RDB_drop_qresult(qrp, ecp, txp);
        return ret;
    }

    RDB_init_obj(&tpl);

    count = 0;
    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        count++;
    }
    RDB_destroy_obj(&tpl, ecp);
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        _RDB_drop_qresult(qrp, ecp, txp);
        goto error;
    }
    RDB_clear_err(ecp);

    ret = _RDB_drop_qresult(qrp, ecp, txp);
    if (ret != RDB_OK)
        goto error;

    if (ntbp->kind != RDB_TB_REAL) {
        ret = RDB_drop_table(ntbp, ecp, txp);
        if (ret != RDB_OK)
            return ret;
    }

    if (tbp->stp != NULL)
        tbp->stp->est_cardinality = count;

    return count;

error:
    if (ntbp->kind != RDB_TB_REAL)
        RDB_drop_table(ntbp, ecp, txp);
    return RDB_ERROR;
}

int
RDB_subset(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_bool *resultp)
{
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;

    if (!RDB_type_equals(tb1p->typ, tb2p->typ)) {
        RDB_raise_type_mismatch("argument types must be equal", ecp);
        return RDB_ERROR;
    }

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return RDB_ERROR;
    }

    qrp = _RDB_table_qresult(tb1p, ecp, txp);
    if (qrp == NULL) {
        return RDB_ERROR;
    }

    RDB_init_obj(&tpl);

    *resultp = RDB_TRUE;
    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, ecp, txp, resultp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            _RDB_drop_qresult(qrp, ecp, txp);
            goto error;
        }
        if (!*resultp) {
            break;
        }
    }

    RDB_destroy_obj(&tpl, ecp);
    if (ret != RDB_OK) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
            _RDB_drop_qresult(qrp, ecp, txp);
            goto error;
        }
        RDB_clear_err(ecp);
    }
    ret = _RDB_drop_qresult(qrp, ecp, txp);
    if (ret != RDB_OK)
        goto error;
    return RDB_OK;

error:
    return RDB_ERROR;
}

int
RDB_table_equals(RDB_table *tb1p, RDB_table *tb2p, RDB_exec_context *ecp,
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

    ret =  RDB_cardinality(tb2p, ecp, txp);
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
    qrp = _RDB_table_qresult(tb1p, ecp, txp);
    if (qrp == NULL)
        return RDB_ERROR;

    RDB_init_obj(&tpl);
    while ((ret = _RDB_next_tuple(qrp, &tpl, ecp, txp)) == RDB_OK) {
        ret = RDB_table_contains(tb2p, &tpl, ecp, txp, resp);
        if (ret != RDB_OK) {
            goto error;
        }
        if (!*resp) {
            RDB_destroy_obj(&tpl, ecp);
            return _RDB_drop_qresult(qrp, ecp, txp);
        }
    }

    *resp = RDB_TRUE;
    RDB_destroy_obj(&tpl, ecp);
    return _RDB_drop_qresult(qrp, ecp, txp);

error:
    RDB_destroy_obj(&tpl, ecp);
    _RDB_drop_qresult(qrp, ecp, txp);
    return ret;
}

/*
 * If the tuples are sorted by an ordered index when read using
 * a table qresult, return the index, otherwise NULL.
 */
struct _RDB_tbindex *
_RDB_sortindex (RDB_table *tbp)
{
    switch (tbp->kind) {
        case RDB_TB_REAL:
            return NULL;
        case RDB_TB_SELECT:
            return _RDB_sortindex(tbp->var.select.tbp);
        case RDB_TB_UNION:
            return NULL;
        case RDB_TB_SEMIMINUS:
            return _RDB_sortindex(tbp->var.semiminus.tb1p);
        case RDB_TB_SEMIJOIN:
            return _RDB_sortindex(tbp->var.semijoin.tb1p);
        case RDB_TB_JOIN:
            return _RDB_sortindex(tbp->var.join.tb1p);
        case RDB_TB_EXTEND:
            return _RDB_sortindex(tbp->var.extend.tbp);
        case RDB_TB_PROJECT:
            return tbp->var.project.indexp;
        case RDB_TB_RENAME:
            return NULL; /* !! */
        case RDB_TB_SUMMARIZE:
            return NULL; /* !! */
        case RDB_TB_WRAP:
            return NULL; /* !! */
        case RDB_TB_UNWRAP:
            return NULL; /* !! */
        case RDB_TB_SDIVIDE:
            return _RDB_sortindex(tbp->var.sdivide.tb1p);
        case RDB_TB_GROUP:
            return NULL; /* !! */
        case RDB_TB_UNGROUP:
            return NULL; /* !! */
    }
    abort();
}
