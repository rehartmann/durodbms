/*
 * $Id$
 *
 * Copyright (C) 2004-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/*
 * Functions for virtual tables
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

RDB_object *
RDB_expr_to_vtable(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_type *tbtyp;
    RDB_object *tbp = _RDB_new_obj(ecp);
    if (tbp == NULL)
        return NULL;

    /* Create type */
    tbtyp = RDB_expr_type(exp, NULL, ecp, txp);
    if (tbtyp == NULL) {
        _RDB_free_obj(tbp, ecp);
        return NULL;
    }

    if (_RDB_init_table(tbp, NULL, RDB_FALSE, 
            tbtyp, 0, NULL, RDB_TRUE, exp, ecp) != RDB_OK) {
        RDB_drop_type(tbtyp, ecp, NULL);
        _RDB_free_obj(tbp, ecp);
        return NULL;
    }
    return tbp;
}

RDB_bool
_RDB_table_def_equals(RDB_object *tb1p, RDB_object *tb2p, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
	int ret;
	RDB_bool res;
	
    if (tb1p == tb2p)
        return RDB_TRUE;

    if (tb1p->var.tb.exp == NULL || tb2p->var.tb.exp == NULL)
        return RDB_FALSE;

    ret = _RDB_expr_equals(tb1p->var.tb.exp,
                    tb2p->var.tb.exp, ecp, txp, &res);
    if (ret != RDB_OK)
       return RDB_FALSE;
    return res;
}

static int
check_keyloss(RDB_expression *exp, int attrc, RDB_attr attrv[], RDB_bool presv[],
        RDB_exec_context *ecp)
{
    int i, j, k;
    int count = 0;
    RDB_string_vec *keyv;
    int keyc = _RDB_infer_keys(exp, ecp, &keyv); /* !! memory? */
    if (keyc == RDB_ERROR)
        return RDB_ERROR;

    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].strc; j++) {
            /* Search for key attribute in attrv */
            for (k = 0;
                 (k < attrc) && (strcmp(keyv[i].strv[j], attrv[k].name) != 0);
                 k++);
            /* If not found, exit loop */
            if (k >= attrc)
                break;
        }
        /* If the loop didn't terminate prematurely, the key is preserved */
        presv[i] = (RDB_bool) (j >= keyv[i].strc);
        if (presv[i])
            count++;
    }
    return count;
}

static RDB_string_vec *
all_key(RDB_expression *exp, RDB_exec_context *ecp)
{
    int attrc;
    int i;
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, ecp, NULL);
    RDB_string_vec *keyv = malloc(sizeof (RDB_string_vec));
    if (keyv == NULL)
        return NULL;

    attrc = keyv[0].strc =
            tbtyp->var.basetyp->var.tuple.attrc;
    keyv[0].strv = malloc(sizeof(char *) * attrc);
    if (keyv[0].strv == NULL) {
        free(keyv);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        keyv[0].strv[i] = NULL;
    for (i = 0; i < attrc; i++) {
        keyv[0].strv[i] = RDB_dup_str(
                tbtyp->var.basetyp->var.tuple.attrv[i].name);
        if (keyv[0].strv[i] == NULL) {
            goto error;
        }
    }

    return keyv;
error:
    RDB_free_strvec(keyv[0].strc, keyv[0].strv);
    free(keyv);
    return NULL;
}

static int
infer_join_keys(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_string_vec **keyvp)
{
    int i, j, k;
    int keyc1, keyc2;
    int newkeyc;
    RDB_string_vec *keyv1, *keyv2;
    RDB_string_vec *newkeyv;

    keyc1 = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv1);
    if (keyc1 < 0)
        return keyc1;
    keyc2 = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv2);
    if (keyc2 < 0)
        return keyc2;

    newkeyc = keyc1 * keyc2;
    newkeyv = malloc(sizeof (RDB_string_vec) * newkeyc);
    if (newkeyv == NULL)
        goto error;
    for (i = 0; i < keyc1; i++) {
        for (j = 0; j < keyc2; j++) {
            RDB_string_vec *attrsp = &newkeyv[i * keyc2 + j];

            attrsp->strc = keyv1[i].strc + keyv2[j].strc;
            attrsp->strv = malloc(sizeof(char *) * attrsp->strc);
            if (attrsp->strv == NULL)
                goto error;
            for (k = 0; k < attrsp->strc; k++)
                attrsp->strv[k] = NULL;
            for (k = 0; k < keyv1[i].strc; k++) {
                attrsp->strv[k] = RDB_dup_str(keyv1[i].strv[k]);
                if (attrsp->strv[k] == NULL)
                    goto error;
            }
            for (k = 0; k < keyv2[j].strc; k++) {
                attrsp->strv[keyv1[i].strc + k] =
                        RDB_dup_str(keyv2[j].strv[k]);
                if (attrsp->strv[keyv1[i].strc + k] == NULL)
                    goto error;
            }
        }
    }
    *keyvp = newkeyv;
    return newkeyc;

error:
    if (newkeyv != NULL) {
        for (i = 0; i < newkeyc; i++) {
            if (newkeyv[i].strv != NULL)
                RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
        }
    }
    RDB_raise_no_memory(ecp);
    return RDB_ERROR;
}

static int
infer_project_keys(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_string_vec **keyvp)
{
    int keyc;
    int newkeyc;
    RDB_string_vec *keyv;
    RDB_string_vec *newkeyv;
    RDB_bool *presv;
    RDB_bool keyloss;
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, ecp, NULL);
    
    keyc = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv);
    if (keyc < 0)
        return keyc;

    presv = malloc(sizeof(RDB_bool) * keyc);
    if (presv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    newkeyc = check_keyloss(exp->var.op.argv[0],
            tbtyp->var.basetyp->var.tuple.attrc,
            tbtyp->var.basetyp->var.tuple.attrv, presv, ecp);
    keyloss = (RDB_bool) (newkeyc == 0);
    if (keyloss) {
        /* Table is all-key */
        newkeyc = 1;
        newkeyv = all_key(exp, ecp);
        if (newkeyv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    } else {
        int i, j;

        /* Pick the keys which survived the projection */
        newkeyv = malloc(sizeof (RDB_string_vec) * newkeyc);
        if (newkeyv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }

        for (i = 0; i < newkeyc; i++) {
            newkeyv[i].strv = NULL;
        }

        for (j = i = 0; j < keyc; j++) {
            if (presv[j]) {
                newkeyv[i].strc = keyv[j].strc;
                newkeyv[i].strv = RDB_dup_strvec(keyv[j].strc,
                        keyv[j].strv);
                if (newkeyv[i].strv == NULL) {
                    RDB_raise_no_memory(ecp);
                    return RDB_ERROR;
                }
                i++;
            }
        }
    }
    free(presv);
    *keyvp = newkeyv;
    return keyc;
}

static int
infer_group_keys(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_string_vec **keyvp)
{
    int i, j;
    RDB_string_vec *newkeyv;
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, ecp, NULL);

    /*
     * Key consists of all attributes which are not grouped
     */    
    newkeyv = malloc(sizeof(RDB_string_vec));
    if (newkeyv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    newkeyv[0].strc = tbtyp->var.basetyp->var.tuple.attrc - 1;
    newkeyv[0].strv = malloc(sizeof (char *) * newkeyv[0].strc);
    if (newkeyv[0].strv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    j = 0;
    for (i = 0; i < tbtyp->var.basetyp->var.tuple.attrc; i++) {
        if (strcmp(tbtyp->var.basetyp->var.tuple.attrv[i].name,
                RDB_obj_string(&exp->var.op.argv[1]->var.obj)) != 0) {
            newkeyv[0].strv[j] = RDB_dup_str(
                    tbtyp->var.basetyp->var.tuple.attrv[i].name);
            if (newkeyv[0].strv[j] == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            j++;
        }
    }

    *keyvp = newkeyv;
    return 1;
}

int
_RDB_infer_keys(RDB_expression *exp, RDB_exec_context *ecp,
       RDB_string_vec **keyvp)
{
    if ((strcmp(exp->var.op.name, "WHERE") == 0)
            || (strcmp(exp->var.op.name, "SEMIMINUS") == 0)
            || (strcmp(exp->var.op.name, "SEMIJOIN") == 0)
            || (strcmp(exp->var.op.name, "EXTEND") == 0)
            || (strcmp(exp->var.op.name, "SDIVIDE") == 0)) {
        return _RDB_infer_keys(exp->var.op.argv[0], ecp, keyvp);
    }
    if ((strcmp(exp->var.op.name, "UNION") == 0)
            || (strcmp(exp->var.op.name, "WRAP") == 0)
            || (strcmp(exp->var.op.name, "UNWRAP") == 0)
            || (strcmp(exp->var.op.name, "UNGROUP") == 0)) {
        *keyvp = all_key(exp, ecp);
        if (*keyvp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        return 1;
    }
    if (strcmp(exp->var.op.name, "JOIN") == 0) {
    	return infer_join_keys(exp, ecp, keyvp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
    	return infer_project_keys(exp, ecp, keyvp);
    }
    if (strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        return _RDB_infer_keys(exp->var.op.argv[1], ecp, keyvp);
    }
    if (strcmp(exp->var.op.name, "RENAME") == 0) {
        int keyc = _RDB_infer_keys(exp->var.op.argv[0], ecp, keyvp);
        /* !!
            newkeyv = dup_rename_keys(keyc, keyv, tbp->var.rename.renc,
                    tbp->var.rename.renv);
            if (newkeyv == NULL) {
                RDB_raise_no_memory(ecp);
                return RDB_ERROR;
            }
            *keyvp = newkeyv;
        */
        return keyc;
    }
    if (strcmp(exp->var.op.name, "GROUP") == 0) {
        return infer_group_keys(exp, ecp, keyvp);
    }
    /* Must never be reached */
    abort();
}

RDB_bool
_RDB_table_refers(const RDB_object *srctbp, const RDB_object *dsttbp)
{
    if (srctbp == dsttbp)
        return RDB_TRUE;
	if (srctbp->var.tb.exp == NULL)
	    return RDB_FALSE;
	return _RDB_expr_refers(srctbp->var.tb.exp, dsttbp);
}

RDB_object **
_RDB_index_objpv(_RDB_tbindex *indexp, RDB_expression *exp, RDB_type *tbtyp,
        int objpc, RDB_bool all_eq, RDB_bool asc)
{
    int i;
    RDB_expression *nodep;
    RDB_expression *attrexp;

    RDB_object **objpv = malloc(sizeof (RDB_object *) * objpc);
    if (objpv == NULL)
        return NULL;
    for (i = 0; i < objpc; i++) {
        nodep = _RDB_attr_node(exp, indexp->attrv[i].attrname, "=");
        if (nodep == NULL && !all_eq) {
            if (asc) {
                nodep = _RDB_attr_node(exp,
                        indexp->attrv[i].attrname, ">=");
                if (nodep == NULL)
                    nodep = _RDB_attr_node(exp, indexp->attrv[i].attrname, ">");
            } else {
                nodep = _RDB_attr_node(exp,
                        indexp->attrv[i].attrname, "<=");
                if (nodep == NULL)
                    nodep = _RDB_attr_node(exp, indexp->attrv[i].attrname, "<");
            }
        }
        attrexp = nodep;
        if (attrexp->kind == RDB_EX_RO_OP
                && strcmp (attrexp->var.op.name, "AND") == 0)
            attrexp = attrexp->var.op.argv[1];
        if (attrexp->var.op.argv[1]->var.obj.typ == NULL
                && (attrexp->var.op.argv[1]->var.obj.kind == RDB_OB_TUPLE
                || attrexp->var.op.argv[1]->var.obj.kind == RDB_OB_ARRAY))
            attrexp->var.op.argv[1]->var.obj.typ = _RDB_dup_nonscalar_type(
                    RDB_type_attr_type(tbtyp, indexp->attrv[i].attrname), NULL /*!!*/);

        objpv[i] = &attrexp->var.op.argv[1]->var.obj;       
    }
    return objpv;
}
