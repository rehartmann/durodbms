/*
 * $Id$
 *
 * Copyright (C) 2004-2006 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 *
 * Functions for virtual tables
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include <gen/strfns.h>
#include <string.h>

/*
 * Turn *tbp into a virtual table defined by exp.
 */
int
_RDB_vtexp_to_obj(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tbp)
{
    /* Create type */
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, ecp, txp);
    if (tbtyp == NULL) {
        return RDB_ERROR;
    }

    tbtyp = _RDB_dup_nonscalar_type(tbtyp, ecp);
    if (tbtyp == NULL)
        return RDB_ERROR;

    if (_RDB_init_table(tbp, NULL, RDB_FALSE, 
            tbtyp, 0, NULL, RDB_TRUE, exp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

RDB_object *
RDB_expr_to_vtable(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *tbp;

    if (!RDB_tx_is_running(txp)) {
        RDB_raise_invalid_tx(ecp);
        return NULL;
    }

    tbp = _RDB_new_obj(ecp);
    if (tbp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (_RDB_vtexp_to_obj(exp, ecp, txp, tbp) != RDB_OK) {
        _RDB_free_obj(tbp, ecp);
        return NULL;
    }

    return tbp;
}

/*
 * Determine which keys are preserved by the projection.
 *
 * exp      a PROJECT expression
 * presv    output - a boolean vector indicating which keys are preserved.
 *          May be NULL.
 */
int
_RDB_check_project_keyloss(RDB_expression *exp,
        int keyc, RDB_string_vec *keyv, RDB_bool presv[],
        RDB_exec_context *ecp)
{
    int i, j, k;
    int count = 0;
    RDB_bool pres;

    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].strc; j++) {
            /* Search for key attribute in projection attrs */
            for (k = 0;
                 (k < exp->var.op.argc - 1)
                        && (strcmp(keyv[i].strv[j],
                            RDB_obj_string(&exp->var.op.argv[k + 1]->var.obj)) != 0);
                 k++);
            /* If not found, exit loop */
            if (k >= exp->var.op.argc - 1)
                break;
        }
        /* If the loop didn't terminate prematurely, the key is preserved */
        pres = (RDB_bool) (j >= keyv[i].strc);

        if (presv != NULL) {
            presv[i] = pres;
        }
        if (pres)
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
    RDB_bool free1, free2;

    keyc1 = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv1, &free1);
    if (keyc1 < 0)
        return keyc1;
    keyc2 = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv2, &free2);
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
    if (free1)
        _RDB_free_keys(keyc1, keyv1);
    if (free2)
        _RDB_free_keys(keyc2, keyv2);
    return newkeyc;

error:
    if (newkeyv != NULL) {
        for (i = 0; i < newkeyc; i++) {
            if (newkeyv[i].strv != NULL)
                RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
        }
    }
    if (free1)
        _RDB_free_keys(keyc1, keyv1);
    if (free2)
        _RDB_free_keys(keyc2, keyv2);
    RDB_raise_no_memory(ecp);
    return RDB_ERROR;
}

static int
infer_project_keys(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_string_vec **keyvp, RDB_bool *caller_must_freep)
{
    int keyc;
    int newkeyc;
    RDB_string_vec *keyv;
    RDB_string_vec *newkeyv;
    RDB_bool *presv;
    RDB_bool freekeys;

    keyc = _RDB_infer_keys(exp->var.op.argv[0], ecp, &keyv, &freekeys);
    if (keyc < 0)
        return keyc;

    presv = malloc(sizeof(RDB_bool) * keyc);
    if (presv == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    newkeyc = _RDB_check_project_keyloss(exp, keyc, keyv, presv, ecp);
    if (newkeyc == 0) {
        /* Table is all-key */
        newkeyc = 1;
        newkeyv = all_key(exp, ecp);
        if (newkeyv == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    } else if (newkeyc == keyc) {
        /* The keys remained intact */
        *caller_must_freep = freekeys;
        *keyvp = keyv;
        return keyc;
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
    if (freekeys)
        _RDB_free_keys(keyc, keyv);
    *keyvp = newkeyv;
    *caller_must_freep = RDB_TRUE;
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
                RDB_obj_string(&exp->var.op.argv[exp->var.op.argc - 1]->var.obj)) != 0) {
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

char *
_RDB_rename_attr(const char *srcname, RDB_expression *exp)
{
    /* Search for attribute in rename arguments */
    int i = 1;
    while (i < exp->var.op.argc
            && strcmp(RDB_obj_string(&exp->var.op.argv[i]->var.obj), srcname) != 0) {
        i += 2;
    }
    if (i < exp->var.op.argc) {
        /* Found - return new attribute name */
        return RDB_obj_string(&exp->var.op.argv[i + 1]->var.obj);
    }
    return NULL;   
}

RDB_string_vec *
_RDB_dup_rename_keys(int keyc, const RDB_string_vec keyv[], RDB_expression *texp)
{
    int i, j;

    RDB_string_vec *nkeyv = malloc(sizeof(RDB_attr) * keyc);
    if (keyv == NULL) {
        return NULL;
    }
    for (i = 0; i < keyc; i++) {
        nkeyv[i].strv = NULL;
    }
    for (i = 0; i < keyc; i++) {
        nkeyv[i].strc = keyv[i].strc;
        nkeyv[i].strv = malloc(sizeof(char *) * keyv[i].strc);
        if (nkeyv[i].strv == NULL)
            goto error;
        for (j = 0; j < keyv[i].strc; j++)
            nkeyv[i].strv[j] = NULL;
        for (j = 0; j < keyv[i].strc; j++) {
            char *nattrname = NULL;
            
            if (texp != NULL) {
                /* If exp is not NULL, rename attributes */
                nattrname = _RDB_rename_attr(keyv[i].strv[j], texp);
            }

            if (nattrname != NULL) {
                nkeyv[i].strv[j] = RDB_dup_str(nattrname);
            } else {
                nkeyv[i].strv[j] = RDB_dup_str(keyv[i].strv[j]);
            }
            if (nkeyv[i].strv[j] == NULL) {
                goto error;
            }
        }
    }
    return nkeyv;

error:
    for (i = 0; i < keyc; i++) {
        if (nkeyv[i].strv != NULL) {
            for (j = 0; j < nkeyv[i].strc; j++)
                free(nkeyv[i].strv[j]);
        }
    }
    free(nkeyv);
    return NULL;
}

int
_RDB_infer_keys(RDB_expression *exp, RDB_exec_context *ecp,
       RDB_string_vec **keyvp, RDB_bool *caller_must_freep)
{
    switch (exp->kind) {
        case RDB_EX_OBJ:
            *caller_must_freep = RDB_FALSE;
            return RDB_table_keys(&exp->var.obj, ecp, keyvp);
        case RDB_EX_TBP:
            *caller_must_freep = RDB_FALSE;
            return RDB_table_keys(exp->var.tbref.tbp, ecp, keyvp);
        case RDB_EX_RO_OP:
            break;
        case RDB_EX_VAR:
        case RDB_EX_TUPLE_ATTR:
        case RDB_EX_GET_COMP:
            RDB_raise_invalid_argument("Expression is not a table", ecp);
            return RDB_ERROR;            
    }

    if ((strcmp(exp->var.op.name, "WHERE") == 0)
            || (strcmp(exp->var.op.name, "MINUS") == 0)
            || (strcmp(exp->var.op.name, "SEMIMINUS") == 0)
            || (strcmp(exp->var.op.name, "SEMIJOIN") == 0)
            || (strcmp(exp->var.op.name, "INTERSECT") == 0)
            || (strcmp(exp->var.op.name, "EXTEND") == 0)
            || (strcmp(exp->var.op.name, "SDIVIDE") == 0)) {
        return _RDB_infer_keys(exp->var.op.argv[0], ecp, keyvp,
                caller_must_freep);
    }
    if (strcmp(exp->var.op.name, "JOIN") == 0) {
        *caller_must_freep = RDB_TRUE;
    	return infer_join_keys(exp, ecp, keyvp);
    }
    if (strcmp(exp->var.op.name, "PROJECT") == 0) {
    	return infer_project_keys(exp, ecp, keyvp, caller_must_freep);
    }
    if (strcmp(exp->var.op.name, "SUMMARIZE") == 0) {
        return _RDB_infer_keys(exp->var.op.argv[1], ecp, keyvp,
                caller_must_freep);
    }
    if (strcmp(exp->var.op.name, "RENAME") == 0) {
        RDB_bool freekey;
        int keyc = _RDB_infer_keys(exp->var.op.argv[0], ecp, keyvp, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;

        *keyvp = _RDB_dup_rename_keys(keyc, *keyvp, exp);
        if (*keyvp == NULL) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        if (freekey) {
            _RDB_free_keys(keyc, *keyvp);
        }
        return keyc;
    }
    if (strcmp(exp->var.op.name, "GROUP") == 0) {
        *caller_must_freep = RDB_TRUE;
        return infer_group_keys(exp, ecp, keyvp);
    }

    /*
     * For all other relational operators, assume all-key table
     */
    *keyvp = all_key(exp, ecp);
    if (*keyvp == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    *caller_must_freep = RDB_TRUE;
    return 1;
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
                    RDB_type_attr_type(tbtyp, indexp->attrv[i].attrname), NULL);
            if (attrexp == NULL) {
                /* !! */
                return NULL;
            }

        objpv[i] = &attrexp->var.op.argv[1]->var.obj;       
    }
    return objpv;
}
