/*
 * Functions for virtual tables
 *
 * Copyright (C) 2004-2009, 2011-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "stable.h"
#include <gen/strfns.h>
#include <obj/key.h>
#include <obj/objinternal.h>

#include <string.h>

/*
 * Turn *tbp into a virtual table defined by exp.
 */
int
RDB_vtexp_to_obj(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp, RDB_object *tbp)
{
    /* Create type */
    RDB_type *tbtyp = RDB_expr_type(exp, NULL, NULL, NULL, ecp, txp);
    if (tbtyp == NULL) {
        return RDB_ERROR;
    }

    if (tbtyp->kind != RDB_TP_RELATION) {
        RDB_raise_type_mismatch("relation type required", ecp);
        return RDB_ERROR;
    }

    tbtyp = RDB_dup_nonscalar_type(tbtyp, ecp);
    if (tbtyp == NULL)
        return RDB_ERROR;

    /* Preserve table name */
    if (RDB_init_table_i(tbp, NULL, RDB_FALSE, 
            tbtyp, 0, NULL, 0, NULL, RDB_TRUE, exp, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
}

/** @addtogroup table
 * @{
 */

/**
 * RDB_expr_to_vtable creates a virtual table from the expression *<var>exp</var>.

If an error occurs, an error value is left in *<var>ecp</var>.

@returns

A pointer to the newly created table, or NULL if an error occurred.

@par Errors:

<dl>
<dt>invalid_argument_error
<dd>*<var>exp</var> does not define a valid virtual table.
<dt>name_error
<dd>*<var>exp</var> refers to an undefined attribute.
<dt>type_mismatch_error
<dd>*<var>exp</var> contains an operator invocation with an argument
of a wrong type.
<dt>operator_not_found_error
<dd>*<var>exp</var> contains an invocation of a non-existing operator.
</dl>
 */
RDB_object *
RDB_expr_to_vtable(RDB_expression *exp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object *tbp;

    tbp = RDB_new_obj(ecp);
    if (tbp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }

    if (RDB_vtexp_to_obj(exp, ecp, txp, tbp) != RDB_OK) {
        RDB_free_obj(tbp, ecp);
        return NULL;
    }

    return tbp;
}

/*@}*/

/**
 * Determine which keys are preserved by the projection.
 *
 * exp      a PROJECT expression
 * presv    output - a boolean vector indicating which keys are preserved.
 *          May be NULL.
 */
int
RDB_check_project_keyloss(RDB_expression *exp,
        int keyc, RDB_string_vec *keyv, RDB_bool presv[],
        RDB_exec_context *ecp)
{
    int i, j;
    int count = 0;
    RDB_bool pres;

    for (i = 0; i < keyc; i++) {
        for (j = 0; j < keyv[i].strc; j++) {
            /* Search for key attribute in projection attrs */
            RDB_expression *argp = exp->def.op.args.firstp->nextp;
            while (argp != NULL
                    && (strcmp(keyv[i].strv[j], RDB_obj_string(&argp->def.obj)) != 0))
                argp = argp->nextp;
            /* If not found, exit loop */
            if (argp == NULL)
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
all_key(RDB_expression *exp, RDB_environment *envp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int attrc;
    int i;
    RDB_string_vec *keyv;
    RDB_type *tbtyp = RDB_expr_type_tpltyp(exp, NULL, NULL, NULL, envp,
            ecp, txp);
    if (tbtyp == NULL)
        return NULL;

    keyv = RDB_alloc(sizeof (RDB_string_vec), ecp);
    if (keyv == NULL)
        return NULL;

    attrc = keyv[0].strc =
            tbtyp->def.basetyp->def.tuple.attrc;
    keyv[0].strv = RDB_alloc(sizeof(char *) * attrc, ecp);
    if (keyv[0].strv == NULL) {
        RDB_free(keyv);
        return NULL;
    }
    for (i = 0; i < attrc; i++)
        keyv[0].strv[i] = NULL;
    for (i = 0; i < attrc; i++) {
        keyv[0].strv[i] = RDB_dup_str(
                tbtyp->def.basetyp->def.tuple.attrv[i].name);
        if (keyv[0].strv[i] == NULL) {
            goto error;
        }
    }

    return keyv;
error:
    RDB_free_strvec(keyv[0].strc, keyv[0].strv);
    RDB_free(keyv);
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

    keyc1 = RDB_infer_keys(exp->def.op.args.firstp, NULL, NULL, NULL, ecp, NULL,
            &keyv1, &free1);
    if (keyc1 < 0)
        return keyc1;
    keyc2 = RDB_infer_keys(exp->def.op.args.firstp, NULL, NULL, NULL, ecp, NULL,
            &keyv2, &free2);
    if (keyc2 < 0)
        return keyc2;

    newkeyc = keyc1 * keyc2;
    newkeyv = RDB_alloc(sizeof (RDB_string_vec) * newkeyc, ecp);
    if (newkeyv == NULL)
        goto error;
    for (i = 0; i < keyc1; i++) {
        for (j = 0; j < keyc2; j++) {
            RDB_string_vec *attrsp = &newkeyv[i * keyc2 + j];

            attrsp->strc = keyv1[i].strc + keyv2[j].strc;
            attrsp->strv = RDB_alloc(sizeof(char *) * attrsp->strc, ecp);
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
        RDB_free_keys(keyc1, keyv1);
    if (free2)
        RDB_free_keys(keyc2, keyv2);
    return newkeyc;

error:
    if (newkeyv != NULL) {
        for (i = 0; i < newkeyc; i++) {
            if (newkeyv[i].strv != NULL)
                RDB_free_strvec(newkeyv[i].strc, newkeyv[i].strv);
        }
    }
    if (free1)
        RDB_free_keys(keyc1, keyv1);
    if (free2)
        RDB_free_keys(keyc2, keyv2);
    return RDB_ERROR;
}

static int
infer_project_keys(RDB_expression *exp, RDB_environment *envp,
        RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_string_vec **keyvp, RDB_bool *caller_must_freep)
{
    int keyc;
    int newkeyc;
    RDB_string_vec *keyv;
    RDB_string_vec *newkeyv;
    RDB_bool *presv;
    RDB_bool freekeys;

    keyc = RDB_infer_keys(exp->def.op.args.firstp, NULL, NULL, envp, ecp, txp,
            &keyv, &freekeys);
    if (keyc < 0)
        return keyc;

    presv = RDB_alloc(sizeof(RDB_bool) * keyc, ecp);
    if (presv == NULL) {
        return RDB_ERROR;
    }
    newkeyc = RDB_check_project_keyloss(exp, keyc, keyv, presv, ecp);
    if (newkeyc == 0) {
        /* Table is all-key */
        newkeyc = 1;
        newkeyv = all_key(exp, envp, ecp, txp);
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
        newkeyv = RDB_alloc(sizeof (RDB_string_vec) * newkeyc, ecp);
        if (newkeyv == NULL) {
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
    RDB_free(presv);
    if (freekeys)
        RDB_free_keys(keyc, keyv);
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
    RDB_type *tbtyp = RDB_expr_type_tpltyp(exp, NULL, NULL, NULL, NULL,
            ecp, NULL);

    /*
     * Key consists of all attributes which are not grouped
     */    
    newkeyv = RDB_alloc(sizeof(RDB_string_vec), ecp);
    if (newkeyv == NULL) {
        return RDB_ERROR;
    }
    newkeyv[0].strc = tbtyp->def.basetyp->def.tuple.attrc - 1;
    newkeyv[0].strv = RDB_alloc(sizeof (char *) * newkeyv[0].strc, ecp);
    if (newkeyv[0].strv == NULL) {
        return RDB_ERROR;
    }

    j = 0;
    for (i = 0; i < tbtyp->def.basetyp->def.tuple.attrc; i++) {
        if (strcmp(tbtyp->def.basetyp->def.tuple.attrv[i].name,
                RDB_obj_string(&exp->def.op.args.lastp->def.obj)) != 0) {
            newkeyv[0].strv[j] = RDB_dup_str(
                    tbtyp->def.basetyp->def.tuple.attrv[i].name);
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
RDB_rename_attr(const char *srcname, const RDB_expression *exp)
{
    /* Search for attribute in rename arguments */
    RDB_expression *argp = exp->def.op.args.firstp->nextp;
    while (argp != NULL  && strcmp(RDB_obj_string(&argp->def.obj), srcname) != 0) {
        argp = argp->nextp->nextp;
    }
    if (argp != NULL) {
        /* Found - return new attribute name */
        return RDB_obj_string(&argp->nextp->def.obj);
    }
    return NULL;   
}

RDB_string_vec *
RDB_dup_rename_keys(int keyc, const RDB_string_vec keyv[], RDB_expression *texp,
        RDB_exec_context *ecp)
{
    int i, j;

    RDB_string_vec *nkeyv = RDB_alloc(sizeof(RDB_attr) * keyc, ecp);
    if (keyv == NULL) {
        return NULL;
    }
    for (i = 0; i < keyc; i++) {
        nkeyv[i].strv = NULL;
    }
    for (i = 0; i < keyc; i++) {
        nkeyv[i].strc = keyv[i].strc;
        nkeyv[i].strv = RDB_alloc(sizeof(char *) * keyv[i].strc, ecp);
        if (nkeyv[i].strv == NULL)
            goto error;
        for (j = 0; j < keyv[i].strc; j++)
            nkeyv[i].strv[j] = NULL;
        for (j = 0; j < keyv[i].strc; j++) {
            char *nattrname = NULL;

            if (texp != NULL) {
                /* If exp is not NULL, rename attributes */
                nattrname = RDB_rename_attr(keyv[i].strv[j], texp);
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
                RDB_free(nkeyv[i].strv[j]);
        }
    }
    RDB_free(nkeyv);
    return NULL;
}

/*
 * Infers keys from a relational expression.
 *
 * If *caller_must_freep is RDB_TRUE, the caller is responsible
 * for freeing the inferred keys.
 */
int
RDB_infer_keys(RDB_expression *exp, RDB_getobjfn *getfnp, void *getdata,
       RDB_environment *envp, RDB_exec_context *ecp, RDB_transaction *txp,
       RDB_string_vec **keyvp, RDB_bool *caller_must_freep)
{
    switch (exp->kind) {
    case RDB_EX_OBJ:
        *caller_must_freep = RDB_FALSE;
        return RDB_table_keys(&exp->def.obj, ecp, keyvp);
    case RDB_EX_TBP:
        *caller_must_freep = RDB_FALSE;
        return RDB_table_keys(exp->def.tbref.tbp, ecp, keyvp);
    case RDB_EX_RO_OP:
        break;
    case RDB_EX_VAR:
        /* Try to resolve variable via getfnp */
        if (getfnp != NULL) {
            RDB_object *srcp = (*getfnp)(exp->def.varname, getdata);
            if (srcp != NULL) {
                *caller_must_freep = RDB_FALSE;
                return RDB_table_keys(srcp, ecp, keyvp);
            }
        }

        /* Try to get table */
        if (txp != NULL) {
            RDB_object *srcp = RDB_get_table(exp->def.varname, ecp, txp);
            if (srcp != NULL) {
                *caller_must_freep = RDB_FALSE;
                return RDB_table_keys(srcp, ecp, keyvp);
            }
        }

        /* Not found */
        RDB_raise_name(exp->def.varname, ecp);
        return RDB_ERROR;
    case RDB_EX_GET_COMP:
        RDB_raise_invalid_argument("Expression is not a table", ecp);
        return RDB_ERROR;
    }

    if ((strcmp(exp->def.op.name, "where") == 0)
            || (strcmp(exp->def.op.name, "minus") == 0)
            || (strcmp(exp->def.op.name, "semiminus") == 0)
            || (strcmp(exp->def.op.name, "semijoin") == 0)
            || (strcmp(exp->def.op.name, "intersect") == 0)
            || (strcmp(exp->def.op.name, "extend") == 0)
            || (strcmp(exp->def.op.name, "divide") == 0)) {
        return RDB_infer_keys(exp->def.op.args.firstp, NULL, NULL,
                envp, ecp, txp, keyvp, caller_must_freep);
    }
    if (strcmp(exp->def.op.name, "join") == 0) {
        *caller_must_freep = RDB_TRUE;
    	return infer_join_keys(exp, ecp, keyvp);
    }
    if (strcmp(exp->def.op.name, "project") == 0) {
    	return infer_project_keys(exp, envp, ecp, txp, keyvp, caller_must_freep);
    }
    if (strcmp(exp->def.op.name, "summarize") == 0) {
        return RDB_infer_keys(exp->def.op.args.firstp->nextp, NULL, NULL,
                envp, ecp, txp, keyvp, caller_must_freep);
    }
    if (strcmp(exp->def.op.name, "rename") == 0) {
        RDB_bool freekey;
        int keyc = RDB_infer_keys(exp->def.op.args.firstp, NULL, NULL,
                envp, ecp, txp, keyvp, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;

        *keyvp = RDB_dup_rename_keys(keyc, *keyvp, exp, ecp);
        if (*keyvp == NULL) {
            return RDB_ERROR;
        }
        if (freekey) {
            RDB_free_keys(keyc, *keyvp);
        }
        *caller_must_freep = RDB_TRUE;
        return keyc;
    }
    if (strcmp(exp->def.op.name, "group") == 0) {
        *caller_must_freep = RDB_TRUE;
        return infer_group_keys(exp, ecp, keyvp);
    }

    /*
     * For all other relational operators, assume all-key table
     */
    *keyvp = all_key(exp, envp, ecp, txp);
    if (*keyvp == NULL) {
        return RDB_ERROR;
    }
    *caller_must_freep = RDB_TRUE;
    return 1;
}

RDB_object **
RDB_index_objpv(RDB_tbindex *indexp, RDB_expression *exp, RDB_type *tbtyp,
        int objpc, RDB_bool asc, RDB_exec_context *ecp)
{
    int i;
    RDB_expression *attrexp;

    RDB_object **objpv = RDB_alloc(sizeof (RDB_object *) * objpc, ecp);
    if (objpv == NULL)
        return NULL;
    for (i = 0; i < objpc; i++) {
        attrexp = RDB_attr_node(exp, indexp->attrv[i].attrname, "=");
        if (attrexp == NULL) {
            if (asc) {
                attrexp = RDB_attr_node(exp,
                        indexp->attrv[i].attrname, ">=");
                if (attrexp == NULL) {
                    attrexp = RDB_attr_node(exp, indexp->attrv[i].attrname, ">");
                    if (attrexp == NULL) {
                        attrexp = RDB_attr_node(exp, indexp->attrv[i].attrname, "starts_with");
                    }
                }
            } else {
                attrexp = RDB_attr_node(exp,
                        indexp->attrv[i].attrname, "<=");
                if (attrexp == NULL)
                    attrexp = RDB_attr_node(exp, indexp->attrv[i].attrname, "<");
            }
        }
        attrexp->def.op.args.firstp->nextp->def.obj.store_typ =
                    RDB_type_attr_type(tbtyp, indexp->attrv[i].attrname);
        objpv[i] = &attrexp->def.op.args.firstp->nextp->def.obj;
    }
    return objpv;
}
