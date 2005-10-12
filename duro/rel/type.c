/*
 * $Id$
 *
 * Copyright (C) 2003-2005 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>
#include <locale.h>

RDB_type RDB_BOOLEAN;
RDB_type RDB_INTEGER;
RDB_type RDB_RATIONAL;
RDB_type RDB_STRING;
RDB_type RDB_BINARY;

static int
compare_int(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp, argv[0]->var.int_val - argv[1]->var.int_val);
    return RDB_OK;
}

static int
compare_rational(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int res;

    if (argv[0]->var.rational_val < argv[1]->var.rational_val) {
        res = -1;
    } else if (argv[0]->var.rational_val > argv[1]->var.rational_val) {
        res = 1;
    } else {
        res = 0;
    }
    RDB_int_to_obj(retvalp, res);
    return RDB_OK;
}

static int
compare_string(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    RDB_int_to_obj(retvalp,
            strcoll(argv[0]->var.bin.datap, argv[1]->var.bin.datap));
    return RDB_OK;
}

void _RDB_init_builtin_types(void)
{
    RDB_BOOLEAN.kind = RDB_TP_SCALAR;
    RDB_BOOLEAN.ireplen = 1;
    RDB_BOOLEAN.name = "BOOLEAN";
    RDB_BOOLEAN.var.scalar.repc = 0;
    RDB_BOOLEAN.var.scalar.arep = NULL;
    RDB_BOOLEAN.var.scalar.constraintp = NULL;
    RDB_BOOLEAN.comparep = NULL;

    RDB_STRING.kind = RDB_TP_SCALAR;
    RDB_STRING.ireplen = RDB_VARIABLE_LEN;
    RDB_STRING.name = "STRING";
    RDB_STRING.var.scalar.repc = 0;
    RDB_STRING.var.scalar.arep = NULL;
    RDB_STRING.var.scalar.constraintp = NULL;
    RDB_STRING.comparep = &compare_string;

    RDB_INTEGER.kind = RDB_TP_SCALAR;
    RDB_INTEGER.ireplen = sizeof (RDB_int);
    RDB_INTEGER.name = "INTEGER";
    RDB_INTEGER.var.scalar.repc = 0;
    RDB_INTEGER.var.scalar.arep = NULL;
    RDB_INTEGER.var.scalar.constraintp = NULL;
    RDB_INTEGER.comparep = &compare_int;

    RDB_RATIONAL.kind = RDB_TP_SCALAR;
    RDB_RATIONAL.ireplen = sizeof (RDB_rational);
    RDB_RATIONAL.name = "RATIONAL";
    RDB_RATIONAL.var.scalar.repc = 0;
    RDB_RATIONAL.var.scalar.arep = NULL;
    RDB_RATIONAL.var.scalar.constraintp = NULL;
    RDB_RATIONAL.comparep = &compare_rational;

    RDB_BINARY.kind = RDB_TP_SCALAR;
    RDB_BINARY.ireplen = RDB_VARIABLE_LEN;
    RDB_BINARY.name = "BINARY";
    RDB_BINARY.var.scalar.repc = 0;
    RDB_BINARY.var.scalar.arep = NULL;
    RDB_BINARY.var.scalar.constraintp = NULL;
    RDB_BINARY.comparep = NULL;
}

RDB_bool
RDB_type_is_numeric(const RDB_type *typ) {
    return (RDB_bool)(typ == &RDB_INTEGER
                      || typ == &RDB_RATIONAL);
}

RDB_type *
_RDB_dup_nonscalar_type(RDB_type *typ)
{
    int ret;
    RDB_type *restyp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            restyp = malloc(sizeof (RDB_type));
            if (restyp == NULL)
                return NULL;
            restyp->name = NULL;
            restyp->kind = typ->kind;
            restyp->ireplen = RDB_VARIABLE_LEN;
            restyp->var.basetyp = _RDB_dup_nonscalar_type(typ->var.basetyp);
            if (restyp->var.basetyp == NULL) {
                free(restyp);
                return NULL;
            }
            return restyp;
        case RDB_TP_TUPLE:
            ret = RDB_create_tuple_type(typ->var.tuple.attrc,
                    typ->var.tuple.attrv, &restyp);
            if (ret != RDB_OK)
                return NULL;
            return restyp;
        case RDB_TP_SCALAR:
            return typ;
    }
    abort();
}

int
RDB_create_tuple_type(int attrc, const RDB_attr attrv[], RDB_type **typp)
{
    RDB_type *tuptyp;
    int i, j;
    int ret;

    tuptyp = malloc(sizeof(RDB_type));
    if (tuptyp == NULL)
        return RDB_NO_MEMORY;
    tuptyp->name = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->ireplen = RDB_VARIABLE_LEN;
    tuptyp->var.tuple.attrv = malloc(sizeof(RDB_attr) * attrc);
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < attrc; i++) {
        tuptyp->var.tuple.attrv[i].typ = NULL;
        tuptyp->var.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < attrc; i++) {
        /* Check if name appears twice */
        for (j = i + 1; j < attrc; j++) {
            if (strcmp(attrv[i].name, attrv[j].name) == 0) {
                ret = RDB_INVALID_ARGUMENT;
                goto error;
            }
        }

        tuptyp->var.tuple.attrv[i].typ = _RDB_dup_nonscalar_type(attrv[i].typ);
        if (tuptyp->var.tuple.attrv[i].typ == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        tuptyp->var.tuple.attrv[i].name = RDB_dup_str(attrv[i].name);
        if (tuptyp->var.tuple.attrv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        tuptyp->var.tuple.attrv[i].defaultp = NULL;
    }
    tuptyp->var.tuple.attrc = attrc;

    *typp = tuptyp;
    return RDB_OK;

error:
    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, NULL);
        }
    }
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);

    return ret;
}

int
RDB_create_relation_type(int attrc, const RDB_attr attrv[], RDB_type **typp)
{
    int ret;
    RDB_type *typ = malloc(sizeof (RDB_type));
    
    if (typ == NULL)
        return RDB_NO_MEMORY;
    
    typ->name = NULL;
    typ->kind = RDB_TP_RELATION;
    typ->ireplen = RDB_VARIABLE_LEN;
    ret = RDB_create_tuple_type(attrc, attrv, &typ->var.basetyp);
    if (ret != RDB_OK) {
        free(typ);
        return ret;
    }
    *typp = typ;
    return RDB_OK;
}

RDB_type *
RDB_create_array_type(RDB_type *basetyp)
{
    RDB_type *typ = malloc(sizeof (RDB_type));
    
    if (typ == NULL)
        return NULL;
    
    typ->name = NULL;
    typ->kind = RDB_TP_ARRAY;
    typ->ireplen = RDB_VARIABLE_LEN;
    typ->var.basetyp = basetyp;

    return typ;
}

RDB_bool
RDB_type_is_builtin(const RDB_type *typ)
{
    return (RDB_bool) ((typ == &RDB_BOOLEAN) || (typ == &RDB_INTEGER)
            || (typ == &RDB_RATIONAL) || (typ == &RDB_STRING)
            || (typ == &RDB_BINARY));
}

RDB_bool
RDB_type_is_scalar(const RDB_type *typ)
{
    return (typ->kind == RDB_TP_SCALAR);
}

static void
free_type(RDB_type *typ)
{
    int i;

    free(typ->name);

    switch (typ->kind) {
        case RDB_TP_TUPLE:
            for (i = 0; i < typ->var.tuple.attrc; i++) {
                RDB_type *attrtyp = typ->var.tuple.attrv[i].typ;
            
                free(typ->var.tuple.attrv[i].name);
                if (attrtyp->name == NULL)
                    RDB_drop_type(attrtyp, NULL);
                if (typ->var.tuple.attrv[i].defaultp != NULL) {
                    RDB_destroy_obj(typ->var.tuple.attrv[i].defaultp);
                    free(typ->var.tuple.attrv[i].defaultp);
                }
            }
            free(typ->var.tuple.attrv);
            break;
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            RDB_drop_type(typ->var.basetyp, NULL);
            break;
        case RDB_TP_SCALAR:
            if (typ->var.scalar.repc > 0) {
                int i, j;
                
                for (i = 0; i < typ->var.scalar.repc; i++) {
                    for (j = 0; j < typ->var.scalar.repv[i].compc; j++) {
                        free(typ->var.scalar.repv[i].compv[i].name);
                    }
                    free(typ->var.scalar.repv[i].compv);
                }
                free(typ->var.scalar.repv);
            }
            if (typ->var.scalar.arep != NULL
                    && typ->var.scalar.arep->name == NULL)
                RDB_drop_type(typ->var.scalar.arep, NULL);
            break;
        default:
            abort();
    }
    typ->kind = (enum _RDB_tp_kind) -1;
    free(typ);
}    

int
RDB_get_type(const char *name, RDB_transaction *txp, RDB_type **typp)
{
    RDB_type *foundtyp;
    int ret;

    if (strcmp(name, "BOOLEAN") == 0) {
        *typp = &RDB_BOOLEAN;
        return RDB_OK;
    }
    if (strcmp(name, "INTEGER") == 0) {
        *typp = &RDB_INTEGER;
        return RDB_OK;
    }
    if (strcmp(name, "RATIONAL") == 0) {
        *typp = &RDB_RATIONAL;
        return RDB_OK;
    }
    if (strcmp(name, "STRING") == 0) {
        *typp = &RDB_STRING;
        return RDB_OK;
    }
    if (strcmp(name, "BINARY") == 0) {
        *typp = &RDB_BINARY;
        return RDB_OK;
    }
    /* search for user defined type */

    foundtyp = RDB_hashmap_get(&txp->dbp->dbrootp->typemap, name);
    if (foundtyp != NULL) {
        *typp = foundtyp;
        return RDB_OK;
    }
    
    ret = _RDB_cat_get_type(name, txp, typp);
    if (ret != RDB_OK)
        return ret;

    return RDB_hashmap_put(&txp->dbp->dbrootp->typemap, name, *typp);
}

int
RDB_define_type(const char *name, int repc, const RDB_possrep repv[],
                RDB_expression *constraintp, RDB_transaction *txp)
{
    RDB_object tpl;
    RDB_object conval;
    RDB_object typedata;
    int ret;
    int i, j;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    RDB_init_obj(&tpl);
    RDB_init_obj(&conval);
    RDB_init_obj(&typedata);

    ret = RDB_binary_set(&typedata, 0, NULL, 0);
    if (ret != RDB_OK)
        return ret;

    /*
     * Insert tuple into SYS_TYPES
     */

    ret = RDB_tuple_set_string(&tpl, "TYPENAME", name);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set(&tpl, "I_AREP_TYPE", &typedata);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "I_AREP_LEN", -2);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_bool(&tpl, "I_SYSIMPL", RDB_FALSE);
    if (ret != RDB_OK)
        goto error;

    /* Store constraint in tuple */
    ret = _RDB_expr_to_obj(&conval, constraintp);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set(&tpl, "I_CONSTRAINT", &conval);
    if (ret != RDB_OK)
        goto error;

    ret = RDB_insert(txp->dbp->dbrootp->types_tbp, &tpl, txp);
    if (ret != RDB_OK)
        goto error;

    /*
     * Insert tuple into SYS_POSSREPS
     */   

    for (i = 0; i < repc; i++) {
        char *prname = repv[i].name;

        if (prname == NULL) {
            /* possrep name may be NULL if there's only 1 possrep */
            if (repc > 1) {
                ret = RDB_INVALID_ARGUMENT;
                goto error;
            }
            prname = (char *)name;
        }
        ret = RDB_tuple_set_string(&tpl, "POSSREPNAME", prname);
        if (ret != RDB_OK)
            goto error;

        for (j = 0; j < repv[i].compc; j++) {
            char *cname = repv[i].compv[j].name;

            if (cname == NULL) {
                if (repv[i].compc > 1) {
                    ret = RDB_INVALID_ARGUMENT;
                    goto error;
                }
                cname = prname;
            }
            ret = RDB_tuple_set_int(&tpl, "COMPNO", (RDB_int)j);
            if (ret != RDB_OK)
                goto error;
            ret = RDB_tuple_set_string(&tpl, "COMPNAME", cname);
            if (ret != RDB_OK)
                goto error;
            ret = RDB_tuple_set_string(&tpl, "COMPTYPENAME",
                    repv[i].compv[j].typ->name);
            if (ret != RDB_OK)
                goto error;

            ret = RDB_insert(txp->dbp->dbrootp->possrepcomps_tbp, &tpl, txp);
            if (ret != RDB_OK)
                goto error;
        }
    }

    RDB_destroy_obj(&typedata);
    RDB_destroy_obj(&conval);    
    RDB_destroy_obj(&tpl);

    if (constraintp != NULL)
        RDB_drop_expr(constraintp);
    
    return RDB_OK;
    
error:
    RDB_destroy_obj(&typedata);
    RDB_destroy_obj(&conval);    
    RDB_destroy_obj(&tpl);

    _RDB_handle_syserr(txp, ret);

    return ret;
}

/* Implements a system-generated selector */
int
_RDB_sys_select(const char *name, int argc, RDB_object *argv[],
        const void *iargp, size_t iarglen, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int ret;
    RDB_type *typ;
    RDB_possrep *prp;

    ret = RDB_get_type((char *) iargp, txp, &typ);
    if (ret != RDB_OK)
        return ret;

    /* Find possrep */
    prp = _RDB_get_possrep(typ, name);
    if (prp == NULL)
        return RDB_INVALID_ARGUMENT;

    /* If *retvalp carries a value, it must match the type */
    if (retvalp->kind != RDB_OB_INITIAL
            && (retvalp->typ == NULL
                || !RDB_type_equals(retvalp->typ, typ)))
        return RDB_TYPE_MISMATCH;

    if (argc == 1) {
        /* Copy value */
        ret = _RDB_copy_obj(retvalp, argv[0], NULL);
        if (ret != RDB_OK)
            return ret;
    } else {
        /* Copy tuple attributes */
        int i;

        RDB_destroy_obj(retvalp);
        RDB_init_obj(retvalp);
        for (i = 0; i < argc; i++) {
            ret = RDB_tuple_set(retvalp, typ->var.scalar.repv[0].compv[i].name,
                    argv[i]);
            if (ret != RDB_OK)
                return ret;
        }
    }
    retvalp->typ = typ;
    return RDB_OK;
}

int
RDB_implement_type(const char *name, RDB_type *arep, RDB_int areplen,
        RDB_transaction *txp)
{
    RDB_expression *exp, *wherep;
    RDB_attr_update upd[3];
    RDB_object typedata;
    int ret;
    int i;
    RDB_bool sysimpl = (arep == NULL) && (areplen == -1);

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    if (sysimpl) {
        /*
         * No actual rep given, so selector and getters/setters must be provided
         * by the system
         */
        RDB_type *typ;
        RDB_type **argtv;
        int compc;
        int i;

        ret = RDB_get_type(name, txp, &typ);
        if (ret != RDB_OK)
            return ret;

        /* # of possreps must be one */
        if (typ->var.scalar.repc != 1)
            return RDB_INVALID_ARGUMENT;

        compc = typ->var.scalar.repv[0].compc;
        if (compc == 1) {
            arep = typ->var.scalar.repv[0].compv[0].typ;
        } else {
            /* More than one component, so internal rep is a tuple */
            ret = RDB_create_tuple_type(typ->var.scalar.repv[0].compc,
                    typ->var.scalar.repv[0].compv, &arep);
            if (ret != RDB_OK)
                return ret;
        }

        typ->var.scalar.arep = arep;
        typ->var.scalar.sysimpl = sysimpl;
        typ->ireplen = arep->ireplen;

        /* Create selector */
        argtv = malloc(sizeof(RDB_type *) * compc);
        if (argtv == NULL)
            return RDB_NO_MEMORY;
        for (i = 0; i < compc; i++)
            argtv[i] = typ->var.scalar.repv[0].compv[i].typ;
        ret = RDB_create_ro_op(typ->var.scalar.repv[0].name, compc, argtv, typ,
                "libduro", "_RDB_sys_select", typ->name, strlen(typ->name) + 1,
                txp);
        free(argtv);
        if (ret != RDB_OK)
            return ret;
    }

    exp = RDB_expr_attr("TYPENAME");
    if (exp == NULL) {
        return RDB_NO_MEMORY;
    }
    wherep = RDB_ro_op_va("=", exp, RDB_string_to_expr(name),
            (RDB_expression *) NULL);
    if (wherep == NULL) {
        RDB_drop_expr(exp);
        return RDB_NO_MEMORY;
    }    

    upd[0].exp = upd[1].exp = upd[2].exp = NULL;

    upd[0].name = "I_AREP_LEN";
    upd[0].exp = RDB_int_to_expr(arep == NULL ? areplen : arep->ireplen);
    if (upd[0].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    upd[1].name = "I_SYSIMPL";
    upd[1].exp = RDB_bool_to_expr(sysimpl);
    if (upd[1].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    if (arep != NULL) {
        RDB_init_obj(&typedata);
        ret = _RDB_type_to_obj(&typedata, arep);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&typedata);
            goto cleanup;
        }

        upd[2].name = "I_AREP_TYPE";
        upd[2].exp = RDB_obj_to_expr(&typedata);
        if (upd[2].exp == NULL) {
            ret = RDB_NO_MEMORY;
            RDB_destroy_obj(&typedata);
            goto cleanup;
        }
    }

    ret = RDB_update(txp->dbp->dbrootp->types_tbp, wherep,
            arep != NULL ? 3 : 2, upd, txp);

cleanup:    
    for (i = 0; i < 3; i++) {
        if (upd[i].exp != NULL)
            RDB_drop_expr(upd[i].exp);
    }
    RDB_drop_expr(wherep);

    _RDB_handle_syserr(txp, ret);

    return ret;
}

int
RDB_drop_type(RDB_type *typ, RDB_transaction *txp)
{
    int ret;

    if (RDB_type_is_builtin(typ))
        return RDB_INVALID_ARGUMENT;

    if (typ->name != NULL) {
        RDB_expression *wherep;
        RDB_type *ntp = NULL;

        if (!RDB_tx_is_running(txp))
            return RDB_INVALID_TRANSACTION;

        /* !! should check if the type is still used by a table */

        /* Delete selector */
        ret = RDB_drop_op(typ->name, txp);
        if (ret != RDB_OK && ret != RDB_NOT_FOUND)
            return ret;

        /* Delete type from type table by puting a NULL pointer into it */
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, typ->name, ntp);
        if (ret != RDB_OK) {
            _RDB_handle_syserr(txp, ret);
            return ret;
        }

        /* Delete type from database */
        wherep = RDB_ro_op_va("=", RDB_expr_attr("TYPENAME"),
                RDB_string_to_expr(typ->name), (RDB_expression *) NULL);
        if (wherep == NULL) {
            return RDB_NO_MEMORY;
        }
        ret = RDB_delete(txp->dbp->dbrootp->types_tbp, wherep, txp);
        if (ret != RDB_OK) {
            RDB_drop_expr(wherep);
            return ret;
        }
        ret = RDB_delete(txp->dbp->dbrootp->possrepcomps_tbp, wherep, txp);
        if (ret != RDB_OK) {
            RDB_drop_expr(wherep);
            return ret;
        }
    }
    free_type(typ);
    return RDB_OK;
}

RDB_bool
RDB_type_equals(const RDB_type *typ1, const RDB_type *typ2)
{
    if (typ1 == typ2)
        return RDB_TRUE;
    if (typ1->kind != typ2->kind)
        return RDB_FALSE;
    
    /* If the two types both have a name, they are equal iff their name
     * is equal */
    if (typ1->name != NULL && typ2->name != NULL)
         return (RDB_bool) (strcmp(typ1->name, typ2->name) == 0);
    
    switch (typ1->kind) {
        case RDB_TP_RELATION:
        case RDB_TP_ARRAY:
            return RDB_type_equals(typ1->var.basetyp, typ2->var.basetyp);
        case RDB_TP_TUPLE:
            {
                int i, j;
                int attrcnt = typ1->var.tuple.attrc;

                if (attrcnt != typ2->var.tuple.attrc)
                    return RDB_FALSE;
                    
                /* check if all attributes of typ1 also appear in typ2 */
                for (i = 0; i < attrcnt; i++) {
                    for (j = 0; j < attrcnt; j++) {
                        if (RDB_type_equals(typ1->var.tuple.attrv[i].typ,
                                typ2->var.tuple.attrv[j].typ)
                                && (strcmp(typ1->var.tuple.attrv[i].name,
                                typ2->var.tuple.attrv[j].name) == 0))
                            break;
                    }
                    if (j >= attrcnt) {
                        /* not found */
                        return RDB_FALSE;
                    }
                }
                return RDB_TRUE;
            }
        default:
            ;
    }
    abort();
}  

char *
RDB_type_name(const RDB_type *typ)
{
    return typ->name;
}

RDB_type *
RDB_type_attr_type(const RDB_type *typ, const char *name)
{
    RDB_attr *attrp;

    switch (typ->kind) {
        case RDB_TP_RELATION:
            attrp = _RDB_tuple_type_attr(typ->var.basetyp, name);
            break;
        case RDB_TP_TUPLE:
            attrp = _RDB_tuple_type_attr(typ, name);
            break;
        case RDB_TP_ARRAY:
        case RDB_TP_SCALAR:
            return NULL;
    }
    if (attrp == NULL)
        return NULL;
    return attrp->typ;
}

int
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
    RDB_type **newtypp)
{
    int i;
    int ret;
    *newtypp = malloc(sizeof (RDB_type));

    if (*newtypp == NULL)
        return RDB_NO_MEMORY;
    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_TUPLE;
    (*newtypp)->var.tuple.attrc = typ->var.tuple.attrc + attrc;
    (*newtypp)->var.tuple.attrv = malloc(sizeof (RDB_attr)
            * ((*newtypp)->var.tuple.attrc));
    if ((*newtypp)->var.tuple.attrv == NULL) {
        free((*newtypp));
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        (*newtypp)->var.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        (*newtypp)->var.tuple.attrv[i].name =
                RDB_dup_str(typ->var.tuple.attrv[i].name);
        if ((*newtypp)->var.tuple.attrv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        (*newtypp)->var.tuple.attrv[i].typ =
                _RDB_dup_nonscalar_type(typ->var.tuple.attrv[i].typ);
        if ((*newtypp)->var.tuple.attrv[i].typ == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        (*newtypp)->var.tuple.attrv[i].defaultp = NULL;
    }
    for (i = 0; i < attrc; i++) {
        /*
         * Check if the attribute is already present in the original tuple type
         */
        if (_RDB_tuple_type_attr(typ, attrv[i].name) != NULL) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }

        (*newtypp)->var.tuple.attrv[typ->var.tuple.attrc + i].name =
                RDB_dup_str(attrv[i].name);
        if ((*newtypp)->var.tuple.attrv[typ->var.tuple.attrc + i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        (*newtypp)->var.tuple.attrv[typ->var.tuple.attrc + i].typ =
                _RDB_dup_nonscalar_type(attrv[i].typ);
        if ((*newtypp)->var.tuple.attrv[typ->var.tuple.attrc + i].typ == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        (*newtypp)->var.tuple.attrv[typ->var.tuple.attrc + i].defaultp = NULL;
    }
    return RDB_OK;    

error:
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        free((*newtypp)->var.tuple.attrv[i].name);
        if ((*newtypp)->var.tuple.attrv[i].typ != NULL
                && !RDB_type_is_scalar((*newtypp)->var.tuple.attrv[i].typ)) {
            RDB_drop_type((*newtypp)->var.tuple.attrv[i].typ, NULL);
        }
    }
    free((*newtypp)->var.tuple.attrv);
    free(*newtypp);
    *newtypp = NULL;
    return ret;
}

int
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[],
        RDB_type **newtypp)
{
    int ret;

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL) {
        return RDB_NO_MEMORY;
    }
    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_RELATION;
    ret = RDB_extend_tuple_type(typ->var.basetyp, attrc, attrv,
            &(*newtypp)->var.basetyp);
    if (ret != RDB_OK) {
        free(*newtypp);
        *newtypp = NULL;
    }
    return ret;
}

int
RDB_join_tuple_types(const RDB_type *typ1, const RDB_type *typ2,
        RDB_type **newtypp)
{
    RDB_type *newtyp;
    int attrc;
    int i, j;
    int ret;
    
    /* Create new tuple type */
    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL)
        return RDB_NO_MEMORY;

    *newtypp = newtyp;
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    
    /* calculate new # of attributes as the sum of the # of attributes
     * of both types.
     * That often will be too high; in this case it is reduced later.
     */
    newtyp->var.tuple.attrc = typ1->var.tuple.attrc
            + typ2->var.tuple.attrc;
    newtyp->var.tuple.attrv = malloc(sizeof (RDB_attr)
            * newtyp->var.tuple.attrc);

    for (i = 0; i < typ1->var.tuple.attrc; i++)
        newtyp->var.tuple.attrv[i].name = NULL;

    /* copy attributes from first tuple type */
    for (i = 0; i < typ1->var.tuple.attrc; i++) {
        newtyp->var.tuple.attrv[i].name = RDB_dup_str(
                typ1->var.tuple.attrv[i].name);
        newtyp->var.tuple.attrv[i].typ = 
                _RDB_dup_nonscalar_type(typ1->var.tuple.attrv[i].typ);
        newtyp->var.tuple.attrv[i].defaultp = NULL;
    }
    attrc = typ1->var.tuple.attrc;

    /* add attributes from second tuple type */
    for (i = 0; i < typ2->var.tuple.attrc; i++) {
        for (j = 0; j < typ1->var.tuple.attrc; j++) {
            if (strcmp(typ2->var.tuple.attrv[i].name,
                    typ1->var.tuple.attrv[j].name) == 0) {
                /* If two attributes match by name, they must be of
                   the same type */
                if (!RDB_type_equals(typ2->var.tuple.attrv[i].typ,
                        typ1->var.tuple.attrv[j].typ)) {
                    ret = RDB_TYPE_MISMATCH;
                    goto error;
                }
                break;
            }
        }
        if (j >= typ1->var.tuple.attrc) {
            /* attribute not found, so add it to result type */
            newtyp->var.tuple.attrv[attrc].name = RDB_dup_str(
                    typ2->var.tuple.attrv[i].name);
            newtyp->var.tuple.attrv[attrc].typ =
                    _RDB_dup_nonscalar_type(typ2->var.tuple.attrv[i].typ);
            newtyp->var.tuple.attrv[attrc].defaultp = NULL;
            attrc++;
        }
    }

    /* adjust array size, if necessary */    
    if (attrc < newtyp->var.tuple.attrc) {
        newtyp->var.tuple.attrc = attrc;
        newtyp->var.tuple.attrv = realloc(newtyp->var.tuple.attrv,
                sizeof(RDB_attr) * attrc);
    }
    return RDB_OK;

error:
    for (i = 0; i < typ1->var.tuple.attrc; i++)
        free(newtyp->var.tuple.attrv[i].name);

    free(newtyp);
    return ret;
}

int
RDB_join_relation_types(const RDB_type *typ1, const RDB_type *typ2,
                     RDB_type **newtypp)
{
    RDB_type *newtyp;
    int ret;

    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL) {
        return RDB_NO_MEMORY;
    }
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_RELATION;

    ret = RDB_join_tuple_types(typ1->var.basetyp, typ2->var.basetyp,
                               &newtyp->var.basetyp);
    if (ret != RDB_OK) {
        free(newtyp);
        return ret;
    }
    *newtypp = newtyp;
    return RDB_OK;
}

/* Return a pointer to the RDB_attr strcuture of the attribute with name attrname in the tuple
   type pointed to by tutyp. */
RDB_attr *
_RDB_tuple_type_attr(const RDB_type *tuptyp, const char *attrname)
{
    int i;
    
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        if (strcmp(tuptyp->var.tuple.attrv[i].name, attrname) == 0)
            return &tuptyp->var.tuple.attrv[i];
    }
    /* not found */
    return NULL;
}

int
RDB_project_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_type **newtypp)
{
    RDB_type *tuptyp;
    int i;
    int ret;

    tuptyp = malloc(sizeof (RDB_type));
    if (tuptyp == NULL) {
        return RDB_NO_MEMORY;
    }
    tuptyp->name = NULL;
    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->var.tuple.attrc = attrc;
    tuptyp->var.tuple.attrv = malloc(attrc * sizeof (RDB_attr));
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < attrc; i++)
        tuptyp->var.tuple.attrv[i].name = NULL;

    for (i = 0; i < attrc; i++) {
        RDB_attr *attrp;
        char *attrname = RDB_dup_str(attrv[i]);

        if (attrname == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        tuptyp->var.tuple.attrv[i].name = attrname;

        attrp = _RDB_tuple_type_attr(typ, attrname);
        if (attrp == NULL) {
            ret = RDB_ATTRIBUTE_NOT_FOUND;
            goto error;
        }
        tuptyp->var.tuple.attrv[i].typ = _RDB_dup_nonscalar_type(attrp->typ);

        tuptyp->var.tuple.attrv[i].defaultp = NULL;
    }
    
    *newtypp = tuptyp;
    
    return RDB_OK;
error:
    for (i = 0; i < attrc; i++)
        free(tuptyp->var.tuple.attrv[i].name);
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);
    return ret;
}

int
RDB_project_relation_type(const RDB_type *typ, int attrc, char *attrv[],
                          RDB_type **newtypp)
{
    RDB_type *reltyp;
    int ret;

    reltyp = malloc(sizeof (RDB_type));
    if (reltyp == NULL) {
        return RDB_NO_MEMORY;
    }

    ret = RDB_project_tuple_type(typ->var.basetyp, attrc, attrv,
            &reltyp->var.basetyp);
    if (ret != RDB_OK) {
        free(reltyp);
        return ret;
    }
    reltyp->name = NULL;
    reltyp->kind = RDB_TP_RELATION;

    *newtypp = reltyp;
    return RDB_OK;
}

int
_RDB_find_rename_from(int renc, const RDB_renaming renv[], const char *name)
{
    int i;

    for (i = 0; i < renc && strcmp(renv[i].from, name) != 0; i++);
    if (i >= renc)
        return -1; /* not found */
    /* found */
    return i;
}

int
RDB_rename_tuple_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_type **newtypp)
{
    RDB_type *newtyp;
    int i, j;
    int ret;

    /*
     * Check arguments
     */
    for (i = 0; i < renc; i++) {
        /* Check if source attribute exists */
        if (_RDB_tuple_type_attr(typ, renv[i].from) == NULL)
            return RDB_ATTRIBUTE_NOT_FOUND;

        /* Check if the dest attribute does not exist */
        if (_RDB_tuple_type_attr(typ, renv[i].to) != NULL)
            return RDB_INVALID_ARGUMENT;

        for (j = i + 1; j < renc; j++) {
            /* Check if source or dest appears twice */
            if (strcmp(renv[i].from, renv[j].from) == 0
                    || strcmp(renv[i].to, renv[j].to) == 0) {
                return RDB_INVALID_ARGUMENT;
            }
        }
    }

    newtyp = malloc(sizeof (RDB_type));
    if (newtyp == NULL)
        return RDB_NO_MEMORY;

    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->var.tuple.attrc = typ->var.tuple.attrc;
    newtyp->var.tuple.attrv = malloc (typ->var.tuple.attrc * sizeof(RDB_attr));
    if (newtyp->var.tuple.attrv == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++)
        newtyp->var.tuple.attrv[i].name = NULL;
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        char *attrname = typ->var.tuple.attrv[i].name; 
        int ai = _RDB_find_rename_from(renc, renv, attrname);

        /* check if the attribute has been renamed */
        newtyp->var.tuple.attrv[i].typ = _RDB_dup_nonscalar_type(
                typ->var.tuple.attrv[i].typ);
        if (ai >= 0)
            newtyp->var.tuple.attrv[i].name = RDB_dup_str(renv[ai].to);
        else
            newtyp->var.tuple.attrv[i].name = RDB_dup_str(attrname);
        if (newtyp->var.tuple.attrv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        newtyp->var.tuple.attrv[i].defaultp = NULL;
     }
     *newtypp = newtyp;
     return RDB_OK;

error:
    if (newtyp->var.tuple.attrv != NULL) {
        for (i = 0; i < newtyp->var.tuple.attrc; i++)
            free(newtyp->var.tuple.attrv[i].name);
        free(newtyp->var.tuple.attrv);
    }

    free(newtyp);
    return ret;
}

int
RDB_rename_relation_type(const RDB_type *typ, int renc, const RDB_renaming renv[],
        RDB_type **newtypp)
{
    int ret;

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL)
        return RDB_NO_MEMORY;

    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_RELATION;

    ret = RDB_rename_tuple_type(typ->var.basetyp, renc, renv,
            &(*newtypp)->var.basetyp);
    if (ret != RDB_OK) {
        free(*newtypp);
        return ret;
    }
    return RDB_OK;
}

RDB_possrep *
_RDB_get_possrep(RDB_type *typ, const char *repname)
{
    int i;

    if (!RDB_type_is_scalar(typ))
        return NULL;
    for (i = 0; i < typ->var.scalar.repc
            && strcmp(typ->var.scalar.repv[i].name, repname) != 0;
            i++);
    if (i >= typ->var.scalar.repc)
        return NULL;
    return &typ->var.scalar.repv[i];
}

RDB_attr *
_RDB_get_icomp(RDB_type *typ, const char *compname)
{
    int i, j;

    for (i = 0; i < typ->var.scalar.repc; i++) {
        for (j = 0; j < typ->var.scalar.repv[i].compc; j++) {
            if (strcmp(typ->var.scalar.repv[i].compv[j].name, compname) == 0)
                return &typ->var.scalar.repv[i].compv[j];
        }
    }
    return NULL;
}

static int
copy_attr(RDB_attr *dstp, const RDB_attr *srcp)
{
    dstp->name = RDB_dup_str(srcp->name);
    if (dstp->name == NULL) {
        return RDB_NO_MEMORY;
    }
    dstp->typ = _RDB_dup_nonscalar_type(srcp->typ);
    if (dstp->typ == NULL) {
        return RDB_NO_MEMORY;
    }
    dstp->defaultp = NULL;
    return RDB_OK;
}

static int
aggr_type(RDB_type *tuptyp, RDB_type *attrtyp, RDB_aggregate_op op,
          RDB_type **resultpp)
{
    if (op == RDB_COUNT || op == RDB_COUNTD) {
        *resultpp = &RDB_INTEGER;
        return RDB_OK;
    }

    switch (op) {
        /* only to avoid compiler warnings */
        case RDB_COUNTD:
        case RDB_COUNT:

        case RDB_AVGD:
        case RDB_AVG:
            if (!RDB_type_is_numeric(attrtyp))
                return RDB_TYPE_MISMATCH;
            *resultpp = &RDB_RATIONAL;
            break;
        case RDB_SUM:
        case RDB_SUMD:
        case RDB_MAX:
        case RDB_MIN:
            if (!RDB_type_is_numeric(attrtyp))
                return RDB_TYPE_MISMATCH;
            *resultpp = attrtyp;
            break;
        case RDB_ALL:
        case RDB_ANY:
            if (attrtyp != &RDB_BOOLEAN)
                return RDB_TYPE_MISMATCH;
            *resultpp = &RDB_BOOLEAN;
            break;
     }
     return RDB_OK;
}

int
RDB_summarize_type(RDB_type *tb1typ, RDB_type *tb2typ,
        int addc, const RDB_summarize_add addv[],
        int avgc, char **avgv, RDB_transaction *txp, RDB_type **newtypp)
{
    int i;
    int ret;
    int attrc = addc + avgc;
    RDB_attr *attrv = malloc(sizeof (RDB_attr) * attrc);
    if (attrv == NULL)
        return RDB_NO_MEMORY;

    for (i = 0; i < addc; i++) {
        if (addv[i].op == RDB_COUNT) {
            attrv[i].typ = &RDB_INTEGER;
        } else {
            RDB_type *typ;

            ret = RDB_expr_type(addv[i].exp, tb1typ->var.basetyp, txp, &typ);
            if (ret != RDB_OK)
                goto error;
            ret = aggr_type(tb1typ->var.basetyp, typ,
                        addv[i].op, &attrv[i].typ);
            if (ret != RDB_OK) {
                if (!RDB_type_is_scalar(typ))
                    RDB_drop_type(typ, NULL);
                goto error;
            }
        }

        attrv[i].name = addv[i].name;
    }
    for (i = 0; i < avgc; i++) {
        attrv[addc + i].name = avgv[i];
        attrv[addc + i].typ = &RDB_INTEGER;
    }

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    
    ret = RDB_extend_relation_type(tb2typ, attrc, attrv, newtypp);
    if (ret != RDB_OK) {
        goto error;
    }

    free(attrv);
    return RDB_OK;

error:
    free(attrv);    
    return ret;
}

int
RDB_wrap_tuple_type(const RDB_type *typ, int wrapc, const RDB_wrapping wrapv[],
        RDB_type **newtypp)
{
    int i, j, k;
    int ret;
    RDB_attr *attrp;
    int attrc;

    /* Compute # of attributes */
    attrc = typ->var.tuple.attrc;
    for (i = 0; i < wrapc; i++)
        attrc += 1 - wrapv[i].attrc;

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL)
        return RDB_NO_MEMORY;

    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_TUPLE;
    (*newtypp)->var.tuple.attrc = attrc;
    (*newtypp)->var.tuple.attrv = malloc(attrc * sizeof(RDB_attr));
    if ((*newtypp)->var.tuple.attrv == NULL) {
        free(newtypp);
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < attrc; i++) {
        (*newtypp)->var.tuple.attrv[i].name = NULL;
        (*newtypp)->var.tuple.attrv[i].typ = NULL;
    }

    /*
     * Copy attributes from wrapv
     */
    for (i = 0; i < wrapc; i++) {
        RDB_type *tuptyp = malloc(sizeof(RDB_type));
        if (tuptyp == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        tuptyp->name = NULL;
        tuptyp->kind = RDB_TP_TUPLE;
        tuptyp->var.tuple.attrc = wrapv[i].attrc;
        tuptyp->var.tuple.attrv = malloc(sizeof(RDB_attr) * wrapv[i].attrc);
        if (tuptyp->var.tuple.attrv == NULL) {
            ret = RDB_NO_MEMORY;
            free(tuptyp);
            goto error;
        }

        for (j = 0; j < wrapv[i].attrc; j++) {
            attrp = _RDB_tuple_type_attr(typ, wrapv[i].attrv[j]);
            if (attrp == NULL) {
                ret = RDB_ATTRIBUTE_NOT_FOUND;
                free(tuptyp->var.tuple.attrv);
                free(tuptyp);
                goto error;
            }

            ret = copy_attr(&tuptyp->var.tuple.attrv[j], attrp);
            if (ret != RDB_OK) {
                free(tuptyp->var.tuple.attrv);
                free(tuptyp);
                goto error;
            }
        }
        (*newtypp)->var.tuple.attrv[i].name = RDB_dup_str(wrapv[i].attrname);
        (*newtypp)->var.tuple.attrv[i].typ = tuptyp;        
        (*newtypp)->var.tuple.attrv[i].defaultp = NULL;
    }

    /*
     * Copy remaining attributes
     */
    k = wrapc;
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in wrapv */
        for (j = 0; j < wrapc && RDB_find_str(wrapv[j].attrc, wrapv[j].attrv,
                typ->var.tuple.attrv[i].name) == -1; j++);
        if (j == wrapc) {
            /* Not found */
            ret = copy_attr(&(*newtypp)->var.tuple.attrv[k],
                    &typ->var.tuple.attrv[i]);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }
    if (k != attrc) {
        ret = RDB_INVALID_ARGUMENT;
        goto error;
    }

    return RDB_OK;

error:
    for (i = 0; i < attrc; i++) {
        attrp = &(*newtypp)->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free (attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ == NULL)
                RDB_drop_type(attrp->typ, NULL);
        }
    }
    free((*newtypp)->var.tuple.attrv);
    free(*newtypp);
    return ret;
}

int
RDB_wrap_relation_type(const RDB_type *typ, int wrapc,
        const RDB_wrapping wrapv[], RDB_type **newtypp)
{
    int ret;

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL)
        return RDB_NO_MEMORY;

    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_RELATION;

    ret = RDB_wrap_tuple_type(typ->var.basetyp, wrapc, wrapv,
            &(*newtypp)->var.basetyp);
    if (ret != RDB_OK) {
        free(*newtypp);
        return ret;
    }
    return RDB_OK;
}

int
RDB_unwrap_tuple_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_type **newtypp)
{
    int nattrc;
    int i, j, k;
    int ret;
    RDB_attr *attrp;

    /* Compute # of attributes */
    nattrc = typ->var.tuple.attrc;
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);
        if (tuptyp == NULL) {
            ret = RDB_ATTRIBUTE_NOT_FOUND;
            goto error;
        }        
        if (tuptyp->kind != RDB_TP_TUPLE) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }        
        nattrc += tuptyp->var.tuple.attrc - 1;
    }

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL)
        return RDB_NO_MEMORY;

    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_TUPLE;
    (*newtypp)->var.tuple.attrc = nattrc;
    (*newtypp)->var.tuple.attrv = malloc(nattrc * sizeof(RDB_attr));
    if ((*newtypp)->var.tuple.attrv == NULL) {
        free(newtypp);
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < nattrc; i++) {
        (*newtypp)->var.tuple.attrv[i].name = NULL;
        (*newtypp)->var.tuple.attrv[i].typ = NULL;
    }

    k = 0;

    /* Copy sub-attributes of attrv */
    for (i = 0; i < attrc; i++) {
        RDB_type *tuptyp = RDB_type_attr_type(typ, attrv[i]);

        for (j = 0; j < tuptyp->var.tuple.attrc; j++) {
            ret = copy_attr(&(*newtypp)->var.tuple.attrv[k],
                    &tuptyp->var.tuple.attrv[j]);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }

    /* Copy remaining attributes */
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        /* Copy attribute if it does not appear in attrv */
        if (RDB_find_str(attrc, attrv, typ->var.tuple.attrv[i].name) == -1) {
            ret = copy_attr(&(*newtypp)->var.tuple.attrv[k],
                    &typ->var.tuple.attrv[i]);
            if (ret != RDB_OK)
                goto error;
            k++;
        }
    }

    if (k != nattrc) {
        ret = RDB_INVALID_ARGUMENT;
        goto error;
    }

    return RDB_OK;

error:
    for (i = 0; i < attrc; i++) {
        attrp = &(*newtypp)->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free (attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, NULL);
        }
    }
    free((*newtypp)->var.tuple.attrv);
    free(*newtypp);
    return ret;
}    

int
RDB_unwrap_relation_type(const RDB_type *typ, int attrc, char *attrv[],
        RDB_type **newtypp)
{
    int ret;

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL)
        return RDB_NO_MEMORY;

    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_RELATION;

    ret = RDB_unwrap_tuple_type(typ->var.basetyp, attrc, attrv,
            &(*newtypp)->var.basetyp);
    if (ret != RDB_OK) {
        free(*newtypp);
        return ret;
    }
    return RDB_OK;
}

int
RDB_group_type(RDB_type *typ, int attrc, char *attrv[], const char *gattr,
        RDB_type **newtypp)
{
    int i, j;
    int ret;
    RDB_attr *rtattrv;
    RDB_attr *attrp;
    RDB_type *tuptyp;
    RDB_type *gattrtyp;

    /*
     * Create relation type for attribute gattr
     */
    rtattrv = malloc(sizeof(RDB_attr) * attrc);
    if (rtattrv == NULL) {
        return RDB_NO_MEMORY;
    }
    for (i = 0; i < attrc; i++) {
        attrp = _RDB_tuple_type_attr(typ->var.basetyp, attrv[i]);
        if (attrp == NULL) {
            free(rtattrv);
            return RDB_ATTRIBUTE_NOT_FOUND;
        }
        rtattrv[i].typ = attrp->typ;
        rtattrv[i].name = attrp->name;
    }
    ret = RDB_create_relation_type(attrc, rtattrv, &gattrtyp);
    free(rtattrv);
    if (ret != RDB_OK)
        return ret;

    /*
     * Create tuple type
     */
    tuptyp = malloc(sizeof (RDB_type));
    if (tuptyp == NULL) {
        RDB_drop_type(gattrtyp, NULL);
        return RDB_NO_MEMORY;
    }

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;
    tuptyp->var.tuple.attrc = typ->var.basetyp->var.tuple.attrc + 1 - attrc;
    tuptyp->var.tuple.attrv = malloc(tuptyp->var.tuple.attrc * sizeof(RDB_attr));
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        RDB_drop_type(gattrtyp, NULL);
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        tuptyp->var.tuple.attrv[i].name = NULL;
        tuptyp->var.tuple.attrv[i].typ = NULL;
    }

    j = 0;
    for (i = 0; i < typ->var.basetyp->var.tuple.attrc; i++) {
        char *attrname = typ->var.basetyp->var.tuple.attrv[i].name;

        if (_RDB_tuple_type_attr(gattrtyp->var.basetyp, attrname) == NULL) {
            if (strcmp(attrname, gattr) == 0) {
                RDB_drop_type(gattrtyp, NULL);
                ret = RDB_INVALID_ARGUMENT;
                goto error;
            }
            ret = copy_attr(&tuptyp->var.tuple.attrv[j],
                    &typ->var.basetyp->var.tuple.attrv[i]);
            if (ret != RDB_OK) {
                RDB_drop_type(gattrtyp, NULL);
                goto error;
            }
            tuptyp->var.tuple.attrv[j].defaultp = NULL;
            j++;
        }
    }
    tuptyp->var.tuple.attrv[j].typ = gattrtyp;
    tuptyp->var.tuple.attrv[j].name = RDB_dup_str(gattr);
    if (tuptyp->var.tuple.attrv[j].name == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    tuptyp->var.tuple.attrv[j].defaultp = NULL;

    /*
     * Create relation type
     */
    (*newtypp) = malloc(sizeof(RDB_type));
    if (*newtypp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    (*newtypp)->kind = RDB_TP_RELATION;
    (*newtypp)->name = NULL;
    (*newtypp)->var.basetyp = tuptyp;

    return RDB_OK;    

error:
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, NULL);
        }
    }
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);

    return ret;
}

int
RDB_ungroup_type(RDB_type *typ, const char *attr, RDB_type **newtypp)
{
    int i, j;
    int ret;
    RDB_type *tuptyp;
    RDB_attr *relattrp = _RDB_tuple_type_attr(typ->var.basetyp, attr);

    if (relattrp == NULL)
        return RDB_ATTRIBUTE_NOT_FOUND;
    if (relattrp->typ->kind != RDB_TP_RELATION)
        return RDB_INVALID_ARGUMENT;

    tuptyp = malloc(sizeof (RDB_type));
    if (tuptyp == NULL)
        return RDB_NO_MEMORY;

    tuptyp->kind = RDB_TP_TUPLE;
    tuptyp->name = NULL;

    /* Compute # of attributes */
    tuptyp->var.tuple.attrc = typ->var.basetyp->var.tuple.attrc
            + relattrp->typ->var.basetyp->var.tuple.attrc - 1;

    /* Allocate tuple attributes */
    tuptyp->var.tuple.attrv = malloc(tuptyp->var.tuple.attrc * sizeof(RDB_attr));
    if (tuptyp->var.tuple.attrv == NULL) {
        free(tuptyp);
        return RDB_NO_MEMORY;
    }

    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        tuptyp->var.tuple.attrv[i].name = NULL;
        tuptyp->var.tuple.attrv[i].typ = NULL;
    }

    /* Copy attributes from the original type */
    j = 0;
    for (i = 0; i < typ->var.basetyp->var.tuple.attrc; i++) {
        if (strcmp(typ->var.basetyp->var.tuple.attrv[i].name,
                attr) != 0) {
            ret = copy_attr(&tuptyp->var.tuple.attrv[j],
                    &typ->var.basetyp->var.tuple.attrv[i]);
            if (ret != RDB_OK)
                goto error;
            j++;
        }
    }

    /* Copy attributes from the attribute type */
    for (i = 0; i < relattrp->typ->var.basetyp->var.tuple.attrc; i++) {
        char *attrname = relattrp->typ->var.basetyp->var.tuple.attrv[i].name;

        /* Check for attribute name clash */
        if (strcmp(attrname, attr) != 0
                && _RDB_tuple_type_attr(typ->var.basetyp, attrname)
                != NULL) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }
        ret = copy_attr(&tuptyp->var.tuple.attrv[j],
                    &relattrp->typ->var.basetyp->var.tuple.attrv[i]);
        if (ret != RDB_OK)
            goto error;
        j++;
    }

    /*
     * Create relation type
     */
    (*newtypp) = malloc(sizeof(RDB_type));
    if (*newtypp == NULL) {
        ret = RDB_NO_MEMORY;
        goto error;
    }
    (*newtypp)->kind = RDB_TP_RELATION;
    (*newtypp)->name = NULL;
    (*newtypp)->var.basetyp = tuptyp;

    return RDB_OK;    

error:
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        RDB_attr *attrp = &tuptyp->var.tuple.attrv[i];
        if (attrp->name != NULL)
            free(attrp->name);
        if (attrp->typ != NULL) {
            if (attrp->typ->name == NULL)
                RDB_drop_type(attrp->typ, NULL);
        }
    }
    free(tuptyp->var.tuple.attrv);
    free(tuptyp);

    return ret;
}
