/* $Id$ */

#include "rdb.h"
#include "typeimpl.h"
#include "internal.h"
#include "catalog.h"
#include "serialize.h"
#include <gen/strfns.h>
#include <string.h>

RDB_type RDB_BOOLEAN;
RDB_type RDB_INTEGER;
RDB_type RDB_RATIONAL;
RDB_type RDB_STRING;
RDB_type RDB_BINARY;

static int
compare_int(const RDB_object *val1p, const RDB_object *val2p)
{
    return val1p->var.int_val - val2p->var.int_val;
}

static int
compare_rational(const RDB_object *val1p, const RDB_object *val2p)
{
    if (val1p->var.rational_val < val2p->var.rational_val)
        return -1;
    if (val1p->var.rational_val > val2p->var.rational_val)
        return 1;
    return 0;
}

void _RDB_init_builtin_types(void)
{
    RDB_BOOLEAN.kind = RDB_TP_SCALAR;
    RDB_BOOLEAN.arep = NULL;
    RDB_BOOLEAN.ireplen = 1;
    RDB_BOOLEAN.name = "BOOLEAN";
    RDB_BOOLEAN.var.scalar.repc = 0;
    RDB_BOOLEAN.comparep = NULL;

    RDB_STRING.kind = RDB_TP_SCALAR;
    RDB_STRING.arep = NULL;
    RDB_STRING.ireplen = RDB_VARIABLE_LEN;
    RDB_STRING.name = "STRING";
    RDB_STRING.var.scalar.repc = 0;
    RDB_STRING.comparep = NULL;

    RDB_INTEGER.kind = RDB_TP_SCALAR;
    RDB_INTEGER.arep = NULL;
    RDB_INTEGER.ireplen = sizeof (RDB_int);
    RDB_INTEGER.name = "INTEGER";
    RDB_INTEGER.var.scalar.repc = 0;
    RDB_INTEGER.comparep = &compare_int;

    RDB_RATIONAL.kind = RDB_TP_SCALAR;
    RDB_RATIONAL.arep = NULL;
    RDB_RATIONAL.ireplen = sizeof (RDB_rational);
    RDB_RATIONAL.name = "RATIONAL";
    RDB_RATIONAL.var.scalar.repc = 0;
    RDB_RATIONAL.comparep = &compare_rational;

    RDB_BINARY.kind = RDB_TP_SCALAR;
    RDB_BINARY.arep = NULL;
    RDB_BINARY.ireplen = RDB_VARIABLE_LEN;
    RDB_BINARY.name = "BINARY";
    RDB_BINARY.var.scalar.repc = 0;
    RDB_BINARY.comparep = NULL;
}

RDB_bool
RDB_type_is_numeric(const RDB_type *typ) {
    return (RDB_bool)(typ == &RDB_INTEGER
                      || typ == &RDB_RATIONAL);
}

RDB_type *
RDB_create_tuple_type(int attrc, RDB_attr attrv[])
{
    RDB_type *tbtyp;
    int i;
    
    if ((tbtyp = malloc(sizeof(RDB_type))) == NULL)
        return NULL;
    tbtyp->name = NULL;
    tbtyp->kind = RDB_TP_TUPLE;
    if ((tbtyp->var.tuple.attrv = malloc(sizeof(RDB_attr) * attrc)) == NULL) {
        free(tbtyp);
        return NULL;
    }
    for (i = 0; i < attrc; i++) {
        tbtyp->var.tuple.attrv[i].typ = attrv[i].typ;
        tbtyp->var.tuple.attrv[i].name = RDB_dup_str(attrv[i].name);
        if (attrv[i].defaultp != NULL) {
            tbtyp->var.tuple.attrv[i].defaultp = malloc(sizeof (RDB_object));
            RDB_init_obj(tbtyp->var.tuple.attrv[i].defaultp);
            RDB_copy_obj(tbtyp->var.tuple.attrv[i].defaultp,
                    attrv[i].defaultp);
        } else {
            tbtyp->var.tuple.attrv[i].defaultp = NULL;
        }
    }
    tbtyp->var.tuple.attrc = attrc;
    
    return tbtyp;
}

RDB_type *
RDB_create_relation_type(int attrc, RDB_attr attrv[])
{
    RDB_type *typ = malloc(sizeof (RDB_type));
    
    if (typ == NULL)
        return NULL;
    
    typ->name = NULL;
    typ->kind = RDB_TP_RELATION;
    if ((typ->var.basetyp = RDB_create_tuple_type(attrc, attrv))
            == NULL) {
        free(typ);
        return NULL;
    }
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
    return (typ->kind != RDB_TP_TUPLE) && (typ->kind != RDB_TP_RELATION);
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
            RDB_drop_type(typ->var.basetyp, NULL);
            break;
        default:
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
    }
    free(typ);
}    

int
RDB_get_type(const char *name, RDB_transaction *txp, RDB_type **typp)
{
    RDB_type **foundtypp;

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

    foundtypp = (RDB_type **)RDB_hashmap_get(&txp->dbp->dbrootp->typemap,
            name, NULL);
    if ((foundtypp != NULL) && (*foundtypp != NULL)) {
        *typp = *foundtypp;
        return RDB_OK;
    }
    
    return _RDB_get_cat_type(name, txp, typp);
}

int
RDB_define_type(const char *name, int repc, RDB_possrep repv[],
                RDB_transaction *txp)
{
    RDB_tuple tpl;
    RDB_object conval;
    int ret;
    int i, j;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    RDB_init_tuple(&tpl);
    RDB_init_obj(&conval);

    /*
     * Insert tuple into SYS_TYPES
     */

    ret = RDB_tuple_set_string(&tpl, "TYPENAME", name);
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "I_LIBNAME", "");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_string(&tpl, "I_AREP_TYPE", "");
    if (ret != RDB_OK)
        goto error;
    ret = RDB_tuple_set_int(&tpl, "I_AREP_LEN", -2);
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
        RDB_expression *exp = repv[i].constraintp;

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
        
        /* Check if type of constraint is RDB_BOOLEAN */
        if (exp != NULL && RDB_expr_type(exp) != &RDB_BOOLEAN) {
            ret = RDB_TYPE_MISMATCH;
            goto error;
        }

        /* Store constraint in tuple */
        ret = _RDB_expr_to_obj(exp, &conval);
        if (ret != RDB_OK)
            goto error;
        ret = RDB_tuple_set(&tpl, "I_CONSTRAINT", &conval);
        if (ret != RDB_OK)
            goto error;

        /* Store tuple */
        ret = RDB_insert(txp->dbp->dbrootp->possreps_tbp, &tpl, txp);
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

    RDB_destroy_obj(&conval);    
    RDB_destroy_tuple(&tpl);
    
    return RDB_OK;
    
error:
    RDB_destroy_obj(&conval);    
    RDB_destroy_tuple(&tpl);

    return ret;
}

int
RDB_implement_type(const char *name, const char *libname, RDB_type *arep,
                   int options, size_t areplen, RDB_transaction *txp)
{
    RDB_expression *exp, *wherep;
    RDB_attr_update upd[3];
    int ret;

    if (!RDB_tx_is_running(txp))
        return RDB_INVALID_TRANSACTION;

    if (libname != NULL) {
        if (arep == NULL) {
            if (!(RDB_FIXED_BINARY & options))
                return RDB_INVALID_ARGUMENT;
        } else {
            if (!RDB_type_is_builtin(arep))
                return RDB_NOT_SUPPORTED;
        }
    } else {
        /*
         * No libname given, so selector and getters/setters must be provided
         * by the system
         */
        RDB_table *tmptb1p;
        RDB_table *tmptb2p;
        RDB_tuple tpl;
        char *possrepname;

        if (arep != NULL)
            return RDB_INVALID_ARGUMENT;

        /* # of possreps must be one */
        ret = _RDB_possreps_query(name, txp, &tmptb1p);
        if (ret != RDB_OK)
            return ret;
        RDB_init_tuple(&tpl);
        ret = RDB_extract_tuple(tmptb1p, &tpl, txp);
        RDB_drop_table(tmptb1p, txp);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
        possrepname = RDB_tuple_get_string(&tpl, "POSSREPNAME");

        /* only 1 possrep component currently supported */
        ret = _RDB_possrepcomps_query(name, possrepname, txp, &tmptb2p);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret;
        }
        ret = RDB_extract_tuple(tmptb2p, &tpl, txp);
        RDB_drop_table(tmptb2p, txp);
        if (ret != RDB_OK) {
            RDB_destroy_tuple(&tpl);
            return ret == RDB_INVALID_ARGUMENT ? RDB_NOT_SUPPORTED : ret;
        }
        ret = RDB_get_type(RDB_tuple_get_string(&tpl, "COMPTYPENAME"), txp, &arep);
        RDB_destroy_tuple(&tpl);
        if (ret != RDB_OK) {
            return ret;
        }
        /* Empty string indicates that there is no library */
        libname = "";
    }

    exp = RDB_expr_attr("TYPENAME", &RDB_STRING);
    if (exp == NULL) {
        return RDB_NO_MEMORY;
    }
    wherep = RDB_eq(exp, RDB_string_const(name));
    if (wherep == NULL) {
        RDB_drop_expr(exp);
        return RDB_NO_MEMORY;
    }    

    upd[0].exp = upd[1].exp = NULL;

    upd[0].name = "I_AREP_TYPE";
    upd[0].exp = RDB_string_const(arep != NULL ? arep->name : "");
    if (upd[0].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    upd[1].name = "I_AREP_LEN";
    upd[1].exp = RDB_int_const(options & RDB_FIXED_BINARY ? areplen : arep->ireplen);
    if (upd[1].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }
    upd[2].name = "I_LIBNAME";
    upd[2].exp = RDB_string_const(libname);
    if (upd[2].exp == NULL) {
        ret = RDB_NO_MEMORY;
        goto cleanup;
    }

    ret = RDB_update(txp->dbp->dbrootp->types_tbp, wherep, 3, upd, txp);

cleanup:    
    if (upd[0].exp != NULL)
        RDB_drop_expr(upd[0].exp);
    if (upd[1].exp != NULL)
        RDB_drop_expr(upd[1].exp);
    if (upd[2].exp != NULL)
        RDB_drop_expr(upd[2].exp);
    RDB_drop_expr(wherep);

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

        /* Delete type from type table by puting a NULL pointer into it */
        ret = RDB_hashmap_put(&txp->dbp->dbrootp->typemap, typ->name, &ntp,
                sizeof (ntp));
        if (ret != RDB_OK) {
            if (RDB_is_syserr(ret))
                RDB_rollback(txp);
            return ret;
        }

        /* Delete type from database */
        wherep = RDB_eq(RDB_expr_attr("TYPENAME", &RDB_STRING),
                        RDB_string_const(typ->name));
        if (wherep == NULL) {
            RDB_rollback(txp);
            return RDB_NO_MEMORY;
        }
        ret = RDB_delete(txp->dbp->dbrootp->types_tbp, wherep, txp);
        if (ret != RDB_OK) {
            RDB_drop_expr(wherep);
            return ret;
        }
        ret = RDB_delete(txp->dbp->dbrootp->possreps_tbp, wherep, txp);
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
RDB_extend_tuple_type(const RDB_type *typ, int attrc, RDB_attr attrv[])
{
    int i;
    RDB_type *newtyp = malloc(sizeof (RDB_type));
    
    if (newtyp == NULL)
        return NULL;
    newtyp->name = NULL;
    newtyp->kind = RDB_TP_TUPLE;
    newtyp->var.tuple.attrc = typ->var.tuple.attrc + attrc;
    newtyp->var.tuple.attrv = malloc(sizeof (RDB_attr)
            * (newtyp->var.tuple.attrc));
    if (newtyp->var.tuple.attrv == NULL) {
        free(newtyp);
        return NULL;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        newtyp->var.tuple.attrv[i].name = NULL;
    }
    for (i = 0; i < typ->var.tuple.attrc; i++) {
        newtyp->var.tuple.attrv[i].name =
                RDB_dup_str(typ->var.tuple.attrv[i].name);
        if (newtyp->var.tuple.attrv[i].name == NULL)
            goto error;
        newtyp->var.tuple.attrv[i].typ = typ->var.tuple.attrv[i].typ;
        newtyp->var.tuple.attrv[i].defaultp = NULL;
    }
    for (i = 0; i < attrc; i++) {
        newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].name =
                RDB_dup_str(attrv[i].name);
        newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].typ =
                attrv[i].typ;
        newtyp->var.tuple.attrv[typ->var.tuple.attrc + i].defaultp = NULL;
    }
    return newtyp;    

error:
    for (i = 0; i < typ->var.tuple.attrc; i++)
        free(newtyp->var.tuple.attrv[i].name);
    free(newtyp->var.tuple.attrv);
    free(newtyp);
    return NULL;
}

RDB_type *
RDB_extend_relation_type(const RDB_type *typ, int attrc, RDB_attr attrv[])
{
    RDB_type *restyp;

    restyp = malloc(sizeof (RDB_type));
    if (restyp == NULL) {
        return NULL;
    }
    restyp->name = NULL;
    restyp->kind = RDB_TP_RELATION;
    restyp->var.basetyp = RDB_extend_tuple_type(
            typ->var.basetyp, attrc, attrv);
    return restyp;
}

int
RDB_join_tuple_types(const RDB_type *typ1, const RDB_type *typ2, RDB_type **newtypp)
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
                typ1->var.tuple.attrv[i].typ;
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
                    typ2->var.tuple.attrv[i].typ;
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

/* Return the type of the attribute with name attrname in the tuple
   type pointed to by tutyp. */
RDB_attr *_RDB_tuple_type_attr(const RDB_type *tuptyp, const char *attrname)
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
        char *attrname;
        RDB_type *attrtyp;
    
        attrname  = RDB_dup_str(attrv[i]);
        if (attrname == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        tuptyp->var.tuple.attrv[i].name = attrname;

        attrtyp = _RDB_tuple_type_attr(typ, attrname)->typ;
        if (attrtyp == NULL) {
            ret = RDB_INVALID_ARGUMENT;
            goto error;
        }
        tuptyp->var.tuple.attrv[i].typ = attrtyp;
        tuptyp->var.tuple.attrv[i].defaultp = NULL;
        tuptyp->var.tuple.attrv[i].options = 0;
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
_RDB_find_rename_from(int renc, RDB_renaming renv[], const char *name)
{
    int i;

    for (i = 0; i < renc && strcmp(renv[i].from, name) != 0; i++);
    if (i >= renc)
        return -1; /* not found */
    /* found */
    return i;
}

int
RDB_rename_tuple_type(const RDB_type *typ, int renc, RDB_renaming renv[],
        RDB_type **newtypp)
{
    RDB_type *newtyp;
    int i;
    int ret;

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
        if (ai >= 0)
            newtyp->var.tuple.attrv[i].name = RDB_dup_str(renv[ai].to);
        else
            newtyp->var.tuple.attrv[i].name = RDB_dup_str(attrname);
        if (newtyp->var.tuple.attrv[i].name == NULL) {
            ret = RDB_NO_MEMORY;
            goto error;
        }
        newtyp->var.tuple.attrv[i].defaultp = NULL;
        newtyp->var.tuple.attrv[i].options = 0;
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
RDB_rename_relation_type(const RDB_type *typ, int renc, RDB_renaming renv[],
        RDB_type **newtypp)
{
    int ret;

    *newtypp = malloc(sizeof (RDB_type));
    if (*newtypp == NULL)
        return RDB_NO_MEMORY;

    (*newtypp)->name = NULL;
    (*newtypp)->kind = RDB_TP_TUPLE;

    ret = RDB_rename_tuple_type(typ->var.basetyp, renc, renv,
            &(*newtypp)->var.basetyp);
    if (ret != RDB_OK) {
        free(*newtypp);
        return ret;
    }
    return RDB_OK;
}

RDB_ipossrep *
_RDB_get_possrep(RDB_type *typ, const char *repname)
{
    int i;

    for (i = 0; i < typ->var.scalar.repc
            && strcmp(typ->var.scalar.repv[i].name, repname) != 0;
            i++);
    if (i >= typ->var.scalar.repc)
        return NULL;
    return &typ->var.scalar.repv[i];
}

RDB_icomp *
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
