/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <dli/parse.h>
#include <gen/strfns.h>
#include <string.h>
#include <ctype.h>
#include <rel/internal.h>

int
Duro_tcl_drop_ltable(table_entry *tbep, Tcl_HashEntry *entryp)
{
    int ret;

    Tcl_DeleteHashEntry(entryp);
    ret = RDB_drop_table(tbep->tablep, NULL);
    Tcl_Free((char *) tbep);
    return ret;
}

int
Duro_get_table(TclState *statep, Tcl_Interp *interp, const char *name,
          RDB_transaction *txp, RDB_table **tbpp)
{
    Tcl_HashEntry *entryp;
    int ret;

    /*
     * Search for transient table first
     */
    entryp = Tcl_FindHashEntry(&statep->ltables, name);
    if (entryp != NULL) {
        /* Found */
        *tbpp = ((table_entry *)Tcl_GetHashValue(entryp))->tablep;
        return TCL_OK;
    }

    /*
     * Search for persistent table
     */
    ret = RDB_get_table(name, txp, tbpp);
    if (ret == RDB_NOT_FOUND) {
        Tcl_AppendResult(interp, "Unknown table: ", name, NULL);
        return TCL_ERROR;
    }
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }
    return TCL_OK;
}

static int
list_to_tuple(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_object *tplp, RDB_transaction *txp)
{
    int ret;
    int llen;
    int i;

    ret = Tcl_ListObjLength(interp, tobjp, &llen);
    if (ret != TCL_OK)
        return ret;

    if (llen % 2 != 0) {
        Tcl_SetResult(interp, "Invalid tuple value", TCL_STATIC);
        return TCL_ERROR;
    }
    for (i = 0; i < llen; i += 2) {
        Tcl_Obj *attrp, *valuep;
        char *attrname;
        RDB_type *attrtyp;
        RDB_object obj;

        /* Get attribute name and type */
        Tcl_ListObjIndex(interp, tobjp, i, &attrp);
        attrname = Tcl_GetString(attrp);
        attrtyp = RDB_type_attr_type(typ, attrname);
        if (attrtyp == NULL) {
            Tcl_AppendResult(interp, "Invalid attribute: ", attrname, NULL);
            return TCL_ERROR;
        }

        /* Get attribute value */
        Tcl_ListObjIndex(interp, tobjp, i + 1, &valuep);

        /* Convert value to RDB_object and set tuple attribute */
        RDB_init_obj(&obj);
        ret = Duro_tcl_to_duro(interp, valuep, attrtyp, &obj, txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(&obj);
            return ret;
        }
        ret = RDB_tuple_set(tplp, attrname, &obj);

        RDB_destroy_obj(&obj);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
    }

    return TCL_OK;
}

static int
list_to_table(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_table **tbpp, RDB_transaction *txp)
{
    int ret;
    int i;
    int llen;
    Tcl_Obj *tplobjp;
    RDB_object tpl;

    ret = RDB_create_table(NULL, RDB_FALSE,
                        typ->var.basetyp->var.tuple.attrc,
                        typ->var.basetyp->var.tuple.attrv,
                        0, NULL, NULL, tbpp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    ret = Tcl_ListObjLength(interp, tobjp, &llen);
    if (ret != TCL_OK)
        return ret;

    RDB_init_obj(&tpl);
    for (i = 0; i < llen; i++) {
        Tcl_ListObjIndex(interp, tobjp, i, &tplobjp);

        ret = list_to_tuple(interp, tplobjp, typ->var.basetyp, &tpl, txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(&tpl);
            return ret;
        }

        ret = RDB_insert(*tbpp, &tpl, NULL);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
    }
    RDB_destroy_obj(&tpl);   

    return TCL_OK;
}

int
call_selector(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
       RDB_object *objp, RDB_transaction *txp)
{
    int ret;
    int i;
    int llen;
    Tcl_Obj *nametobjp;
    RDB_object **argpv;
    RDB_object *argv;
    RDB_ipossrep *irep;
    char *selname;

    ret = Tcl_ListObjLength(interp, tobjp, &llen);
    if (ret != TCL_OK)
        return ret;

    if (llen == 0) {
        Tcl_SetResult(interp, "Invalid type representation", TCL_STATIC);
        return TCL_ERROR;
    }

    ret = Tcl_ListObjIndex(interp, tobjp, 0, &nametobjp);
    if (ret != TCL_OK)
        return ret;

    selname = Tcl_GetString(nametobjp);
    irep = _RDB_get_possrep(typ, selname);
    if (irep == NULL) {
        Tcl_AppendResult(interp, "Invalid selector: ", selname, NULL);
        return TCL_ERROR;
    }

    argv = (RDB_object *) Tcl_Alloc(sizeof(RDB_object) * (llen - 1));
    argpv = (RDB_object **) Tcl_Alloc(sizeof(RDB_object *) * (llen - 1));

    for (i = 0; i < llen - 1; i++)
        RDB_init_obj(&argv[i]);

    for (i = 0; i < llen - 1; i++) {
        Tcl_Obj *argtobjp;

        ret = Tcl_ListObjIndex(interp, tobjp, i + 1, &argtobjp);
        if (ret != TCL_OK)
            goto cleanup;

        ret = Duro_tcl_to_duro(interp, argtobjp, irep->compv[i].typ,
                &argv[i], txp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            ret = TCL_ERROR;
            goto cleanup;
        }
        argpv[i] = &argv[i];
    }

    ret = RDB_call_ro_op(Tcl_GetString(nametobjp), llen - 1, argpv, txp, objp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
        goto cleanup;
    }

    ret = TCL_OK;

cleanup:
    for (i = 0; i < llen - 1; i++)
        RDB_destroy_obj(&argv[i]);
    Tcl_Free((char *) argv);
    Tcl_Free((char *) argpv);
    return ret;
}

int
Duro_tcl_to_duro(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_object *objp, RDB_transaction *txp)
{
    int ret;

    if (typ == &RDB_STRING) {
        RDB_string_to_obj(objp, Tcl_GetString(tobjp));
        return TCL_OK;
    }
    if (typ == &RDB_INTEGER) {
        int val;

        ret = Tcl_GetIntFromObj(interp, tobjp, &val);
        if (ret != TCL_OK)
            return ret;
        RDB_int_to_obj(objp, (RDB_int) val);
        return TCL_OK;
    }
    if (typ == &RDB_RATIONAL) {
        double val;

        ret = Tcl_GetDoubleFromObj(interp, tobjp, &val);
        if (ret != TCL_OK)
            return ret;
        RDB_rational_to_obj(objp, (RDB_rational) val);
        return TCL_OK;
    }
    if (typ == &RDB_BOOLEAN) {
        int val;

        ret = Tcl_GetBooleanFromObj(interp, tobjp, &val);
        if (ret != TCL_OK)
            return ret;
        RDB_bool_to_obj(objp, (RDB_bool) val);
        return TCL_OK;
    }
    if (typ->kind == RDB_TP_TUPLE) {
        return list_to_tuple(interp, tobjp, typ, objp, txp);
    }
    if (typ->kind == RDB_TP_RELATION) {
        RDB_table *tbp;

        ret = list_to_table(interp, tobjp, typ, &tbp, txp);
        if (ret != TCL_OK)
            return ret;
        RDB_table_to_obj(objp, tbp);
        return TCL_OK;
    }
    if (RDB_type_is_scalar(typ)) {
        return call_selector(interp, tobjp, typ, objp, txp);
    }
    Tcl_AppendResult(interp, "Unsupported type", TCL_STATIC);
    return TCL_ERROR;
}

static void
add_table(TclState *statep, RDB_table *tbp, RDB_environment *envp)
{
    int new;
    Tcl_HashEntry *entryp;
    table_entry *tbep = (table_entry *) Tcl_Alloc(sizeof (table_entry));

    statep->ltable_uid++;
    entryp = Tcl_CreateHashEntry(&statep->ltables, RDB_table_name(tbp), &new);

    tbep->tablep = tbp;
    tbep->envp = envp;
    
    Tcl_SetHashValue(entryp, (ClientData)tbep);
}

static RDB_bool
contains_space(const char *s)
{
    for (; *s != '\0'; s++) {
        if (isspace(*s)) {
            return RDB_TRUE;
        }
    }
    return RDB_FALSE;
}    

int
Duro_get_type(Tcl_Obj *objp, Tcl_Interp *interp, RDB_transaction *txp,
         RDB_type **typp)
{
    int ret;

    /*
     * Check for a tuple or relation type by searching for whitespace
     */
    if (contains_space(Tcl_GetString(objp))) {
        int llen;
        int attrc;
        int i;
        RDB_attr *attrv;
        Tcl_Obj *elemp;
        RDB_bool istuple;

        Tcl_ListObjLength(interp, objp, &llen);
        if (llen == 0) {
            Tcl_SetResult(interp, "Invalid type", TCL_STATIC);
            return TCL_ERROR;
        }

        ret = Tcl_ListObjIndex(interp, objp, 0, &elemp);
        if (ret != TCL_OK)
            return ret;

        if (strcmp(Tcl_GetString(elemp), "tuple") == 0) {
            istuple = RDB_TRUE;
        } else if (strcmp(Tcl_GetString(elemp), "relation") == 0) {
            istuple = RDB_FALSE;
        } else {
            Tcl_SetResult(interp, "Invalid type", TCL_STATIC);
            return TCL_ERROR;
        }

        attrc = llen - 1;
        attrv = malloc(attrc * sizeof (RDB_attr));
        for (i = 0; i < attrc; i++) {
            Tcl_Obj *elem2p;

            Tcl_ListObjIndex(interp, objp, i + 1, &elemp);

            /* Get attribute name and type */
            Tcl_ListObjLength(interp, elemp, &llen);
            if (llen != 2) {
                Tcl_SetResult(interp, "Invalid attribute definition", TCL_STATIC);
                free(attrv);
                return TCL_ERROR;
            }

            Tcl_ListObjIndex(interp, elemp, 0, &elem2p);
            attrv[i].name = Tcl_GetString(elem2p);

            Tcl_ListObjIndex(interp, elemp, 1, &elem2p);
            ret = Duro_get_type(elem2p, interp, txp, &attrv[i].typ);
            if (ret != TCL_OK) {
                free(attrv);
                return ret;
            }
            
            attrv[i].defaultp = NULL;
        }
        if (istuple) {
            ret = RDB_create_tuple_type(attrc, attrv, typp);
        } else {
            ret = RDB_create_relation_type(attrc, attrv, typp);
        }
        free(attrv);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
        return TCL_OK;
    }

    ret = RDB_get_type(Tcl_GetString(objp), txp, typp);
    if (ret != RDB_OK) {
        if (ret == RDB_NOT_FOUND) {
            Tcl_AppendResult(interp, "Invalid type \"", Tcl_GetString(objp),
                             "\"", NULL);
        } else {
            Duro_dberror(interp, ret);
        }
        return TCL_ERROR;
    }
    return TCL_OK;
}

static int
table_create_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    int attrc, keyc;
    int i, j;
    char *flagstr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_table *tbp;
    RDB_attr *attrv = NULL;
    RDB_string_vec *keyv = NULL;
    RDB_bool persistent = RDB_TRUE;

    if (objc == 7) {
        /*
         * Read flag
         */
        flagstr = Tcl_GetString(objv[2]);
        if (strcmp(flagstr, "-global") == 0) {
        } else if (strcmp(flagstr, "-local") == 0) {
            persistent = RDB_FALSE;
        } else {
            Tcl_SetResult(interp,
                    "Wrong flag: must be -global or -local", TCL_STATIC);
            return TCL_ERROR;
        }
    } else if (objc != 6) {
        Tcl_WrongNumArgs(interp, 2, objv, "?flag? tablename attrs keys tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Tcl_ListObjLength(interp, objv[objc - 3], &attrc);
    if (ret != TCL_OK)
        goto cleanup;
    attrv = (RDB_attr *) Tcl_Alloc(sizeof (RDB_attr) * attrc);
    for (i = 0; i < attrc; i++) {
        attrv[i].name = NULL;
        attrv[i].defaultp = NULL;
    }
    /*
     * Read attribute defs
     */
    for (i = 0; i < attrc; i++) {
        Tcl_Obj *attrobjp, *nameobjp, *typeobjp;
        int llen;
    
        ret = Tcl_ListObjIndex(interp, objv[objc - 3], i, &attrobjp);
        if (ret != TCL_OK)
            goto cleanup;

        ret = Tcl_ListObjLength(interp, attrobjp, &llen);
        if (ret != TCL_OK)
            goto cleanup;
        if (llen < 2 || llen > 4) {
            Tcl_SetResult(interp, "Invalid attribute definition", TCL_STATIC);
            ret = TCL_ERROR;
            goto cleanup;
        }
        
        Tcl_ListObjIndex(interp, attrobjp, 0, &nameobjp);
        Tcl_ListObjIndex(interp, attrobjp, 1, &typeobjp);
        attrv[i].name = RDB_dup_str(Tcl_GetStringFromObj(nameobjp, NULL));
        ret = Duro_get_type(typeobjp, interp, txp, &attrv[i].typ);
        if (ret != TCL_OK) {
            goto cleanup;
        }
        if (llen >= 3) {
            /*
             * Read default value
             */
            Tcl_Obj *defvalobjp;

            attrv[i].defaultp = (RDB_object *) Tcl_Alloc(sizeof (RDB_object));
            RDB_init_obj(attrv[i].defaultp);
            Tcl_ListObjIndex(interp, attrobjp, 2, &defvalobjp);
            ret = Duro_tcl_to_duro(interp, defvalobjp, attrv[i].typ,
                    attrv[i].defaultp, txp);
            if (ret != RDB_OK) {
                goto cleanup;
            }
        } else {
            attrv[i].defaultp = NULL;
        }
        attrv[i].options = 0;
    }

    ret = Tcl_ListObjLength(interp, objv[objc - 2], &keyc);
    if (ret != TCL_OK)
        goto cleanup;
    keyv = (RDB_string_vec *) Tcl_Alloc(sizeof (RDB_string_vec) * keyc);
    for (i = 0; i < keyc; i++) {
        Tcl_Obj *keyobjp;
        Tcl_Obj *keyattrobjp;

        Tcl_ListObjIndex(interp, objv[objc - 2], i, &keyobjp);
        Tcl_ListObjLength(interp, keyobjp, &keyv[i].strc);
        keyv[i].strv = (char **) Tcl_Alloc(sizeof (char *) * keyv[i].strc);
        for (j = 0; j < keyv[i].strc; j++) {
            Tcl_ListObjIndex(interp, keyobjp, j, &keyattrobjp);
            keyv[i].strv[j] = RDB_dup_str(Tcl_GetStringFromObj(keyattrobjp, NULL));
        }
    }
    
    ret = RDB_create_table(Tcl_GetStringFromObj(objv[objc - 4], NULL), persistent,
            attrc, attrv, keyc, keyv, txp, &tbp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
        goto cleanup;
    }

    if (!persistent) {
        add_table(statep, tbp, RDB_db_env(RDB_tx_db(txp)));
    }

cleanup:
    if (attrv != NULL) {
        for (i = 0; i < attrc; i++) {
            free(attrv[i].name);
            if (attrv[i].defaultp != NULL) {
                RDB_destroy_obj(attrv[i].defaultp);
                Tcl_Free((char *) attrv[i].defaultp);
            }
        }
        Tcl_Free((char *) attrv);
    }
    if (keyv != NULL) {
        for (i = 0; i < keyc; i++) {
            for (j = 0; j < keyv[i].strc; j++)
                free(keyv[i].strv[j]);
            Tcl_Free((char *) keyv[i].strv);
        }
        Tcl_Free((char *) keyv);
    }
                
    return ret;
}

static int
table_expr_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *flagstr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_table *tbp;
    RDB_bool persistent = RDB_FALSE;

    if (objc == 6) {
        flagstr = Tcl_GetString(objv[2]);
        if (strcmp(flagstr, "-global") == 0) {
            persistent = RDB_TRUE;
        } else if (strcmp(flagstr, "-local") == 0) {
        } else {
            Tcl_SetResult(interp,
                    "Wrong flag: must be -global or -local", TCL_STATIC);
            return TCL_ERROR;
        }
    } else if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "?flag? tablename expression tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = RDB_parse_table(Tcl_GetStringFromObj(objv[objc - 2], NULL),
            &Duro_get_ltable, statep, txp, &tbp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    ret = RDB_set_table_name(tbp, Tcl_GetStringFromObj(objv[objc - 3], NULL),
            txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    if (persistent) {
        ret = RDB_add_table(tbp, txp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
    } else {
        add_table(statep, tbp, RDB_db_env(RDB_tx_db(txp)));
    }

    return ret;
}

static int
table_drop_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "tablename tx");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[2], NULL);
    txstr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    if (tbp->is_persistent) {
        ret = RDB_drop_table(tbp, txp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
    } else {
        entryp = Tcl_FindHashEntry(&statep->ltables, name);
        ret = Duro_tcl_drop_ltable((table_entry *)Tcl_GetHashValue(entryp),
                entryp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
    }

    return TCL_OK;
}

int
Duro_update_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    int i;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    int updc;
    RDB_attr_update *updv;
    RDB_expression *wherep;
    int upd_arg_idx;
    TclState *statep = (TclState *) data;

    if (objc < 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "tablename ?where? ?attrname expression ...? tx");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[1], NULL);

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    if (objc % 2 == 0) {
        /* Read where conditon */
        ret = RDB_parse_expr(Tcl_GetStringFromObj(objv[2], NULL),
                Duro_get_ltable, statep, txp, &wherep);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
        upd_arg_idx = 3;
    } else {
        wherep = NULL;
        upd_arg_idx = 2;
    }

    updc = (objc - 3) / 2;
    updv = (RDB_attr_update *) Tcl_Alloc(updc * sizeof (RDB_attr_update));
    for (i = 0; i < updc; i++)
        updv[i].exp = NULL;
    for (i = 0; i < updc; i++) {
        updv[i].name = Tcl_GetStringFromObj(objv[upd_arg_idx + i * 2], NULL);
        ret = RDB_parse_expr(
                Tcl_GetStringFromObj(objv[upd_arg_idx + i * 2 + 1], NULL),
                &Duro_get_ltable, statep, txp, &updv[i].exp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            ret = TCL_ERROR;
            goto cleanup;
        }
    }
    ret = RDB_update(tbp, wherep, updc, updv, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
        goto cleanup;
    }

    ret = TCL_OK;

cleanup:
    for (i = 0; i < updc; i++) {
        if (updv[i].exp != NULL)
            RDB_drop_expr(updv[i].exp);
    }
    Tcl_Free((char *) updv);
    return ret;                    
}

int
Duro_delete_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    int ret;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    RDB_expression *wherep;
    TclState *statep = (TclState *) data;

    if (objc < 3 || objc > 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "table ?where? tx");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[1], NULL);

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    if (objc == 4) {
        /* Read where-conditon */
        ret = RDB_parse_expr(Tcl_GetStringFromObj(objv[2], NULL),
                &Duro_get_ltable, statep, txp, &wherep);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
    } else {
        wherep = NULL;
    }

    ret = RDB_delete(tbp, wherep, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

static int
table_contains_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    int attrcount;
    int i;
    RDB_object tpl;
    RDB_type *typ;

    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "tablename tuple tx");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[2], NULL);

    txstr = Tcl_GetStringFromObj(objv[4], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }
    typ = RDB_table_type(tbp);

    Tcl_ListObjLength(interp, objv[3], &attrcount);
    if (attrcount % 2 != 0) {
        Tcl_SetResult(interp, "Invalid tuple value", TCL_STATIC);
        return TCL_ERROR;
    } 

    RDB_init_obj(&tpl);
    for (i = 0; i < attrcount; i += 2) {
        Tcl_Obj *nameobjp, *valobjp;
        RDB_type *attrtyp;
        char *attrname;
        RDB_object obj;

        RDB_init_obj(&obj);

        Tcl_ListObjIndex(interp, objv[3], i, &nameobjp);
        attrname = Tcl_GetStringFromObj(nameobjp, NULL);
        attrtyp = RDB_type_attr_type(typ, attrname);
        if (attrtyp == NULL) {
            Tcl_AppendResult(interp, "Invalid attribute: ", attrname, NULL);
            ret = TCL_ERROR;
            RDB_destroy_obj(&obj);
            goto cleanup;
        }

        Tcl_ListObjIndex(interp, objv[3], i + 1, &valobjp);
        ret = Duro_tcl_to_duro(interp, valobjp, attrtyp, &obj, txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(&obj);
            goto cleanup;
        }

        RDB_tuple_set(&tpl, attrname, &obj);

        RDB_destroy_obj(&obj);
    }

    ret = RDB_table_contains(tbp, &tpl, txp);
    if (ret == RDB_OK) {
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(1));
        ret = TCL_OK;        
    } else if (ret == RDB_NOT_FOUND) {
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(0));
        ret = TCL_OK;
    } else {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
    }

cleanup:
    RDB_destroy_obj(&tpl);

    return ret;
}

static int
table_add_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "tablename tx");
        return TCL_ERROR;
    }

    name = Tcl_GetString(objv[2]);

    txstr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    ret = RDB_add_table(tbp, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

static Tcl_Obj *
type_to_tobj(const RDB_type *typ)
{
    char *name = RDB_type_name(typ);

    if (name != NULL)
        return Tcl_NewStringObj(name, strlen(name));
    return Tcl_NewStringObj("", 0);
}

static int
table_attrs_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    int i;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    RDB_type *tuptyp;
    Tcl_Obj *listobjp;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "tablename tx");
        return TCL_ERROR;
    }

    name = Tcl_GetString(objv[2]);
    txstr = Tcl_GetString(objv[3]);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }
    tuptyp = RDB_table_type(tbp)->var.basetyp;

    listobjp = Tcl_NewListObj(0, NULL);
    for (i = 0; i < tuptyp->var.tuple.attrc; i++) {
        char *name = tuptyp->var.tuple.attrv[i].name;

        ret = Tcl_ListObjAppendElement(interp, listobjp,
                Tcl_NewStringObj(name, strlen(name)));
        if (ret != TCL_OK)
            return ret;
        ret = Tcl_ListObjAppendElement(interp, listobjp,
                type_to_tobj(tuptyp->var.tuple.attrv[i].typ));
        if (ret != TCL_OK)
            return ret;
    }

    Tcl_SetObjResult(interp, listobjp);
    return TCL_OK;
}

static int
table_keys_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    int i, j;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    int keyc;
    RDB_string_vec *keyv;
    Tcl_Obj *listobjp;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "tablename tx");
        return TCL_ERROR;
    }

    name = Tcl_GetString(objv[2]);
    txstr = Tcl_GetString(objv[3]);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }
    keyc = RDB_table_keys(tbp, &keyv);
    if (keyc < 0) {
        Duro_dberror(interp, keyc);
        return TCL_ERROR;
    }

    listobjp = Tcl_NewListObj(0, NULL);
    for (i = 0; i < keyc; i++) {
        Tcl_Obj *ilistobjp = Tcl_NewListObj(0, NULL);

        for (j = 0; j < keyv[i].strc; j++) {
            ret = Tcl_ListObjAppendElement(interp, ilistobjp,
               Tcl_NewStringObj(keyv[i].strv[j], strlen(keyv[i].strv[j])));
            if (ret != TCL_OK)
                return ret;
        }
        ret = Tcl_ListObjAppendElement(interp, listobjp, ilistobjp);
        if (ret != TCL_OK)
            return ret;
    }

    Tcl_SetObjResult(interp, listobjp);
    return TCL_OK;
}

int
Duro_table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "drop", "expr", "contains", "add", "attrs", "keys", NULL
    };
    enum table_ix {
        create_ix, drop_ix, expr_ix, contains_ix, add_ix, attrs_ix, keys_ix
    };
    int index;

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "arg ?arg ...?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], sub_cmds, "option", 0, &index)
            != TCL_OK) {
        return TCL_ERROR;
    }

    switch (index) {
        case create_ix:
            return table_create_cmd(statep, interp, objc, objv);
        case drop_ix:
            return table_drop_cmd(statep, interp, objc, objv);
        case expr_ix:
            return table_expr_cmd(statep, interp, objc, objv);
        case contains_ix:
            return table_contains_cmd(statep, interp, objc, objv);
        case add_ix:
            return table_add_cmd(statep, interp, objc, objv);
        case attrs_ix:
            return table_attrs_cmd(statep, interp, objc, objv);
        case keys_ix:
            return table_keys_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}
