/* $Id$ */

#include "duro.h"
#include <dli/parse.h>
#include <gen/strfns.h>
#include <string.h>

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
        *tbpp = Tcl_GetHashValue(entryp);
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
tcl_to_duro(Tcl_Interp *interp, Tcl_Obj *tobjp, const RDB_type *typ,
        RDB_object *objp)
{
    int ret;

    if (typ == &RDB_STRING) {
        RDB_obj_set_string(objp, Tcl_GetStringFromObj(tobjp, NULL));
        return TCL_OK;
    }
    if (typ == &RDB_INTEGER) {
        int val;

        ret = Tcl_GetIntFromObj(interp, tobjp, &val);
        if (ret != TCL_OK)
            return ret;
        RDB_obj_set_int(objp, (RDB_int) val);
        return TCL_OK;
    }
    if (typ == &RDB_RATIONAL) {
        double val;

        ret = Tcl_GetDoubleFromObj(interp, tobjp, &val);
        if (ret != TCL_OK)
            return ret;
        RDB_obj_set_rational(objp, (RDB_rational) val);
        return TCL_OK;
    }
    if (typ == &RDB_BOOLEAN) {
        int val;

        ret = Tcl_GetBooleanFromObj(interp, tobjp, &val);
        if (ret != TCL_OK)
            return ret;
        RDB_obj_set_bool(objp, (RDB_bool) val);
        return TCL_OK;
    }    
    Tcl_SetResult(interp, "Unsupported type", TCL_STATIC);
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
        flagstr = Tcl_GetStringFromObj(objv[2], NULL);
        if ((strcmp(flagstr, "-persistent") == 0)
                || (strcmp(flagstr, "-global") == 0)) {
        } else if ((strcmp(flagstr, "-transient") == 0)
                || (strcmp(flagstr, "-local") == 0)) {
            persistent = RDB_FALSE;
        } else {
            Tcl_SetResult(interp,
                    "Wrong flag: must be -global, -persistent,"
                    " -local, or -transient", TCL_STATIC);
            return TCL_ERROR;
        }
    } else if (objc != 6) {
        Tcl_WrongNumArgs(interp, 2, objv, "?flag? name attrs keys tx");
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
    for (i = 0; i < attrc; i++)
        attrv[i].name = NULL;
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
        ret = RDB_get_type(Tcl_GetStringFromObj(typeobjp, NULL),
                txp, &attrv[i].typ);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            ret = TCL_ERROR;
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
            ret = tcl_to_duro(interp, defvalobjp, attrv[i].typ,
                    attrv[i].defaultp);
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
        flagstr = Tcl_GetStringFromObj(objv[2], NULL);
        if ((strcmp(flagstr, "-persistent") == 0)
                || (strcmp(flagstr, "-global") == 0)) {
            persistent = RDB_TRUE;
        } else if ((strcmp(flagstr, "-transient") == 0)
                || (strcmp(flagstr, "-local") == 0)) {
        } else {
            Tcl_SetResult(interp,
                    "Wrong flag: must be -global, -persistent,"
                    " -local, or -transient", TCL_STATIC);
            return TCL_ERROR;
        }
    } else if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "?flag? name expression tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = RDB_parse_table(Tcl_GetStringFromObj(objv[objc - 2], NULL), txp, &tbp);
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
        ret = RDB_make_persistent(tbp, txp);
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
        Tcl_WrongNumArgs(interp, 2, objv, "name tx");
        return TCL_ERROR;
    }


    txstr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    name = Tcl_GetStringFromObj(objv[2], NULL);
    ret = Duro_get_table(statep, interp, name, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    ret = RDB_drop_table(tbp, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return RDB_OK;
}

static int
table_update_cmd(TclState *statep, Tcl_Interp *interp, int objc,
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

    if (objc < 6) {
        Tcl_WrongNumArgs(interp, 2, objv, "name ?where? ?attribute val? ... tx");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[2], NULL);

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

    if (objc % 2 == 1) {
        /* Read where conditon */
        ret = RDB_parse_expr(Tcl_GetStringFromObj(objv[3], NULL),
                txp, &wherep);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
        upd_arg_idx = 4;
    } else {
        wherep = NULL;
        upd_arg_idx = 3;
    }

    updc = (objc - 4) / 2;
    updv = (RDB_attr_update *) Tcl_Alloc(updc * sizeof (RDB_attr_update));
    for (i = 0; i < updc; i++)
        updv[i].exp = NULL;
    for (i = 0; i < updc; i++) {
        updv[i].name = Tcl_GetStringFromObj(objv[upd_arg_idx + i * 2], NULL);
        ret = RDB_parse_expr(
                Tcl_GetStringFromObj(objv[upd_arg_idx + i * 2 + 1], NULL),
                txp, &updv[i].exp);
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

static int
table_delete_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    RDB_expression *wherep;

    if (objc < 4 || objc > 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "table ?where? tx");
        return TCL_ERROR;
    }

    name = Tcl_GetStringFromObj(objv[2], NULL);

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

    if (objc == 5) {
        /* Read where conditon */
        ret = RDB_parse_expr(Tcl_GetStringFromObj(objv[3], NULL),
                txp, &wherep);
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
table_extract_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *name;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;
    RDB_table *tbp;
    RDB_tuple tpl;
    Tcl_Obj *listobjp;

    if (objc < 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "table tx");
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

    RDB_init_tuple(&tpl);

    ret = RDB_extract_tuple(tbp, &tpl, txp);
    if (ret != RDB_OK) {
        RDB_destroy_tuple(&tpl);
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    listobjp = Duro_tuple_to_list(interp, &tpl);
    RDB_destroy_tuple(&tpl);
    if (listobjp == NULL)
        return TCL_ERROR;

    Tcl_SetObjResult(interp, listobjp);
    return TCL_OK;
}

static int
table_insert_cmd(TclState *statep, Tcl_Interp *interp, int objc,
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
    RDB_tuple tpl;
    RDB_object obj;
    RDB_type *typ;

    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "name tuple tx");
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

    RDB_init_tuple(&tpl);
    RDB_init_obj(&obj);
    for (i = 0; i < attrcount; i += 2) {
        Tcl_Obj *nameobjp, *valobjp;
        RDB_type *attrtyp;
        char *attrname;

        Tcl_ListObjIndex(interp, objv[3], i, &nameobjp);
        attrname = Tcl_GetStringFromObj(nameobjp, NULL);
        attrtyp = RDB_type_attr_type(typ, attrname);
        if (attrtyp == NULL) {
            Tcl_AppendResult(interp, "Unknown attribute: ", attrname, NULL);
            ret = TCL_ERROR;
            goto cleanup;
        }

        Tcl_ListObjIndex(interp, objv[3], i + 1, &valobjp);
        ret = tcl_to_duro(interp, valobjp, attrtyp, &obj);
        if (ret != TCL_OK)
            goto cleanup;

        RDB_tuple_set(&tpl, attrname, &obj);
    }
    ret = RDB_insert(tbp, &tpl, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        ret = TCL_ERROR;
        goto cleanup;
    }
    ret = TCL_OK;

cleanup:
    RDB_destroy_tuple(&tpl);
    RDB_destroy_obj(&obj);

    return ret;
}

int
Duro_table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "expr", "drop", "insert", "update", "delete", "extract", NULL
    };
    enum table_ix {
        create_ix, expr_ix, drop_ix, insert_ix, update_ix, delete_ix, extract_ix
    };
    int index;

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "option ?arg ...?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], sub_cmds, "option", 0, &index)
            != RDB_OK) {
        return TCL_ERROR;
    }

    switch (index) {
        case create_ix:
            return table_create_cmd(statep, interp, objc, objv);
        case expr_ix:
            return table_expr_cmd(statep, interp, objc, objv);
        case drop_ix:
            return table_drop_cmd(statep, interp, objc, objv);
        case insert_ix:
            return table_insert_cmd(statep, interp, objc, objv);
        case update_ix:
            return table_update_cmd(statep, interp, objc, objv);
        case delete_ix:
            return table_delete_cmd(statep, interp, objc, objv);
        case extract_ix:
            return table_extract_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}
