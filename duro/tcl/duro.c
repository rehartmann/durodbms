/*
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <rel/rdb.h>
#include <rel/internal.h>
#include <dli/parse.h>
#include <string.h>
#include <stdio.h>
#include <locale.h>

extern int yydebug;

static void
duro_cleanup(ClientData data)
{
    RDB_environment *envp;
    RDB_transaction *txp;
    RDB_object *arrayp;
    Tcl_HashEntry *entryp;
    Tcl_HashSearch search;
    TclState *statep = (TclState *) data;

    /* Destroy existing arrays */
    entryp = Tcl_FirstHashEntry(&statep->arrays, &search);
    while (entryp != NULL) {
        arrayp = Tcl_GetHashValue(entryp);
        Duro_tcl_drop_array(arrayp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->arrays, &search);
    }

    /* Abort pending transactions */
    entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    while (entryp != NULL) {
        txp = Tcl_GetHashValue(entryp);
        Duro_tcl_rollback(txp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    }

    /* Close open DB environments */
    entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    while (entryp != NULL) {
        envp = Tcl_GetHashValue(entryp);
        Duro_tcl_close_env(statep, envp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    }

    Tcl_Free((char *) statep);
}

static const char *
errcode_str(int err)
{
    switch (err) {
        case RDB_OK:
            return "RDB_OK";

        case RDB_NO_SPACE:
            return "RDB_NO_SPACE";
        case RDB_NO_MEMORY:
            return "RDB_NO_MEMORY";
        case RDB_SYSTEM_ERROR:
            return "RDB_SYSTEM_ERROR";
        case RDB_DEADLOCK:
            return "RDB_DEADLOCK";
        case RDB_INTERNAL:
            return "RDB_INTERNAL";
        case RDB_RESOURCE_NOT_FOUND:
            return "RDB_RESOURCE_NOT_FOUND";

        case RDB_INVALID_ARGUMENT:
            return "RDB_INVALID_ARGUMENT";
        case RDB_NOT_FOUND:
            return "RDB_NOT_FOUND";
        case RDB_INVALID_TRANSACTION:
            return "RDB_INVALID_TRANSACTION";
        case RDB_ELEMENT_EXISTS:
            return "RDB_ELEMENT_EXISTS";
        case RDB_TYPE_MISMATCH:
            return "RDB_TYPE_MISMATCH";
        case RDB_KEY_VIOLATION:
            return "RDB_KEY_VIOLATION";
        case RDB_PREDICATE_VIOLATION:
            return "RDB_PREDICATE_VIOLATION";
        case RDB_AGGREGATE_UNDEFINED:
            return "RDB_AGGREGATE_UNDEFINED";
        case RDB_TYPE_CONSTRAINT_VIOLATION:
            return "RDB_TYPE_CONSTRAINT_VIOLATION";
        case RDB_ATTRIBUTE_NOT_FOUND:
            return "RDB_ATTRIBUTE_NOT_FOUND";
        case RDB_OPERATOR_NOT_FOUND:
            return "RDB_OPERATOR_NOT_FOUND";
        case RDB_SYNTAX:
            return "RDB_SYNTAX";

        case RDB_NOT_SUPPORTED:
            return "RDB_NOT_SUPPORTED";
    }
    return NULL;
}

void
Duro_dberror(Tcl_Interp *interp, int err)
{
    const char *errcode = errcode_str(err);

    Tcl_AppendResult(interp, "database error: ", (char *) RDB_strerror(err),
            TCL_STATIC);
    if (errcode != NULL)
        Tcl_SetErrorCode(interp, "Duro", errcode, (char *) RDB_strerror(err),
                NULL);
}    

int
Duro_init_tcl(Tcl_Interp *interp, TclState **statepp)
{
    if (Tcl_InitStubs(interp, "8.1", 0) == NULL) {
        return TCL_ERROR;
    }

    setlocale(LC_COLLATE, "");
    setlocale(LC_CTYPE, "");

    *statepp = (TclState *) Tcl_Alloc(sizeof (TclState));
    Tcl_InitHashTable(&(*statepp)->envs, TCL_STRING_KEYS);
    (*statepp)->env_uid = 0;
    Tcl_InitHashTable(&(*statepp)->txs, TCL_STRING_KEYS);
    (*statepp)->tx_uid = 0;
    Tcl_InitHashTable(&(*statepp)->arrays, TCL_STRING_KEYS);
    (*statepp)->array_uid = 0;
    Tcl_InitHashTable(&(*statepp)->ltables, TCL_STRING_KEYS);
    (*statepp)->ltable_uid = 0;

    Tcl_CreateCommand(interp, "duro::env", Duro_env_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateCommand(interp, "duro::begin", Duro_begin_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateCommand(interp, "duro::commit", Duro_commit_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateCommand(interp, "duro::rollback", Duro_rollback_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateCommand(interp, "duro::txdb", Duro_txdb_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateCommand(interp, "duro::db", Duro_db_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::table", Duro_table_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::insert", Duro_insert_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::update", Duro_update_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::delete", Duro_delete_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::array", Duro_array_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::operator", Duro_operator_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::call", Duro_call_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::expr", Duro_expr_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::type", Duro_type_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::index", Duro_index_cmd,
            (ClientData)*statepp, NULL);
    Tcl_CreateObjCommand(interp, "duro::constraint", Duro_constraint_cmd,
            (ClientData)*statepp, NULL);

    Tcl_CreateExitHandler(duro_cleanup, (ClientData)*statepp);

    return Tcl_PkgProvide(interp, "duro", "0.10");
}

int
Durotcl_Init(Tcl_Interp *interp)
{
    TclState *statep;

    /* yydebug = 1; */

    return Duro_init_tcl(interp, &statep);
}

RDB_table *
Duro_get_ltable(const char *name, void *arg)
{
    TclState *statep = (TclState *)arg;
    Tcl_HashEntry *entryp = Tcl_FindHashEntry(&statep->ltables, name);

    if (entryp == NULL)
        return NULL;

    return ((table_entry *)Tcl_GetHashValue(entryp))->tablep;
}

RDB_seq_item *
Duro_tobj_to_seq_items(Tcl_Interp *interp, Tcl_Obj *tobjp, int *seqitcp,
        RDB_bool needdir, RDB_bool *orderedp)
{
    int ret;
    int len, i;
    RDB_seq_item *seqitv;

    Tcl_ListObjLength(interp, tobjp, &len);
    if (len % 2 != 0) {
        Tcl_SetResult(interp, "Invalid order", TCL_STATIC);
        return NULL;
    }
    *orderedp = RDB_FALSE;
    *seqitcp = len / 2;
    if (*seqitcp > 0) {
        seqitv = (RDB_seq_item *) Tcl_Alloc(*seqitcp * sizeof(RDB_seq_item));
        for (i = 0; i < *seqitcp; i++) {
            Tcl_Obj *dirobjp, *nameobjp;
            char *dir;

            ret = Tcl_ListObjIndex(interp, tobjp, i * 2, &nameobjp);
            if (ret != TCL_OK) {
                Tcl_Free((char *) seqitv);
                return NULL;
            }
            ret = Tcl_ListObjIndex(interp, tobjp, i * 2 + 1, &dirobjp);
            if (ret != TCL_OK) {
                Tcl_Free((char *) seqitv);
                return NULL;
            }

            seqitv[i].attrname = Tcl_GetString(nameobjp);

            dir = Tcl_GetString(dirobjp);
            if (strcmp(dir, "asc") == 0) {
                seqitv[i].asc = RDB_TRUE;
                *orderedp = RDB_TRUE;
            } else if (strcmp(dir, "desc") == 0) {
                seqitv[i].asc = RDB_FALSE;
                *orderedp = RDB_TRUE;
            } else {
                if (needdir) {
                    Tcl_AppendResult(interp, "invalid direction: \"", dir,                            
                            "\", must be \"asc\" or \"desc\"", NULL);
                    return NULL;
                } else if (strcmp(dir, "-") != 0) {
                    Tcl_AppendResult(interp, "invalid direction: \"", dir,                            
                            "\", must be \"asc\", \"desc\", or \"-\"", NULL);
                    return NULL;
                }
            }
        }
    }
    return seqitv;
}

int
Duro_add_table(Tcl_Interp *interp, TclState *statep, RDB_table *tbp,
        const char *name, RDB_environment *envp)
{
    int new;
    Tcl_HashEntry *entryp;
    table_entry *tbep;

    statep->ltable_uid++;
    entryp = Tcl_CreateHashEntry(&statep->ltables, name, &new);
    if (!new) {
        Tcl_AppendResult(interp, "local table \"", name, " \"already exists");
        return TCL_ERROR;
    }

    tbep = (table_entry *) Tcl_Alloc(sizeof (table_entry));
    tbep->tablep = tbp;
    tbep->envp = envp;
    
    Tcl_SetHashValue(entryp, (ClientData)tbep);
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
        Tcl_AppendResult(interp, "invalid tuple value: ", Tcl_GetString(tobjp),
                NULL);
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
            Tcl_AppendResult(interp, "invalid attribute: ", attrname, NULL);
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
list_to_array(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_object *arrp, RDB_transaction *txp)
{
    int ret;
    int llen;
    int i;

    ret = Tcl_ListObjLength(interp, tobjp, &llen);
    if (ret != TCL_OK)
        return ret;

    ret = RDB_set_array_length(arrp, (RDB_int) llen);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_OK;
    }

    for (i = 0; i < llen; i++) {
        Tcl_Obj *valuep;
        RDB_object obj;

        /* Get attribute value */
        Tcl_ListObjIndex(interp, tobjp, i, &valuep);

        /* Convert value to RDB_object and set tuple attribute */
        RDB_init_obj(&obj);
        ret = Duro_tcl_to_duro(interp, valuep, typ->var.basetyp, &obj, txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(&obj);
            return ret;
        }
        ret = RDB_array_set(arrp, (RDB_int) i, &obj);

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
        Tcl_AppendResult(interp, "invalid type representation: ",
                Tcl_GetString(tobjp), NULL);
        return TCL_ERROR;
    }

    ret = Tcl_ListObjIndex(interp, tobjp, 0, &nametobjp);
    if (ret != TCL_OK)
        return ret;

    selname = Tcl_GetString(nametobjp);
    irep = _RDB_get_possrep(typ, selname);
    if (irep == NULL) {
        Tcl_AppendResult(interp, "invalid selector: ", selname, NULL);
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

    txp->user_data = interp;
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
    RDB_table *tbp;

    if (typ == &RDB_STRING) {
        /* Convert from UTF */
        char *s = Tcl_GetString(tobjp);
        int srclen = strlen(s);
        int dstlen = (srclen + 1) * 2;
        char *dst = Tcl_Alloc(dstlen);
        ret = Tcl_UtfToExternal(interp, NULL, s, strlen(s), 0, NULL,
                dst, dstlen, NULL, NULL, NULL);
        if (ret != TCL_OK) {
            Tcl_Free(dst);
            return ret;
        }

        RDB_string_to_obj(objp, dst);
        Tcl_Free(dst);
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
    if (typ == &RDB_BINARY) {
        int len;
        unsigned char *bp = Tcl_GetByteArrayFromObj(tobjp, &len);
        if (bp == NULL)
            return TCL_ERROR;
        ret = RDB_binary_set(objp, 0, bp, (size_t) len);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return TCL_ERROR;
        }
        return TCL_OK;
    }
    if (RDB_type_is_scalar(typ)) {
        return call_selector(interp, tobjp, typ, objp, txp);
    }
    switch (typ->kind) {
        case RDB_TP_TUPLE:
            return list_to_tuple(interp, tobjp, typ, objp, txp);
        case RDB_TP_RELATION:
            ret = list_to_table(interp, tobjp, typ, &tbp, txp);
            if (ret != TCL_OK)
                return ret;
            RDB_table_to_obj(objp, tbp);
            return TCL_OK;
        case RDB_TP_ARRAY:
            return list_to_array(interp, tobjp, typ, objp, txp);
        default: ;
    }
    Tcl_AppendResult(interp, "unsupported type: ", Tcl_GetString(tobjp),
            NULL);
    return TCL_ERROR;
}

static Tcl_Obj *
array_to_list(Tcl_Interp *interp, RDB_object *arrayp,
        RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object *objp;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);

    for (i = 0; (ret = RDB_array_get(arrayp, i, &objp)) == RDB_OK; i++) {
        Tcl_Obj *tobjp = Duro_to_tcl(interp, objp, txp);

        if (tobjp == NULL) {
            return NULL;
        }

        Tcl_ListObjAppendElement(interp, listobjp, tobjp);
    }
    return listobjp;
}

static Tcl_Obj *
table_to_list(Tcl_Interp *interp, RDB_table *tbp, RDB_transaction *txp)
{
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);

    RDB_init_obj(&arr);
    ret = RDB_table_to_array(&arr, tbp, 0, NULL, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        RDB_destroy_obj(&arr);
        return NULL;
    }

    for (i = 0;
         (ret = RDB_array_get(&arr, i, &tplp)) == RDB_OK;
         i++) {
        Tcl_ListObjAppendElement(interp, listobjp,
                Duro_to_tcl(interp, tplp, txp));
    }
    if (ret != RDB_NOT_FOUND) {
        RDB_destroy_obj(&arr);
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        Duro_dberror(interp, ret);
        return NULL;
    }

    ret = RDB_destroy_obj(&arr);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        Duro_dberror(interp, ret);
        return NULL;
    }

    return listobjp;
}

static Tcl_Obj *
tuple_to_list(Tcl_Interp *interp, const RDB_object *tplp,
        RDB_transaction *txp)
{
    int i;
    char **namev;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);
    int attrcount = RDB_tuple_size(tplp);

    if (attrcount == 0)
        return listobjp;
    
    namev = (char **) Tcl_Alloc(attrcount * sizeof(char *));

    RDB_tuple_attr_names(tplp, namev);
    for (i = 0; i < attrcount; i++) {
        RDB_object *objp = RDB_tuple_get(tplp, namev[i]);
        Tcl_Obj *tobjp = Duro_to_tcl(interp, objp, txp);

        if (tobjp == NULL) {
            Tcl_Free((char *) namev);
            return NULL;
        }

        Tcl_ListObjAppendElement(interp, listobjp,
                Tcl_NewStringObj(namev[i], strlen(namev[i])));
        Tcl_ListObjAppendElement(interp, listobjp, tobjp);
    }
    return listobjp;
}

Tcl_Obj *
Duro_irep_to_tcl(Tcl_Interp *interp, const RDB_object *objp,
        RDB_transaction *txp)
{
    int ret;
    RDB_type *typ = RDB_obj_type(objp);

    if (typ == &RDB_STRING) {
        Tcl_Obj *robjp;
        char *str = RDB_obj_string((RDB_object *)objp);
        int srclen = strlen(str);
        int dstlen = (srclen + 1) * 2;
        char *dst = Tcl_Alloc(dstlen);

        ret = Tcl_ExternalToUtf(interp, NULL, str, srclen, 0, NULL,
                dst, dstlen, NULL, NULL, NULL);
        if (ret != TCL_OK)
            return NULL;
        robjp = Tcl_NewStringObj(dst, strlen(dst));
        Tcl_Free(dst);
        return robjp;
    }
    if (typ == &RDB_INTEGER) {
        return Tcl_NewIntObj((int) RDB_obj_int(objp));
    }
    if (typ == &RDB_RATIONAL) {
        return Tcl_NewDoubleObj((double) RDB_obj_rational(objp));
    }
    if (typ == &RDB_BOOLEAN) {
        return Tcl_NewBooleanObj((int) RDB_obj_bool(objp));
    }
    if (typ == &RDB_BINARY) {
        Tcl_Obj *tobjp;
        void *datap;
        int ret;
        size_t len = RDB_binary_length(objp);

        if (len < 0) {
            Duro_dberror(interp, ret);
            return NULL;
        }

        ret = RDB_binary_get(objp, 0, &datap, len, NULL);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            return NULL;
        }

        tobjp = Tcl_NewByteArrayObj(datap, len);
        return tobjp;
    }
    if (objp->kind == RDB_OB_TUPLE || objp->kind == RDB_OB_INITIAL) {
        return tuple_to_list(interp, objp, txp);
    }
    if (objp->kind == RDB_OB_TABLE) {
        return table_to_list(interp, RDB_obj_table(objp), txp);
    }
    if (objp->kind == RDB_OB_ARRAY) {
        return array_to_list(interp, (RDB_object *) objp, txp);
    }
    Tcl_SetResult(interp, "Unsupported type", TCL_STATIC);
    return NULL;
}

static Tcl_Obj *
uobj_to_list(Tcl_Interp *interp, const RDB_object *objp, RDB_transaction *txp)
{
    int i;
    int ret;
    RDB_object comp;
    Tcl_Obj *tcomp;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);
    RDB_ipossrep *rep = &objp->typ->var.scalar.repv[0];

    /* Convert object to its first possible representation */

    Tcl_ListObjAppendElement(interp, listobjp,
            Tcl_NewStringObj(rep->name, strlen(rep->name)));

    RDB_init_obj(&comp);

    txp->user_data = interp;
    for (i = 0; i < rep->compc; i++) {
        ret = RDB_obj_comp(objp, rep->compv[i].name, &comp, txp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, ret);
            RDB_destroy_obj(&comp);
            return NULL;
        }
        tcomp = Duro_to_tcl(interp, &comp, txp);
        if (tcomp == NULL) {
            RDB_destroy_obj(&comp);
            return NULL;
        }
        Tcl_ListObjAppendElement(interp, listobjp, tcomp);
    }

    RDB_destroy_obj(&comp);
    return listobjp;
}

Tcl_Obj *
Duro_to_tcl(Tcl_Interp *interp, const RDB_object *objp,
        RDB_transaction *txp)
{
    RDB_type *typ = RDB_obj_type(objp);

    if (typ != NULL && !RDB_type_is_builtin(typ) && RDB_type_is_scalar(typ)) {
        return uobj_to_list(interp, objp, txp);
    }

    return Duro_irep_to_tcl(interp, objp, txp);
}

int
Duro_parse_expr_utf(Tcl_Interp *interp, const char *s, void *arg,
        RDB_transaction *txp, RDB_expression **expp)
{
    int ret;
    int srclen = strlen(s);
    int dstlen = (srclen + 1) * 2;
    char *dst = Tcl_Alloc(dstlen);

    ret = Tcl_UtfToExternal(interp, NULL, s, strlen(s), 0, NULL, dst, dstlen,
            NULL, NULL, NULL);
    if (ret != TCL_OK)
        return ret;

    ret = RDB_parse_expr(dst, Duro_get_ltable, arg, txp, expp);
    Tcl_Free(dst);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }
    return TCL_OK;
}
