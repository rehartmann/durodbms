/*
 * Copyright (C) 2003, 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <rel/internal.h>
#include <gen/strfns.h>
#include <string.h>

int
Duro_tcl_drop_array(RDB_object *arrayp, Tcl_HashEntry *entryp)
{
    int ret;

    Tcl_DeleteHashEntry(entryp);
    ret = RDB_destroy_obj(arrayp);
    Tcl_Free((char *) arrayp);
    return ret;
}

static int
array_create_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *tbname;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_table *tbp;
    RDB_object *arrayp;
    int new;
    char handle[20];
    RDB_seq_item *seqitv = NULL;
    int seqitc = 0;

    if (objc < 4 || objc > 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "table ?{ ?attr dir ...? }? tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    tbname = Tcl_GetStringFromObj(objv[2], NULL);
    ret = Duro_get_table(statep, interp, tbname, txp, &tbp);
    if (ret != TCL_OK) {
        return TCL_ERROR;
    }

    if (objc == 5) {
        seqitv = Duro_tobj_to_seq_items(interp, objv[3], &seqitc, RDB_TRUE);
        if (seqitv == NULL)
            return TCL_ERROR;
    }

    arrayp = (RDB_object *) Tcl_Alloc(sizeof (RDB_object));
    RDB_init_obj(arrayp);

    ret = RDB_table_to_array(arrayp, tbp, seqitc, seqitv, txp);
    if (seqitv != NULL)
        Tcl_Free((char *) seqitv);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    statep->array_uid++;
    sprintf(handle, "a%d", statep->array_uid);
    entryp = Tcl_CreateHashEntry(&statep->arrays, handle, &new);
    Tcl_SetHashValue(entryp, (ClientData)arrayp);

    Tcl_SetStringObj(Tcl_GetObjResult(interp), handle, -1);
    return TCL_OK;
}

static int
array_drop_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 2, objv, "arrayname");
        return TCL_ERROR;
    }

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);
    ret = Duro_tcl_drop_array(arrayp, entryp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }      

    return TCL_OK;
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
    RDB_qresult *qrp;
    RDB_object tpl;
    int ret;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);

    ret = _RDB_table_qresult(tbp, txp, &qrp);
    if (ret != RDB_OK) {
        if (RDB_is_syserr(ret)) {
            RDB_rollback_all(txp);
        }
        Duro_dberror(interp, ret);
        return NULL;
    }

    RDB_init_obj(&tpl);

    while ((ret = _RDB_next_tuple(qrp, &tpl, NULL)) == RDB_OK) {
        Tcl_ListObjAppendElement(interp, listobjp,
                Duro_to_tcl(interp, &tpl, txp));
    }
    RDB_destroy_obj(&tpl);
    if (ret != RDB_NOT_FOUND) {
        _RDB_drop_qresult(qrp, NULL);
        if (RDB_is_syserr(ret))
            RDB_rollback_all(txp);
        Duro_dberror(interp, ret);
        return NULL;
    }

    ret = _RDB_drop_qresult(qrp, NULL);
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
    RDB_type *typ = RDB_obj_type(objp);

    if (typ == &RDB_STRING) {
        char *str = RDB_obj_string((RDB_object *)objp);

        return Tcl_NewStringObj(str, strlen(str));
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

static int
array_index_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;
    RDB_object *tplp;
    int idx;
    Tcl_Obj *listobjp;

    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "arrayname idx tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    ret = Tcl_GetIntFromObj(interp, objv[3], &idx);
    if (ret != TCL_OK)
        return ret;

    ret = RDB_array_get(arrayp, (RDB_int) idx, &tplp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    listobjp = tuple_to_list(interp, tplp, txp);
    if (listobjp == NULL)
        return TCL_ERROR;

    Tcl_SetObjResult(interp, listobjp);
    return TCL_OK;
}

static int
array_foreach_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;
    RDB_object *tplp;
    int i;
    Tcl_Obj *elemobjp;

    if (objc != 6) {
        Tcl_WrongNumArgs(interp, 2, objv, "varname arrayname body tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[5], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    /* Get duro array */
    arraystr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    for (i = 0; (ret = RDB_array_get(arrayp, i, &tplp)) == RDB_OK; i++) {
        /* Set variable */
        elemobjp = Duro_to_tcl(interp, tplp, txp);
        if (elemobjp == NULL)
            return TCL_ERROR;
        
        Tcl_ObjSetVar2(interp, objv[2], NULL, elemobjp, 0);

        /* Invoke script */
        ret = Tcl_EvalObjEx(interp, objv[4], 0);
        if (ret != TCL_OK)
            return ret;
    }
    if (ret != RDB_NOT_FOUND) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }
        
    return TCL_OK;
}

static int
array_length_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    char *arraystr;
    RDB_object *arrayp;
    RDB_int len;
    Tcl_HashEntry *entryp;
    Tcl_Obj *robjp;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 2, objv, "array");
        return TCL_ERROR;
    }

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    len = RDB_array_length(arrayp);
    if (len < 0) {
        Duro_dberror(interp, len);
        return TCL_ERROR;
    }

    robjp = Tcl_NewIntObj((int) len);
    Tcl_SetObjResult(interp, robjp);

    return TCL_OK;
}

static int
array_set_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    char *txstr;
    RDB_transaction *txp;
    Tcl_HashEntry *entryp;
    RDB_object *arrayp;
    int idx;
    RDB_type *typ;
    RDB_object obj;

    if (objc != 7) {
        Tcl_WrongNumArgs(interp, 2, objv, "arrayname idx value type tx");
        return TCL_ERROR;
    }

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    arraystr = Tcl_GetStringFromObj(objv[2], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    ret = Tcl_GetIntFromObj(interp, objv[3], &idx);
    if (ret != TCL_OK)
        return ret;

    ret = Duro_get_type(objv[5], interp, txp, &typ);
    if (ret != TCL_OK)
        return ret;

    RDB_init_obj(&obj);
    ret = Duro_tcl_to_duro(interp, objv[4], typ, &obj, txp);
    if (ret != TCL_OK) {
        RDB_destroy_obj(&obj);
        return ret;
    }

    ret = RDB_array_set(arrayp, (RDB_int) idx, &obj);
    RDB_destroy_obj(&obj);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    return TCL_OK;
}

int
Duro_array_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "drop", "length", "index", "foreach", "set", NULL
    };
    enum array_ix {
        create_ix, drop_ix, length_ix, index_ix, foreach_ix, set_ix
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
            return array_create_cmd(statep, interp, objc, objv);
        case drop_ix:
            return array_drop_cmd(statep, interp, objc, objv);
        case length_ix:
            return array_length_cmd(statep, interp, objc, objv);
        case index_ix:
            return array_index_cmd(statep, interp, objc, objv);
        case foreach_ix:
            return array_foreach_cmd(statep, interp, objc, objv);
        case set_ix:
            return array_set_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}
