/* $Id$ */

#include "duro.h"
#include <gen/strfns.h>
#include <string.h>

int
Duro_tcl_drop_array(RDB_array *arrayp, Tcl_HashEntry *entryp)
{
    int ret;

    Tcl_DeleteHashEntry(entryp);
    ret = RDB_destroy_array(arrayp);
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
    RDB_array *arrayp;
    int new;
    char handle[20];
    RDB_seq_item *seqitv;
    int seqitc = 0;

    if (objc < 4 || objc > 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "table ?{ ?attr dir? ... }? tx");
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
        int len, i;

        Tcl_ListObjLength(interp, objv[3], &len);
        if (len % 2 != 0) {
            Tcl_SetResult(interp, "Invalid order", TCL_STATIC);
            return TCL_ERROR;
        }
        seqitc = len / 2;
        if (seqitc > 0) {
            seqitv = (RDB_seq_item *) Tcl_Alloc(seqitc * sizeof(RDB_seq_item));
            for (i = 0; i < seqitc; i++) {
                Tcl_Obj *dirobjp, *nameobjp;
                char *dir;

                ret = Tcl_ListObjIndex(interp, objv[3], i * 2, &nameobjp);
                if (ret != TCL_OK) {
                    Tcl_Free((char *) seqitv);
                    return TCL_ERROR;
                }
                ret = Tcl_ListObjIndex(interp, objv[3], i * 2 + 1, &dirobjp);
                if (ret != TCL_OK) {
                    Tcl_Free((char *) seqitv);
                    return TCL_ERROR;
                }
 
                seqitv[i].attrname = Tcl_GetStringFromObj(nameobjp, NULL);
 
                dir = Tcl_GetStringFromObj(dirobjp, NULL);
                if (strcmp(dir, "asc") == 0)
                    seqitv[i].asc = RDB_TRUE;
                else if (strcmp(dir, "desc") == 0)
                    seqitv[i].asc = RDB_FALSE;
                else {
                    Tcl_SetResult(interp,
                            "Invalid direction, must be asc or desc",
                            TCL_STATIC);
                    return TCL_ERROR;
                }
            }
        }
    }

    arrayp = (RDB_array *) Tcl_Alloc(sizeof (RDB_array));
    RDB_init_array(arrayp);

    ret = RDB_table_to_array(arrayp, tbp, seqitc, seqitv, txp);
    if (seqitc > 0)
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
    RDB_array *arrayp;

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
duro_to_tcl(Tcl_Interp *interp, const RDB_object *objp)
{
    RDB_type *typ = RDB_obj_type(objp);

    if (typ == &RDB_STRING) {
        char *str = RDB_obj_string((RDB_object *)objp);

        return Tcl_NewStringObj(str, strlen (str));
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
    Tcl_SetResult(interp, "Unsupported type", TCL_STATIC);
    return NULL;
}

Tcl_Obj *
Duro_tuple_to_list(Tcl_Interp *interp, RDB_tuple *tplp)
{
    int i;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);
    int attrcount = RDB_tuple_size(tplp);
    char **namev = (char **) Tcl_Alloc(attrcount * sizeof(char *));

    RDB_tuple_attr_names(tplp, namev);
    for (i = 0; i < attrcount; i++) {
        RDB_object *objp = RDB_tuple_get(tplp, namev[i]);
        Tcl_Obj *tobjp = duro_to_tcl(interp, objp);

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

static int
array_index_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    char *arraystr;
    Tcl_HashEntry *entryp;
    RDB_array *arrayp;
    RDB_tuple *tplp;
    int idx;
    Tcl_Obj *listobjp;

    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 2, objv, "arrayname idx");
        return TCL_ERROR;
    }

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

    ret = RDB_array_get_tuple(arrayp, (RDB_int) idx, &tplp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, ret);
        return TCL_ERROR;
    }

    listobjp = Duro_tuple_to_list(interp, tplp);
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
    Tcl_HashEntry *entryp;
    RDB_array *arrayp;
    RDB_tuple *tplp;
    int i;
    Tcl_Obj *listobjp;

    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 2, objv, "varname arrayname command");
        return TCL_ERROR;
    }

    /* Get duro array */
    arraystr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->arrays, arraystr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown array: ", arraystr, NULL);
        return TCL_ERROR;
    }
    arrayp = Tcl_GetHashValue(entryp);

    for (i = 0; (ret = RDB_array_get_tuple(arrayp, i, &tplp)) == RDB_OK; i++) {
        /* Set variable */
        listobjp = Duro_tuple_to_list(interp, tplp);
        if (listobjp == NULL)
            return TCL_ERROR;
        
        Tcl_ObjSetVar2(interp, objv[2], NULL, listobjp, 0);

        /* Invoke script */
        ret = Tcl_EvalObjEx(interp, objv[4], 0);
        if (ret != TCL_OK)
            return ret;
    }
    return TCL_OK;
}

static int
array_length_cmd(TclState *statep, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    char *arraystr;
    RDB_array *arrayp;
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

int
Duro_array_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "drop", "length", "index", "foreach", NULL
    };
    enum array_ix {
        create_ix, drop_ix, length_ix, index_ix, foreach_ix
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
    }
    return TCL_ERROR;
}