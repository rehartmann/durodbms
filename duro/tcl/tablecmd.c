/* $Id$ */

#include "duro.h"
#include <gen/strfns.h>
#include <string.h>

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
        flagstr = Tcl_GetStringFromObj(objv[2], NULL);
        if ((strcmp(flagstr, "-persistent") == 0)
                || (strcmp(flagstr, "-global") == 0)) {
        } else if ((strcmp(flagstr, "-transient") == 0)
                || (strcmp(flagstr, "-local") == 0)) {
            persistent = RDB_FALSE;
        } else {
            interp->result = "Wrong flag: must be -global, -persistent, "
                    "-local, or -transient";
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
            interp->result = "Invalid attribute definition";
            ret = TCL_ERROR;
                goto cleanup;
        }
        
        Tcl_ListObjIndex(interp, attrobjp, 0, &nameobjp);
        Tcl_ListObjIndex(interp, attrobjp, 1, &typeobjp);
        attrv[i].name = RDB_dup_str(Tcl_GetStringFromObj(nameobjp, NULL));
        ret = RDB_get_type(Tcl_GetStringFromObj(typeobjp, NULL),
                txp, &attrv[i].typ);
        if (ret != RDB_OK) {
            Tcl_AppendResult(interp, RDB_strerror(ret), NULL);
            ret = TCL_ERROR;
            goto cleanup;
        }
        attrv[i].defaultp = NULL;
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
        interp->result = (char *) RDB_strerror(ret);
        ret = TCL_ERROR;
        goto cleanup;
    }

    if (!persistent) {
        /* ... */
    }

cleanup:
    if (attrv != NULL) {
        for (i = 0; i < attrc; i++)
            free(attrv[i].name);
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

    name = Tcl_GetStringFromObj(objv[2], NULL);

    txstr = Tcl_GetStringFromObj(objv[3], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    ret = RDB_get_table(name, txp, &tbp);
    if (ret != RDB_OK) {
        interp->result = (char *) RDB_strerror(ret);
        return TCL_ERROR;
    }

    ret = RDB_drop_table(tbp, txp);
    if (ret != RDB_OK) {
        interp->result = (char *) RDB_strerror(ret);
        return TCL_ERROR;
    }

    return RDB_OK;
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

    ret = RDB_get_table(name, txp, &tbp);
    if (ret != RDB_OK) {
        interp->result = (char *) RDB_strerror(ret);
        return TCL_ERROR;
    }

    /* ... */

    return TCL_OK;
}

int
RDB_table_cmd(ClientData data, Tcl_Interp *interp, int objc, Tcl_Obj *CONST objv[])
{
    TclState *statep = (TclState *) data;

    const char *sub_cmds[] = {
        "create", "drop", "insert", NULL
    };
    enum table_ix {
        create_ix, drop_ix, insert_ix
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
        case drop_ix:
            return table_drop_cmd(statep, interp, objc, objv);
        case insert_ix:
            return table_insert_cmd(statep, interp, objc, objv);
    }
    return TCL_ERROR;
}
