/*
 * $Id$
 *
 * Copyright (C) 2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <string.h>

static int
list_to_ins(TclState *statep, Tcl_Interp *interp, Tcl_Obj *tobjp,
        RDB_transaction *txp, RDB_ma_insert *insp)
{
    int len;
    Tcl_Obj **tobjpv;
    char *dstname;
    int ret = Tcl_ListObjGetElements(interp, tobjp, &len, &tobjpv);
    if (ret != TCL_OK)
        return ret;

    if (len != 3) {
        Tcl_SetResult(interp, "two arguments required for insert", TCL_STATIC);
        return TCL_ERROR;
    }

    dstname = Tcl_GetString(tobjpv[1]);
    if (dstname == NULL)
        return TCL_ERROR;

    /* Get destination table */
    ret = Duro_get_table(statep, interp, dstname, txp, &insp->tbp);
    if (ret != TCL_OK) {
        return ret;
    }

    insp->tplp = (RDB_object *) Tcl_Alloc(sizeof (RDB_object));
    RDB_init_obj(insp->tplp);

    ret = Duro_tcl_to_duro(interp, tobjpv[2],
            RDB_table_type(insp->tbp)->var.basetyp, insp->tplp, txp);
    if (ret != TCL_OK) {
        RDB_destroy_obj(insp->tplp);
        Tcl_Free((char *) insp->tplp);
        return ret;
    }

    return TCL_OK;
}

static int
list_to_upd(TclState *statep, Tcl_Interp *interp, Tcl_Obj *tobjp,
       RDB_transaction *txp, RDB_ma_update *updp)
{
    int len;
    int i;
    int firstai;
    Tcl_Obj **tobjpv;
    char *dstname;
    int ret = Tcl_ListObjGetElements(interp, tobjp, &len, &tobjpv);
    if (ret != TCL_OK)
        return ret;

    if (len < 4) {
        Tcl_SetResult(interp, "invalid # of arguments for update",
                TCL_STATIC);
        return TCL_ERROR;
    }

    dstname = Tcl_GetString(tobjpv[1]);
    if (dstname == NULL)
        return TCL_ERROR;

    /* Get destination table */
    ret = Duro_get_table(statep, interp, dstname, txp, &updp->tbp);
    if (ret != TCL_OK) {
        return ret;
    }

    if (len % 2 == 1) {
        /* Get condition */
        char *exptxt = Tcl_GetString(tobjpv[2]);
        if (exptxt == NULL)
            return TCL_ERROR;

        ret = Duro_parse_expr_utf(interp, exptxt, statep, txp, &updp->condp);
        if (ret != TCL_OK) {
            return ret;
        }
        firstai = 3;
    } else {
        updp->condp = NULL;
        firstai = 2;
    }

    updp->updc = (len - 2) / 2;
    updp->updv = (RDB_attr_update *)
            Tcl_Alloc(sizeof (RDB_attr_update) * updp->updc);
    for (i = 0; i < updp->updc; i++) {
        updp->updv[i].name = Tcl_GetString(tobjpv[firstai + i * 2]);
        ret = Duro_parse_expr_utf(interp,
                Tcl_GetString(tobjpv[firstai + i * 2 + 1]), statep, txp,
                &updp->updv[i].exp);
        if (ret != TCL_OK) {
            int j;

            for (j = 0; j < i - 1; j++) {
                RDB_drop_expr(updp->updv[j].exp);
            }
            if (updp->condp != NULL) {
                RDB_drop_expr(updp->condp);
            }
            return ret;
        }
    }

    return TCL_OK;
}

static int
list_to_del(TclState *statep, Tcl_Interp *interp, Tcl_Obj *tobjp,
       RDB_transaction *txp, RDB_ma_delete *delp)
{
    int len;
    Tcl_Obj **tobjpv;
    char *dstname;
    int ret = Tcl_ListObjGetElements(interp, tobjp, &len, &tobjpv);
    if (ret != TCL_OK)
        return ret;

    if (len < 2 || len > 3) {
        Tcl_SetResult(interp, "invalid # of arguments for delete",
                TCL_STATIC);
        return TCL_ERROR;
    }

    dstname = Tcl_GetString(tobjpv[1]);
    if (dstname == NULL)
        return TCL_ERROR;

    /* Get destination table */
    ret = Duro_get_table(statep, interp, dstname, txp, &delp->tbp);
    if (ret != TCL_OK) {
        return ret;
    }

    if (len == 3) {
        /* Get condition */
        char *exptxt = Tcl_GetString(tobjpv[2]);
        if (exptxt == NULL)
            return TCL_ERROR;

        ret = Duro_parse_expr_utf(interp, exptxt, statep, txp, &delp->condp);
        if (ret != TCL_OK) {
            return ret;
        }
    } else {
        delp->condp = NULL;
    }

    return TCL_OK;
}

static int
list_to_copy(TclState *statep, Tcl_Interp *interp, Tcl_Obj *tobjp,
        RDB_transaction *txp, RDB_ma_copy *copyp)
{
    int len;
    Tcl_Obj **tobjpv;
    char *dstname, *src;
    RDB_table *dsttbp, *srctbp;
    int ret = Tcl_ListObjGetElements(interp, tobjp, &len, &tobjpv);
    if (ret != TCL_OK)
        return ret;

    if (len != 3) {
        Tcl_SetResult(interp, "two arguments required for copy", TCL_STATIC);
        return TCL_ERROR;
    }

    dstname = Tcl_GetString(tobjpv[1]);
    if (dstname == NULL)
        return TCL_ERROR;

    /* Get destination table */
    ret = Duro_get_table(statep, interp, dstname, txp, &dsttbp);
    if (ret != TCL_OK) {
        return ret;
    }

    src = Tcl_GetString(tobjpv[2]);
    if (src == NULL) {
        return TCL_ERROR;
    }

    /* Parse source table expression */
    ret = Duro_parse_table_utf(interp, src, statep, txp, &srctbp);
    if (ret != TCL_OK) {
        return ret;
    }

    copyp->dstp = (RDB_object *) Tcl_Alloc(sizeof(RDB_object));
    copyp->srcp = (RDB_object *) Tcl_Alloc(sizeof(RDB_object));

    RDB_init_obj(copyp->dstp);
    RDB_init_obj(copyp->srcp);

    RDB_table_to_obj(copyp->dstp, dsttbp);
    RDB_table_to_obj(copyp->srcp, srctbp);

    return TCL_OK;
}

int
Duro_massign_cmd(ClientData data, Tcl_Interp *interp, int objc,
        Tcl_Obj *CONST objv[])
{
    int ret;
    int i, j;
    char *txstr;
    Tcl_HashEntry *entryp;
    RDB_transaction *txp;

    /* total # of assignments */
    int ac;

    RDB_ma_insert *insv;
    RDB_ma_update *updv;
    RDB_ma_delete *delv;
    RDB_ma_copy *copyv;

    int insc = 0, updc = 0, delc = 0, copyc = 0;
    TclState *statep = (TclState *) data;

    if (objc < 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "assignment ?assignment ...? txId");
        return TCL_ERROR;
    }
    ac = objc - 2;

    txstr = Tcl_GetStringFromObj(objv[objc - 1], NULL);
    entryp = Tcl_FindHashEntry(&statep->txs, txstr);
    if (entryp == NULL) {
        Tcl_AppendResult(interp, "Unknown transaction: ", txstr, NULL);
        return TCL_ERROR;
    }
    txp = Tcl_GetHashValue(entryp);

    insv = (RDB_ma_insert *) Tcl_Alloc(sizeof (RDB_ma_insert) * ac);
    updv = (RDB_ma_update *) Tcl_Alloc(sizeof (RDB_ma_update) * ac);
    delv = (RDB_ma_delete *) Tcl_Alloc(sizeof (RDB_ma_delete) * ac);
    copyv = (RDB_ma_copy *) Tcl_Alloc(sizeof (RDB_ma_copy) * ac);

    for (i = 0; i < ac; i++) {
        Tcl_Obj *opobj;
        char *op;

        ret = Tcl_ListObjIndex(interp, objv[i + 1], 0, &opobj);
        if (ret != TCL_OK)
            goto cleanup;

        op = Tcl_GetString(opobj);
        if (op == NULL) {
            ret = TCL_ERROR;
            goto cleanup;
        }
        if (strcmp(op, "insert") == 0) {
            ret = list_to_ins(statep, interp, objv[i + 1], txp, &insv[insc]);
            if (ret != TCL_OK)
                goto cleanup;
            insc++;
        } else if (strcmp(op, "update") == 0) {
            ret = list_to_upd(statep, interp, objv[i + 1], txp, &updv[updc]);
            if (ret != TCL_OK)
                goto cleanup;
            updc++;
        } else if (strcmp(op, "delete") == 0) {
            ret = list_to_del(statep, interp, objv[i + 1], txp, &delv[delc]);
            if (ret != TCL_OK)
                goto cleanup;
            delc++;
        } else if (strcmp(op, "copy") == 0) {
            ret = list_to_copy(statep, interp, objv[i + 1], txp, &copyv[copyc]);
            if (ret != TCL_OK)
                goto cleanup;
            copyc++;
        } else {
            Tcl_AppendResult(interp, "invalid assignment: ", op,
                    (char *) NULL);
            ret = TCL_ERROR;
            goto cleanup;
        }
    }

    ret = RDB_multi_assign(insc, insv, updc, updv, delc, delv, copyc, copyv,
            txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, txp, ret);
        ret = TCL_ERROR;
    }

cleanup:
    for (i = 0; i < insc; i++) {
        RDB_destroy_obj(insv[i].tplp);
        Tcl_Free((char *) insv[i].tplp);
    }
    Tcl_Free((char *) insv);

    for (i = 0; i < updc; i++) {
        if (updv[i].condp != NULL)
            RDB_drop_expr(updv[i].condp);
        for (j = 0; j < updv[i].updc; j++) {
            RDB_drop_expr(updv[i].updv[j].exp);
        }
        Tcl_Free((char *) updv[i].updv);
    }
    Tcl_Free((char *) updv);

    for (i = 0; i < delc; i++) {
        if (delv[i].condp != NULL)
            RDB_drop_expr(delv[i].condp);
    }
    Tcl_Free((char *) delv);

    for (i = 0; i < copyc; i++) {
        RDB_destroy_obj(copyv[i].dstp);
        RDB_destroy_obj(copyv[i].srcp);
        Tcl_Free((char *) copyv[i].dstp);
        Tcl_Free((char *) copyv[i].srcp);
    }
    Tcl_Free((char *) copyv);

    return ret;                    
}
