/*
 * $Id$
 *
 * Copyright (C) 2003-2005 René Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "duro.h"
#include <rel/rdb.h>
#include <rel/internal.h>
#include <dli/parse.h>
#include <dli/tabletostr.h>
#include <string.h>
#include <stdio.h>
#include <locale.h>
#include <ctype.h>

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
        Duro_tcl_drop_array(arrayp, entryp, statep->current_ecp);
        entryp = Tcl_FirstHashEntry(&statep->arrays, &search);
    }

    /* Abort pending transactions */
    entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    while (entryp != NULL) {
        txp = Tcl_GetHashValue(entryp);
        Duro_tcl_rollback(entryp, statep->current_ecp, txp);
        entryp = Tcl_FirstHashEntry(&statep->txs, &search);
    }

    /* Close open DB environments */
    entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    while (entryp != NULL) {
        envp = Tcl_GetHashValue(entryp);
        Duro_tcl_close_env(statep, envp, entryp);
        entryp = Tcl_FirstHashEntry(&statep->envs, &search);
    }

    RDB_destroy_exec_context(&statep->ec);

    Tcl_Free((char *) statep);
}

void
Duro_dberror(Tcl_Interp *interp, const RDB_object *errp, RDB_transaction *txp)
{
    char *typename = RDB_type_name(RDB_obj_type(errp));

    Tcl_AppendResult(interp, "database error: ", typename, (char *) NULL);

    if (txp != NULL) {
        RDB_object info;
        RDB_exec_context ec;
        Tcl_Obj *tobjv[2];

        RDB_init_exec_context(&ec);
        RDB_init_obj(&info);
        
        /*
         * Try to get error info
         */

        if (RDB_obj_comp(errp, "MSG", &info, &ec, txp) == RDB_OK) {
            if (info.typ == &RDB_STRING && RDB_obj_string(&info)[0] != '\0') {
                Tcl_AppendResult(interp, ": ", RDB_obj_string(&info),
                        (char *) NULL);
            }            
        }

        /*
         * Convert error to Tutorial D rep
         */

        tobjv[0] = Tcl_NewStringObj("DURO", -1);
        tobjv[1] = Duro_to_tcl(interp, errp, &ec, txp);
        if (tobjv[0] != NULL && tobjv[1] != NULL) {
            Tcl_Obj *terrcodep = Tcl_NewListObj(2, tobjv);
            Tcl_SetObjErrorCode(interp, terrcodep);
        }

        RDB_destroy_obj(&info, NULL);
        RDB_destroy_exec_context(&ec);
    } else {
        /*
         * If no tx is available, use type name only
         */
        Tcl_SetErrorCode(interp, "DURO", typename, (char *) NULL);
    }
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
    Tcl_CreateObjCommand(interp, "duro::massign", Duro_massign_cmd,
            (ClientData)*statepp, NULL);

    Tcl_CreateExitHandler(duro_cleanup, (ClientData)*statepp);

    RDB_init_exec_context(&(*statepp)->ec);
    (*statepp)->current_ecp = &(*statepp)->ec;
    if (RDB_ec_set_property(&(*statepp)->ec, "TCL_INTERP", interp)
            != RDB_OK) {
        RDB_destroy_exec_context(&(*statepp)->ec);
        return TCL_ERROR;
    }

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
        Tcl_AppendResult(interp, "local table \"", name, "\" already exists",
                (char *) NULL);
        Tcl_SetErrorCode(interp, "Duro",
                "RDB_ELEMENT_EXISTS_ERROR(\"local table exists\")", (char *) NULL);
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
        RDB_object *tplp, RDB_exec_context *ecp, RDB_transaction *txp)
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
        ret = Duro_tcl_to_duro(interp, valuep, attrtyp, &obj, ecp, txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(&obj, ecp);
            return ret;
        }
        ret = RDB_tuple_set(tplp, attrname, &obj, ecp);

        RDB_destroy_obj(&obj, ecp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return TCL_ERROR;
        }
    }

    return TCL_OK;
}

static int
list_to_array(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_object *arrp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int llen;
    int i;

    ret = Tcl_ListObjLength(interp, tobjp, &llen);
    if (ret != TCL_OK)
        return ret;

    ret = RDB_set_array_length(arrp, (RDB_int) llen, ecp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, RDB_get_err(ecp), txp);
        return TCL_OK;
    }

    for (i = 0; i < llen; i++) {
        Tcl_Obj *valuep;
        RDB_object obj;

        /* Get attribute value */
        Tcl_ListObjIndex(interp, tobjp, i, &valuep);

        /* Convert value to RDB_object and set tuple attribute */
        RDB_init_obj(&obj);
        ret = Duro_tcl_to_duro(interp, valuep, typ->var.basetyp, &obj, ecp, txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(&obj, ecp);
            return ret;
        }
        ret = RDB_array_set(arrp, (RDB_int) i, &obj, ecp);

        RDB_destroy_obj(&obj, ecp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return TCL_ERROR;
        }
    }

    return TCL_OK;
}

static int
list_to_table(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_table **tbpp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    int llen;
    Tcl_Obj *tplobjp;
    RDB_object tpl;

    *tbpp = RDB_create_table(NULL, RDB_FALSE,
                        typ->var.basetyp->var.tuple.attrc,
                        typ->var.basetyp->var.tuple.attrv,
                        0, NULL, ecp, NULL);
    if (*tbpp == NULL) {
        Duro_dberror(interp, RDB_get_err(ecp), txp);
        return TCL_ERROR;
    }

    ret = Tcl_ListObjLength(interp, tobjp, &llen);
    if (ret != TCL_OK)
        return ret;

    RDB_init_obj(&tpl);
    for (i = 0; i < llen; i++) {
        Tcl_ListObjIndex(interp, tobjp, i, &tplobjp);

        ret = list_to_tuple(interp, tplobjp, typ->var.basetyp, &tpl, ecp, txp);
        if (ret != TCL_OK) {
            RDB_destroy_obj(&tpl, ecp);
            return ret;
        }

        ret = RDB_insert(*tbpp, &tpl, ecp, NULL);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl, ecp);
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return TCL_ERROR;
        }
    }
    RDB_destroy_obj(&tpl, ecp);

    return TCL_OK;
}

int
Duro_tcl_to_duro(Tcl_Interp *interp, Tcl_Obj *tobjp, RDB_type *typ,
        RDB_object *objp, RDB_exec_context *ecp, RDB_transaction *txp)
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

        RDB_string_to_obj(objp, dst, ecp);
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
    if (typ == &RDB_DOUBLE) {
        double val;

        ret = Tcl_GetDoubleFromObj(interp, tobjp, &val);
        if (ret != TCL_OK)
            return ret;
        RDB_double_to_obj(objp, (RDB_double) val);
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
        ret = RDB_binary_set(objp, 0, bp, (size_t) len, ecp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return TCL_ERROR;
        }
        return TCL_OK;
    }
    if (RDB_type_is_scalar(typ)) {
        RDB_expression *exp;

        exp = RDB_parse_expr(Tcl_GetString(tobjp), NULL, NULL, ecp, txp);
        if (exp == NULL) {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return TCL_ERROR;
        }
        ret = RDB_evaluate(exp, NULL, ecp, txp, objp);
        RDB_drop_expr(exp, ecp);
        if (ret != RDB_OK) {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return TCL_ERROR;
        }
        return TCL_OK;
    }
    switch (typ->kind) {
        case RDB_TP_TUPLE:
            return list_to_tuple(interp, tobjp, typ, objp, ecp, txp);
        case RDB_TP_RELATION:
            ret = list_to_table(interp, tobjp, typ, &tbp, ecp, txp);
            if (ret != TCL_OK)
                return ret;
            RDB_table_to_obj(objp, tbp, ecp);
            return TCL_OK;
        case RDB_TP_ARRAY:
            return list_to_array(interp, tobjp, typ, objp, ecp, txp);
        default: ;
    }
    Tcl_AppendResult(interp, "unsupported type: ", Tcl_GetString(tobjp),
            NULL);
    return TCL_ERROR;
}

static Tcl_Obj *
array_to_list(Tcl_Interp *interp, RDB_object *arrayp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_object *objp;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);

    for (i = 0; (objp = RDB_array_get(arrayp, i, ecp)) != NULL; i++) {
        Tcl_Obj *tobjp = Duro_to_tcl(interp, objp, ecp, txp);

        if (tobjp == NULL) {
            return NULL;
        }

        Tcl_ListObjAppendElement(interp, listobjp, tobjp);
    }
    return listobjp;
}

static Tcl_Obj *
table_to_list(Tcl_Interp *interp, RDB_table *tbp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    RDB_object arr;
    RDB_object *tplp;
    int ret;
    int i;
    Tcl_Obj *listobjp = Tcl_NewListObj(0, NULL);

    RDB_init_obj(&arr);
    ret = RDB_table_to_array(&arr, tbp, 0, NULL, ecp, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, RDB_get_err(ecp), txp);
        RDB_destroy_obj(&arr, ecp);
        return NULL;
    }

    for (i = 0;
         (tplp = RDB_array_get(&arr, i, ecp)) != NULL;
         i++) {
        Tcl_ListObjAppendElement(interp, listobjp,
                Duro_to_tcl(interp, tplp, ecp, txp));
    }
    if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR) {
        RDB_destroy_obj(&arr, ecp);
        Duro_dberror(interp, RDB_get_err(ecp), txp);
        return NULL;
    }
    RDB_clear_err(ecp);

    ret = RDB_destroy_obj(&arr, ecp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, RDB_get_err(ecp), txp);
        return NULL;
    }

    return listobjp;
}

RDB_table *
Duro_get_table(TclState *statep, Tcl_Interp *interp, const char *name,
          RDB_transaction *txp)
{
    RDB_table *tbp;
    Tcl_HashEntry *entryp;

    /*
     * Search for transient table first
     */
    entryp = Tcl_FindHashEntry(&statep->ltables, name);
    if (entryp != NULL) {
        /* Found */
        return ((table_entry *)Tcl_GetHashValue(entryp))->tablep;
    }

    /*
     * Search for persistent table
     */
    tbp = RDB_get_table(name, statep->current_ecp, txp);
    if (tbp == NULL) {
        if (RDB_obj_type(RDB_get_err(statep->current_ecp))
                == &RDB_NOT_FOUND_ERROR) {
            Tcl_AppendResult(interp, "Unknown table: ", name, NULL);
        } else {
            Duro_dberror(interp, RDB_get_err(statep->current_ecp), txp);
        }
        return NULL;
    }
    return tbp;
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

RDB_type *
Duro_get_type(Tcl_Obj *objp, Tcl_Interp *interp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_type *typ;

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
            Tcl_AppendResult(interp, "invalid type: ", Tcl_GetString(objp),
                    NULL);
            return NULL;
        }

        ret = Tcl_ListObjIndex(interp, objp, 0, &elemp);
        if (ret != TCL_OK)
            return NULL;

        if (strcmp(Tcl_GetString(elemp), "tuple") == 0) {
            istuple = RDB_TRUE;
        } else if (strcmp(Tcl_GetString(elemp), "relation") == 0) {
            istuple = RDB_FALSE;
        } else {
            Tcl_AppendResult(interp, "invalid type: ", Tcl_GetString(objp), NULL);
            return NULL;
        }

        attrc = llen - 1;
        attrv = malloc(attrc * sizeof (RDB_attr));
        for (i = 0; i < attrc; i++) {
            Tcl_Obj *elem2p;

            Tcl_ListObjIndex(interp, objp, i + 1, &elemp);

            /* Get attribute name and type */
            Tcl_ListObjLength(interp, elemp, &llen);
            if (llen != 2) {
                Tcl_AppendResult(interp, "invalid attribute definition: ",
                        Tcl_GetString(elemp), NULL);
                free(attrv);
                return NULL;
            }

            Tcl_ListObjIndex(interp, elemp, 0, &elem2p);
            attrv[i].name = Tcl_GetString(elem2p);

            Tcl_ListObjIndex(interp, elemp, 1, &elem2p);
            attrv[i].typ = Duro_get_type(elem2p, interp, ecp, txp);
            if (attrv[i].typ == NULL) {
                free(attrv);
                return NULL;
            }
            
            attrv[i].defaultp = NULL;
        }
        if (istuple) {
            typ = RDB_create_tuple_type(attrc, attrv, ecp);
        } else {
            typ = RDB_create_relation_type(attrc, attrv, ecp);
        }
        free(attrv);
        if (typ == NULL) {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return NULL;
        }
        return typ;
    }

    typ = RDB_get_type(Tcl_GetString(objp), ecp, txp);
    if (typ == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            Tcl_AppendResult(interp, "invalid type \"", Tcl_GetString(objp),
                             "\"", NULL);
        } else {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
        }
        return NULL;
    }
    return typ;
}

static Tcl_Obj *
tuple_to_list(Tcl_Interp *interp, const RDB_object *tplp,
        RDB_exec_context *ecp, RDB_transaction *txp)
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
        Tcl_Obj *tobjp = Duro_to_tcl(interp, objp, ecp, txp);

        if (tobjp == NULL) {
            Tcl_Free((char *) namev);
            return NULL;
        }

        Tcl_ListObjAppendElement(interp, listobjp,
                Tcl_NewStringObj(namev[i], -1));
        Tcl_ListObjAppendElement(interp, listobjp, tobjp);
    }
    return listobjp;
}

Tcl_Obj *
Duro_irep_to_tcl(Tcl_Interp *interp, const RDB_object *objp,
        RDB_exec_context *ecp, RDB_transaction *txp)
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
        robjp = Tcl_NewStringObj(dst, -1);
        Tcl_Free(dst);
        return robjp;
    }
    if (typ == &RDB_INTEGER) {
        return Tcl_NewIntObj((int) RDB_obj_int(objp));
    }
    if (typ == &RDB_DOUBLE) {
        return Tcl_NewDoubleObj((double) RDB_obj_double(objp));
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
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return NULL;
        }

        ret = RDB_binary_get(objp, 0, len, ecp, &datap, NULL);
        if (ret != RDB_OK) {
            Duro_dberror(interp, RDB_get_err(ecp), txp);
            return NULL;
        }

        tobjp = Tcl_NewByteArrayObj(datap, len);
        return tobjp;
    }
    if (objp->kind == RDB_OB_TUPLE || objp->kind == RDB_OB_INITIAL) {
        return tuple_to_list(interp, objp, ecp, txp);
    }
    if (objp->kind == RDB_OB_TABLE) {
        return table_to_list(interp, RDB_obj_table(objp), ecp, txp);
    }
    if (objp->kind == RDB_OB_ARRAY) {
        return array_to_list(interp, (RDB_object *) objp, ecp, txp);
    }
    Tcl_SetResult(interp, "Unsupported type", TCL_STATIC);
    return NULL;
}

static Tcl_Obj *
uobj_to_tobj(Tcl_Interp *interp, const RDB_object *objp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    RDB_object strobj;

    RDB_init_obj(&strobj);
    ret = _RDB_obj_to_str(&strobj, objp, ecp, txp);
    if (ret != RDB_OK) {
        Duro_dberror(interp, RDB_get_err(ecp), txp);
        RDB_destroy_obj(&strobj, ecp);
        return NULL;
    }
    return Tcl_NewStringObj(RDB_obj_string(&strobj), strobj.var.bin.len - 1);
}

Tcl_Obj *
Duro_to_tcl(Tcl_Interp *interp, const RDB_object *objp,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_type *typ = RDB_obj_type(objp);

    if (typ != NULL && !RDB_type_is_builtin(typ) && RDB_type_is_scalar(typ)) {
        return uobj_to_tobj(interp, objp, ecp, txp);
    }

    return Duro_irep_to_tcl(interp, objp, ecp, txp);
}

RDB_expression *
Duro_parse_expr_utf(Tcl_Interp *interp, const char *s, void *arg,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    RDB_expression *exp;
    int srclen = strlen(s);
    int dstlen = (srclen + 1) * 2;
    char *dst = Tcl_Alloc(dstlen);

    ret = Tcl_UtfToExternal(interp, NULL, s, strlen(s), 0, NULL, dst, dstlen,
            NULL, NULL, NULL);
    if (ret != TCL_OK)
        return NULL;

    exp = RDB_parse_expr(dst, Duro_get_ltable, arg, ecp, txp);
    Tcl_Free(dst);
    if (exp == NULL) {
        Duro_dberror(interp, RDB_get_err(ecp), txp);
        return NULL;
    }
    return exp;
}
