/*
 * Copyright (C) 2003 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include "duro.h"
#include <string.h>
#include <stdio.h>

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

    Tcl_AppendResult(interp, "Database error: ", (char *) RDB_strerror(err),
            TCL_STATIC);
    if (errcode != NULL)
        Tcl_SetErrorCode(interp, "Duro", errcode, (char *) RDB_strerror(err),
                NULL);
}    

int
Durotcl_Init(Tcl_Interp *interp)
{
    TclState *statep;

    if (Tcl_InitStubs(interp, "8.1", 0) == NULL) {
        return TCL_ERROR;
    }

    statep = (TclState *) Tcl_Alloc(sizeof (TclState));
    Tcl_InitHashTable(&statep->envs, TCL_STRING_KEYS);
    statep->env_uid = 0;
    Tcl_InitHashTable(&statep->txs, TCL_STRING_KEYS);
    statep->tx_uid = 0;
    Tcl_InitHashTable(&statep->arrays, TCL_STRING_KEYS);
    statep->array_uid = 0;
    Tcl_InitHashTable(&statep->ltables, TCL_STRING_KEYS);
    statep->ltable_uid = 0;

    Tcl_CreateCommand(interp, "duro::env", Duro_env_cmd, (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::begin", Duro_begin_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::commit", Duro_commit_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::rollback", Duro_rollback_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::txdb", Duro_txdb_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateCommand(interp, "duro::db", Duro_db_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::table", Duro_table_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::insert", Duro_insert_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::update", Duro_update_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::delete", Duro_delete_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::array", Duro_array_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::operator", Duro_operator_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::call", Duro_call_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::expr", Duro_expr_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::type", Duro_type_cmd,
            (ClientData)statep, NULL);
    Tcl_CreateObjCommand(interp, "duro::index", Duro_index_cmd,
            (ClientData)statep, NULL);

    Tcl_CreateExitHandler(duro_cleanup, (ClientData)statep);

    /* yydebug = 1; */
    
    return Tcl_PkgProvide(interp, "duro", "0.8");
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
