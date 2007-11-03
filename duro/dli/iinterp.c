/*
 * $Id$
 *
 * Copyright (C) 2007 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Statement execution functions.
 */

#include "iinterp.h"
#include "stmtser.h"
#include <rel/rdb.h>
#include <rel/internal.h>

#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

typedef struct tx_node {
    RDB_transaction tx;
    struct tx_node *nextp;
} tx_node;

typedef int upd_op_func(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *, RDB_transaction *);

varmap_node toplevel_vars;

int err_line;

static varmap_node *current_varmapp;

static RDB_op_map opmap;

RDB_environment *envp = NULL;

static tx_node *txnp = NULL;

static int
add_varmap(RDB_exec_context *ecp)
{
    varmap_node *nodep = RDB_alloc(sizeof(varmap_node), ecp);
    if (nodep == NULL) {
        return RDB_ERROR;
    }
    RDB_init_hashmap(&nodep->map, 128);
    nodep->parentp = current_varmapp;
    current_varmapp = nodep;
    return RDB_OK;
}

static void
remove_varmap(void) {
    /* !! destroy vars */
    varmap_node *parentp = current_varmapp->parentp;
    RDB_destroy_hashmap(&current_varmapp->map);
    free(current_varmapp);
    current_varmapp = parentp;
}

static RDB_object *
lookup_local_var(const char *name, varmap_node *varmapp)
{
    RDB_object *objp;
    varmap_node *nodep = varmapp;

    do {
        objp = RDB_hashmap_get(&nodep->map, name);
        if (objp != NULL)
            return objp;
        nodep = nodep->parentp;
    } while (nodep != NULL);

    return NULL;
}

static RDB_object *
lookup_var(const char *name, RDB_exec_context *ecp)
{
    RDB_object *objp = lookup_local_var(name, current_varmapp);
    if (objp != NULL)
        return objp;
     
    if (txnp != NULL) {
        /* Try to get table from DB */
        objp = RDB_get_table(name, ecp, &txnp->tx);
    }
    if (objp == NULL)
        RDB_raise_name(name, ecp);
    return objp;
}

static RDB_object *
get_var(const char *name, void *maparg)
{
    return lookup_local_var(name, (varmap_node *) maparg);
}

void
Duro_exit_interp(void)
{
    if (txnp != NULL) {
        RDB_exec_context ec;

        RDB_init_exec_context(&ec);
        RDB_rollback(&ec, &txnp->tx);
        RDB_destroy_exec_context(&ec);

        if (_RDB_parse_interactive)
            printf("Transaction rolled back.\n");
    }
    if (envp != NULL)
        RDB_close_env(envp);
}

static int
println_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    puts(RDB_obj_string(argv[0]));
    return RDB_OK;
}   

static int
print_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    fputs(RDB_obj_string(argv[0]), stdout);
    return RDB_OK;
}

static int
readln_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    char buf[128];
    size_t len;

    if (RDB_string_to_obj(argv[0], "", ecp) != RDB_OK)
        return RDB_ERROR;

    if (fgets(buf, sizeof(buf), stdin) == NULL)
        return RDB_OK;
    len = strlen(buf);

    /* Read until a complete line has been read */
    while (buf[len - 1] != '\n') {
        if (RDB_append_string(argv[0], buf, ecp) != RDB_OK)
            return RDB_ERROR;
        if (fgets(buf, sizeof(buf), stdin) == NULL)
            return RDB_OK;
        len = strlen(buf);
    }
    buf[len - 1] = '\0';
    return RDB_append_string(argv[0], buf, ecp);
}

static int
exit_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_exit_interp();
    exit(0);
}   

static int
exit_op_int(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_exit_interp();
    exit(RDB_obj_int(argv[0]));
}   

static int
connect_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret = RDB_open_env(RDB_obj_string(argv[1]), &envp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
create_db_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (envp == NULL) {
        /* !! */
        printf("No env\n");
        return RDB_ERROR;
    }

    if (RDB_create_db_from_env(RDB_obj_string(argv[0]), envp, ecp) == NULL)
        return RDB_ERROR;
    return RDB_OK;
}   

static int
create_env_op(const char *name, int argc, RDB_object *argv[],
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    /* Create directory if does not exist */
    if (mkdir(RDB_obj_string(argv[0]),
            S_IRUSR | S_IWUSR | S_IXUSR) == -1
            && errno != EEXIST) {
        RDB_raise_system(strerror(errno), ecp);
        return RDB_ERROR;
    }

    ret = RDB_open_env(RDB_obj_string(argv[0]), &envp);
    if (ret != RDB_OK) {
        _RDB_handle_errcode(ret, ecp, txp);
        envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}   

static RDB_object *
new_obj(RDB_exec_context *ecp)
{
    RDB_object *objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return NULL;
    }
    RDB_init_obj(objp);
    return objp;
}

int
Duro_init_exec(RDB_exec_context *ecp, const char *dbname)
{
    static RDB_type *println_types[1];
    static RDB_type *print_types[1];
    static RDB_type *readln_types[1];
    static RDB_type *exit_int_types[1];
    static RDB_type *connect_types[2];
    static RDB_type *create_db_types[1];
    static RDB_type *create_env_types[1];
    RDB_object *objp;

    println_types[0] = &RDB_STRING;
    print_types[0] = &RDB_STRING;
    readln_types[0] = &RDB_STRING;
    exit_int_types[0] = &RDB_INTEGER;
    connect_types[0] = &RDB_STRING;
    connect_types[1] = &RDB_STRING;
    create_db_types[0] = &RDB_STRING;
    create_env_types[0] = &RDB_STRING;

    RDB_init_hashmap(&toplevel_vars.map, 256);
    toplevel_vars.parentp = NULL;
    current_varmapp = &toplevel_vars;

    RDB_init_op_map(&opmap);

    if (RDB_put_op(&opmap, "PRINTLN", 1, println_types, println_op, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "PRINT", 1, print_types, print_op, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "READLN", 1, readln_types, readln_op, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "EXIT", 0, NULL, exit_op, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "EXIT", 1, exit_int_types, exit_op_int, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "CONNECT", 2, connect_types, connect_op, ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "CREATE_DB", 1, create_db_types, create_db_op, ecp)
            != RDB_OK)
        return RDB_ERROR;
    if (RDB_put_op(&opmap, "CREATE_ENV", 1, create_env_types, create_env_op, ecp)
            != RDB_OK)
        return RDB_ERROR;

    objp = new_obj(ecp);
    if (objp == NULL)
        return RDB_ERROR;

    if (RDB_string_to_obj(objp, dbname, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (RDB_hashmap_put(&toplevel_vars.map, "CURRENT_DB", objp) != RDB_OK) {
        RDB_destroy_obj(objp, ecp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
init_obj(RDB_object *, RDB_type *, RDB_exec_context *, RDB_transaction *);

static int
init_obj_by_selector(RDB_object *objp, RDB_possrep *rep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    RDB_object *objv;
    RDB_object **objpv;

    for (i = 0; i < rep->compc; i++)
        RDB_init_obj(&objv[i]);

    objv = RDB_alloc(sizeof(RDB_object) * rep->compc, ecp);
    objpv = RDB_alloc(sizeof(RDB_object *) * rep->compc, ecp);
    if (objv == NULL || objpv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Get selector agruments */
    for (i = 0; i < rep->compc; i++) {
        ret = init_obj(&objv[i], rep->compv[i].typ, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;
        objpv[i] = &objv[i];
    }

    /* Call selector */
    ret = RDB_call_ro_op(rep->name, rep->compc, objpv, ecp, txp, objp);

cleanup:
    for (i = 0; i < rep->compc; i++)
        RDB_destroy_obj(&objv[i], ecp);
    free(objv);
    free(objpv);

    return ret;
}

static int
init_obj(RDB_object *objp, RDB_type *typ, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int i;

    if (typ == &RDB_BOOLEAN) {
        RDB_bool_to_obj(objp, RDB_FALSE);
    } else if (typ == &RDB_INTEGER) {
        RDB_int_to_obj(objp, 0);
    } else if (typ == &RDB_FLOAT) {
        RDB_double_to_obj(objp, 0.0);
    } else if (typ == &RDB_STRING) {
        return RDB_string_to_obj(objp, "", ecp);
    } else if (typ == &RDB_BINARY) {
        return RDB_binary_set(objp, 0, NULL, (size_t) 0, ecp);
    } else if (typ->kind == RDB_TP_TUPLE) {
        for (i = 0; i < typ->var.tuple.attrc; i++) {
            if (RDB_tuple_set(objp, typ->var.tuple.attrv[i].name,
                    NULL, ecp) != RDB_OK)
                return RDB_ERROR;
            if (init_obj(RDB_tuple_get(objp, typ->var.tuple.attrv[i].name),
                    typ->var.tuple.attrv[i].typ, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        }
    } else {
        if (typ->var.scalar.repc > 0) {
            if (txp == NULL) {
                printf("Error: no transaction\n");
                return RDB_ERROR;        
            }
            return init_obj_by_selector(objp, &typ->var.scalar.repv[0],
                    ecp, txp);
        }
    }
    return RDB_OK;
}

static RDB_type *
expr_to_type(RDB_expression *exp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    int attrc;
    RDB_attr *attrv;
    RDB_expression *argp;
    RDB_type *typ = NULL;

    if (exp->kind == RDB_EX_VAR) {
        return RDB_get_type(exp->var.varname, ecp, txp);
    }

    if (exp->kind != RDB_EX_RO_OP) {
        RDB_raise_invalid_argument("invalid type definition", ecp);
        return NULL;
    }

    attrc = RDB_expr_list_length(&exp->var.op.args);
    if (attrc % 2 != 0) {
        RDB_raise_invalid_argument("invalid type definition", ecp);
        return NULL;
    }
    attrc /= 2;

    attrv = RDB_alloc(attrc * sizeof(RDB_attr), ecp);
    if (attrv == NULL)
        return NULL;

    argp = exp->var.op.args.firstp;
    for (i = 0; i < attrc; i++) {
        if (argp->kind != RDB_EX_VAR) {
            RDB_raise_invalid_argument("invalid type definition", ecp);
            goto cleanup;
        }
        attrv[i].name = argp->var.varname;
        argp = argp->nextp;
        attrv[i].typ = expr_to_type(argp, ecp, txp);
        if (attrv[i].typ == NULL)
            goto cleanup;
        argp = argp->nextp;
    }

    if (strcmp(exp->var.op.name, "TUPLE") == 0)
        typ = RDB_create_tuple_type(attrc, attrv, ecp);
    else if (strcmp(exp->var.op.name, "RELATION") == 0)
        typ = RDB_create_relation_type(attrc, attrv, ecp);
    else
        RDB_raise_invalid_argument("invalid type definition", ecp);

cleanup:
    for (i = 0; i < attrc; i++) {
        if (attrv[i].typ != NULL && !RDB_type_is_scalar(attrv[i].typ))
            RDB_drop_type(attrv[i].typ, ecp, NULL);
    }
    free(attrv);
    return typ;
}

static int
eval_parse_type(RDB_parse_type *ptyp, RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (ptyp->typ != NULL)
        return RDB_OK;

    ptyp->typ = expr_to_type(ptyp->exp, ecp, txp);
    if (ptyp->typ == NULL)
        return RDB_ERROR;

    return RDB_OK;
}

static int
exec_vardef(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *objp;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);
    RDB_transaction *txp = txnp != NULL ? &txnp->tx : NULL;

    /*
     * Check if the variable already exists
     */
    if (RDB_hashmap_get(&current_varmapp->map, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return RDB_ERROR;
    }
    RDB_init_obj(objp);
    if (stmtp->var.vardef.exp == NULL) {
        if (eval_parse_type(&stmtp->var.vardef.type, ecp, txp) != RDB_OK) {
            RDB_destroy_obj(objp, ecp);
            free(objp);
            return RDB_ERROR;
        }
        if (init_obj(objp, stmtp->var.vardef.type.typ, ecp, txp) != RDB_OK) {
            RDB_destroy_obj(objp, ecp);
            free(objp);
            return RDB_ERROR;
        }
    } else {
        if (RDB_evaluate(stmtp->var.vardef.exp, &get_var,
                current_varmapp, ecp, NULL, objp) != RDB_OK) {
            RDB_destroy_obj(objp, ecp);
            free(objp);
            return RDB_ERROR;
        }
        if (stmtp->var.vardef.type.typ != NULL
                && !RDB_type_equals(stmtp->var.vardef.type.typ,
                                    RDB_obj_type(objp))) {
            RDB_destroy_obj(objp, ecp);
            RDB_raise_type_mismatch("", ecp);
            return RDB_ERROR;            
        }
    }
    if (RDB_hashmap_put(&current_varmapp->map, varname, objp) != RDB_OK) {
        RDB_destroy_obj(objp, ecp);
        free(objp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static RDB_string_vec *
keylist_to_keyv(RDB_parse_keydef *firstkeyp, int *keycp, RDB_exec_context *ecp)
{
    int i, j;
    RDB_string_vec *keyv;
    RDB_expression *keyattrp;
    RDB_parse_keydef *keyp = firstkeyp;
    *keycp = 0;
    while (keyp != NULL) {
        (*keycp)++;
        keyp = keyp->nextp;
    }

    keyv = RDB_alloc(sizeof(RDB_string_vec) * (*keycp), ecp);
    if (keyv == NULL) {
        return NULL;
    }
    keyp = firstkeyp;
    for (i = 0; i < *keycp; i++) {
        keyv[i].strc = RDB_expr_list_length(&keyp->attrlist);
        keyv[i].strv = RDB_alloc(keyv[i].strc * sizeof(char *), ecp);
        if (keyv[i].strv == NULL) {
            return NULL;
        }
        keyattrp = keyp->attrlist.firstp;
        for (j = 0; j < keyv[i].strc; j++) {
            keyv[i].strv[j] = RDB_obj_string(RDB_expr_obj(keyattrp));
            keyattrp = keyattrp->nextp;
        }
        keyp = keyp->nextp;
    }
    return keyv;
}

static int
exec_vardef_real(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_string_vec *keyv;
    RDB_bool freekey;
    RDB_type *tbtyp;
    int keyc = 0;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);

    if (eval_parse_type(&stmtp->var.vardef.type, ecp, NULL) != RDB_OK)
        return RDB_ERROR;
    
    tbtyp = RDB_dup_nonscalar_type(stmtp->var.vardef.type.typ, ecp);
    if (tbtyp == NULL)
        return RDB_ERROR;

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    if (stmtp->var.vardef.firstkeyp != NULL) {
        keyv = keylist_to_keyv(stmtp->var.vardef.firstkeyp, &keyc, ecp);
        if (keyv == NULL)
            return RDB_ERROR;
    } else {
        /* Get keys from expression */

        printf("Inferring keys\n");

        keyc = _RDB_infer_keys(stmtp->var.vardef.exp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;
    }

    tbp = RDB_create_table_from_type(varname, tbtyp, keyc, keyv, ecp, &txnp->tx);
    if (tbp == NULL) {
        RDB_drop_type(tbtyp, ecp, NULL);
        return RDB_ERROR;
    }

    /* !! */   

    if (_RDB_parse_interactive)
        printf("Table %s created.\n", varname);
    return RDB_OK;
}

static int
exec_vardef_virtual(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);
    RDB_expression *texp = RDB_dup_expr(stmtp->var.vardef.exp,
            ecp);
    if (texp == NULL)
        return RDB_ERROR;

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }
    tbp = RDB_expr_to_vtable(texp, ecp, &txnp->tx);
    if (tbp == NULL) {
        RDB_drop_expr(texp, ecp);
        return RDB_ERROR;
    }
    if (RDB_set_table_name(tbp, varname, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;
    if (RDB_add_table(tbp, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (_RDB_parse_interactive)
        printf("Table %s created.\n", varname);
    return RDB_OK;
}

static int
exec_vardef_private(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_string_vec *keyv;
    RDB_bool freekey;
    RDB_type *tbtyp;
    int keyc = 0;
    char *varname = RDB_obj_string(&stmtp->var.vardef.varname);

    if (eval_parse_type(&stmtp->var.vardef.type, ecp, NULL) != RDB_OK)
        return RDB_ERROR;
    
    tbtyp = RDB_dup_nonscalar_type(stmtp->var.vardef.type.typ, ecp);
    if (tbtyp == NULL)
        return RDB_ERROR;

    if (stmtp->var.vardef.firstkeyp != NULL) {
        keyv = keylist_to_keyv(stmtp->var.vardef.firstkeyp, &keyc, ecp);
        if (keyv == NULL)
            return RDB_ERROR;
    } else {
        /* Get keys from expression */

        printf("Inferring keys\n");

        keyc = _RDB_infer_keys(stmtp->var.vardef.exp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;
    }

    tbp = RDB_alloc(sizeof(RDB_object), ecp);
    if (tbp == NULL) {
        RDB_drop_type(tbtyp, ecp, NULL);
        return RDB_ERROR;
    }

    RDB_init_obj(tbp);
    if (RDB_init_table_from_type(tbp, varname, tbtyp, keyc, keyv, ecp)
            != RDB_OK) {
        RDB_destroy_obj(tbp, ecp);
        RDB_free(tbp);
        RDB_drop_type(tbtyp, ecp, NULL);
        return RDB_ERROR;
    }

    if (RDB_hashmap_put(&current_varmapp->map, varname, tbp) != RDB_OK) {
        RDB_destroy_obj(tbp, ecp);
        RDB_free(tbp);
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    /* !! */   

    if (_RDB_parse_interactive)
        printf("Local table %s created.\n", varname);
    return RDB_OK;
}

static int
exec_vardrop(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    char *varname = RDB_obj_string(&stmtp->var.vardrop.varname);
    RDB_object *objp = RDB_hashmap_get(&current_varmapp->map, varname);
    if (objp != NULL) {
        ret = RDB_hashmap_put(&current_varmapp->map, varname, NULL);
        if (ret != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
        if (RDB_destroy_obj(objp, ecp) != RDB_OK)
            return RDB_ERROR;
        free(objp);
        return RDB_OK;
    }

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    objp = RDB_get_table(varname, ecp, &txnp->tx);
    if (objp == NULL)
        return RDB_ERROR;

    if (RDB_drop_table(objp, ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    if (_RDB_parse_interactive)
        printf("Table %s dropped.\n", varname);
    return RDB_OK;
}

static int
exec_call(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int ret;
    int i;
    int argc;
    RDB_object argv[DURO_MAX_LLEN];
    RDB_object *argpv[DURO_MAX_LLEN];
    RDB_type *argtv[DURO_MAX_LLEN];
    RDB_bool argvar[DURO_MAX_LLEN];
    char *opname;
    void *op;
    upd_op_func *opfnp;
    RDB_expression *argp;

    argp = stmtp->var.call.arglist.firstp;
    i = 0;
    while (argp != NULL) {
        argvar[i] = (RDB_bool) (argp->kind == RDB_EX_VAR);
        if (argvar[i]) {
            argpv[i] = lookup_var(argp->var.varname, ecp);
            if (argpv[i] == NULL)
                return RDB_ERROR;
        } else {
            RDB_init_obj(&argv[i]);
            if (RDB_evaluate(argp, &get_var, current_varmapp,
                    ecp, txnp != NULL ? &txnp->tx : NULL, &argv[i]) != RDB_OK)
                return RDB_ERROR;
            argpv[i] = &argv[i];
        }            
        argtv[i] = RDB_obj_type(argpv[i]);
        i++;
        argp = argp->nextp;
    }
    argc = i;
    opname = RDB_obj_string(&stmtp->var.call.opname);
    op = RDB_get_op(&opmap, opname, argc, argtv);
    if (op == NULL) {
        if (txnp == NULL) {
            printf("Error: no transaction\n");
            ret = RDB_ERROR;
            goto cleanup;
        }

        ret = RDB_call_update_op(opname, argc, argpv, ecp, &txnp->tx);
    } else {
        opfnp = (upd_op_func *) op;
        ret = (*opfnp) (opname, argc, argpv, ecp,
            txnp != NULL ? &txnp->tx : NULL);
    }

cleanup:
    for (i = 0; i < argc; i++) {
        if (argvar[i])
            RDB_destroy_obj(&argv[i], ecp);
    }
    return ret;
}

static int
exec_stmtlist(RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    int ret;
    do {
        ret = Duro_exec_stmt(stmtp, ecp, retvalp);
        if (ret != RDB_OK)
            return ret;
        stmtp = stmtp->nextp;
    } while(stmtp != NULL);
    return RDB_OK;
}

static int
exec_if(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    RDB_bool b;
    int ret;

    if (RDB_evaluate_bool(stmtp->var.ifthen.condp, &get_var, current_varmapp,
            ecp, NULL, &b) != RDB_OK)
        return RDB_ERROR;
    if (add_varmap(ecp) != RDB_OK)
        return RDB_ERROR;
    if (b) {
        ret = exec_stmtlist(stmtp->var.ifthen.ifp, ecp, retvalp);
    } else if (stmtp->var.ifthen.elsep != NULL) {
        ret = exec_stmtlist(stmtp->var.ifthen.elsep, ecp, retvalp);
    } else {
        ret = RDB_OK;
    }
    remove_varmap();
    return ret;
}

static int
exec_while(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    RDB_bool b;

    for(;;) {
        if (RDB_evaluate_bool(stmtp->var.whileloop.condp, &get_var, current_varmapp,
                ecp, NULL, &b) != RDB_OK)
            return RDB_ERROR;
        if (!b)
            return RDB_OK;
        if (add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        if (exec_stmtlist(stmtp->var.whileloop.bodyp, ecp, retvalp) != RDB_OK) {
            remove_varmap();
            return RDB_ERROR;
        }
        remove_varmap();
    }
}

static int
exec_for(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    RDB_object *varp;
    RDB_object endval;

    if (stmtp->var.forloop.varexp->kind != RDB_EX_VAR) {
        RDB_raise_syntax("variable name expected", ecp);
        return RDB_ERROR;
    }
    varp = RDB_hashmap_get(&current_varmapp->map, stmtp->var.forloop.varexp->var.varname);
    if (varp == NULL) {
        printf("undefined variable %s\n",
                stmtp->var.forloop.varexp->var.varname);
        return RDB_ERROR;
    }
    if (RDB_obj_type(varp) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("loop variable must be INTEGER", ecp);
        return RDB_ERROR;
    }
    if (RDB_evaluate(stmtp->var.forloop.fromp, &get_var,
            current_varmapp, ecp, NULL, varp) != RDB_OK) {
        return RDB_ERROR;
    }
    RDB_init_obj(&endval);
    if (RDB_evaluate(stmtp->var.forloop.top, &get_var,
            current_varmapp, ecp, NULL, &endval) != RDB_OK) {
        RDB_destroy_obj(&endval, ecp);
        return RDB_ERROR;
    }
    if (RDB_obj_type(&endval) != &RDB_INTEGER) {
        RDB_destroy_obj(&endval, ecp);
        RDB_raise_type_mismatch("expression must be INTEGER", ecp);
        return RDB_ERROR;
    }
    while (varp->var.int_val <= endval.var.int_val) {
        if (add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        if (exec_stmtlist(stmtp->var.forloop.bodyp, ecp, retvalp) != RDB_OK) {
            remove_varmap();
            RDB_destroy_obj(&endval, ecp);
            return RDB_ERROR;
        }
        remove_varmap();
        varp->var.int_val++;
    }
    RDB_destroy_obj(&endval, ecp);
    return RDB_OK;
}

static RDB_attr_update *
convert_attr_assigns(RDB_parse_attr_assign *assignlp, int *updcp,
        RDB_exec_context *ecp)
{
    int i;
    RDB_attr_update *updv;
    RDB_parse_attr_assign *ap = assignlp;
    *updcp = 0;
    do {
        (*updcp)++;
        ap = ap->nextp;
    } while (ap != NULL);

    updv = RDB_alloc(*updcp * sizeof(RDB_attr_update), ecp);
    if (updv == NULL)
        return NULL;
    ap = assignlp;
    for (i = 0; i < *updcp; i++) {
        updv[i].name = ap->dstp->var.varname;
        updv[i].exp = ap->srcp;
        ap = ap->nextp;
    }
    return updv;
}

static int
exec_assign(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int cnt;
    RDB_expression *rexp;
    RDB_ma_copy copyv[DURO_MAX_LLEN];
    RDB_ma_insert insv[DURO_MAX_LLEN];
    RDB_ma_update updv[DURO_MAX_LLEN];
    RDB_ma_delete delv[DURO_MAX_LLEN];
    RDB_object srcobjv[DURO_MAX_LLEN];
    int copyc = 0;
    int insc = 0;
    int updc = 0;
    int delc = 0;

    for (i = 0; i < stmtp->var.assignment.ac; i++) {
        RDB_init_obj(&srcobjv[i]);
    }

    for (i = 0; i < stmtp->var.assignment.ac; i++) {
        switch (stmtp->var.assignment.av[i].kind) {
            case RDB_STMT_COPY:
                if (stmtp->var.assignment.av[i].var.copy.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                copyv[copyc].dstp = lookup_var(
                        stmtp->var.assignment.av[i].var.copy.dstp->var.varname,
                        ecp);
                if (copyv[copyc].dstp == NULL) {
                    return RDB_ERROR;
                }
                copyv[copyc].srcp = &srcobjv[i];
                if (RDB_evaluate(stmtp->var.assignment.av[i].var.copy.srcp, get_var,
                        current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL, &srcobjv[i])
                               != RDB_OK)
                    return RDB_ERROR;
                copyc++;
                break;
            case RDB_STMT_INSERT:
                if (stmtp->var.assignment.av[i].var.copy.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                insv[insc].tbp = lookup_var(
                        stmtp->var.assignment.av[i].var.ins.dstp->var.varname,
                        ecp);
                if (insv[insc].tbp == NULL) {
                    return RDB_ERROR;
                }
                insv[insc].objp = &srcobjv[i];
                rexp = RDB_expr_resolve_varnames(stmtp->var.assignment.av[i].var.ins.srcp,
                        &get_var, current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
                if (rexp == NULL)
                    return RDB_ERROR;
                if (RDB_evaluate(rexp, NULL, NULL, ecp,
                            txnp != NULL ? &txnp->tx : NULL, &srcobjv[i]) != RDB_OK) {
                    RDB_drop_expr(rexp, ecp);
                    return RDB_ERROR;
                }
                RDB_drop_expr(rexp, ecp);
                insc++;
                break;
            case RDB_STMT_UPDATE:
                if (stmtp->var.assignment.av[i].var.upd.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                updv[updc].tbp = lookup_var(
                        stmtp->var.assignment.av[i].var.upd.dstp->var.varname,
                        ecp);
                if (updv[updc].tbp == NULL) {
                    return RDB_ERROR;
                }
                updv[updc].updv = convert_attr_assigns(
                        stmtp->var.assignment.av[i].var.upd.assignlp,
                        &updv[updc].updc, ecp);
                if (updv[updc].updv == NULL) {
                    RDB_raise_no_memory(ecp);
                    return RDB_ERROR;
                }
                updv[updc].condp = stmtp->var.assignment.av[i].var.upd.condp;
                updc++;
                break;
            case RDB_STMT_DELETE:
                if (stmtp->var.assignment.av[i].var.del.dstp->kind
                        != RDB_EX_VAR) {
                    RDB_raise_syntax("invalid assignment target", ecp);
                    return RDB_ERROR;
                }
                delv[delc].tbp = lookup_var(
                        stmtp->var.assignment.av[i].var.del.dstp->var.varname,
                        ecp);
                if (delv[insc].tbp == NULL) {
                    return RDB_ERROR;
                }
                delv[delc].condp = stmtp->var.assignment.av[i].var.del.condp;
                delc++;
                break;                
        }
    }
    cnt = RDB_multi_assign(insc, insv, updc, updv, delc, delv, copyc, copyv,
            ecp, txnp != NULL ? &txnp->tx : NULL);
    if (cnt == (RDB_int) RDB_ERROR)
        return RDB_ERROR;

    if (_RDB_parse_interactive) {
        if (cnt == 1) {
            printf("1 element affected.\n");
        } else {
            printf("%d elements affected.\n", (int) cnt);
        }
    }

    return RDB_OK;
}

static int
exec_begin_tx(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    char *dbname;
    RDB_database *dbp;
    RDB_object *dbnameobjp = RDB_hashmap_get(&toplevel_vars.map, "CURRENT_DB");
    if (dbnameobjp == NULL) {
        printf("no database\n");
        return RDB_ERROR;
    }
    dbname = RDB_obj_string(dbnameobjp);
    if (*dbname == '\0') {
        printf("no database\n");
        return RDB_ERROR;
    }
    if (envp == NULL) {
        printf("no connection\n");
        return RDB_ERROR;
    }
    if (txnp != NULL) {
        /* !! */
        printf("Hier w�re eine nested Tx f�llig!\n");
        return RDB_ERROR;
    }

    dbp = RDB_get_db_from_env(dbname, envp, ecp);
    if (dbp == NULL)
        return RDB_ERROR;

    txnp = RDB_alloc(sizeof(tx_node), ecp);
    if (txnp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &txnp->tx, dbp, NULL) != RDB_OK) {
        free(txnp);
        txnp = NULL;
        return RDB_ERROR;
    }
    txnp->nextp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction started.\n");
    return RDB_OK;
}

static int
exec_commit(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    if (RDB_commit(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    free(txnp);
    txnp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction committed.\n");
    return RDB_OK;
}

static int
exec_rollback(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    if (RDB_rollback(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    free(txnp);
    txnp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction rolled back.\n");
    return RDB_OK;
}

static int
parserep_to_rep(const RDB_parse_possrep *prep, RDB_possrep *rep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int i;
    RDB_expression *exp;
    
    rep->name = prep->namexp != NULL ? RDB_obj_string(&prep->namexp->var.obj) : NULL;
    rep->compc = RDB_expr_list_length(&prep->attrlist) / 2;

    rep->compv = RDB_alloc(rep->compc * sizeof(RDB_attr), ecp);
    if (rep->compv == NULL)
        return RDB_ERROR;
    i = 0;
    exp = prep->attrlist.firstp;
    while (exp != NULL) {
        rep->compv[i].name = exp->var.varname;
        exp = exp->nextp;
        rep->compv[i].typ = expr_to_type(exp, ecp, txp);
        if (rep->compv[i].typ == NULL)
            return RDB_ERROR;
        exp = exp->nextp;
        i++;
    }
    return RDB_OK;
}

static int
exec_deftype(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i, j;
    int repc;
    RDB_possrep *repv;
    RDB_parse_possrep *prep;

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    repc = 0;
    prep = stmtp->var.deftype.replistp;
    while (prep != NULL) {
        repc++;
        prep = prep->nextp;
    }

    repv = RDB_alloc(repc * sizeof(RDB_possrep), ecp);
    if (repv == NULL)
        return RDB_ERROR;

    i = 0;
    prep = stmtp->var.deftype.replistp;
    while (prep != NULL) {
        if (parserep_to_rep(prep, &repv[i], ecp, &txnp->tx) != RDB_OK)
            goto error;
        prep = prep->nextp;
        i++;
    }
    
    if (RDB_define_type(RDB_obj_string(&stmtp->var.deftype.typename),
            repc, repv, stmtp->var.deftype.constraintp, ecp, &txnp->tx) != RDB_OK)
        goto error;

    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_drop_type(repv[i].compv[j].typ, ecp, NULL);
        }
    }
    RDB_free(repv);
    return RDB_OK;

error:
    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_drop_type(repv[i].compv[j].typ, ecp, NULL);
        }
    }
    RDB_free(repv);
    return RDB_ERROR;
}

static int
exec_typedrop(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    RDB_type *typ;
    
    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }
    
    typ = RDB_get_type(RDB_obj_string(&stmtp->var.vardrop.varname), ecp, &txnp->tx);
    if (typ == NULL)
        return RDB_ERROR;

    return RDB_drop_type(typ, ecp, &txnp->tx);
}

static int
exec_opdrop(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    return RDB_drop_op(RDB_obj_string(&stmtp->var.opdrop.opname), ecp, &txnp->tx);
}

int
Duro_invoke_dt_ro_op(const char *name, int argc, RDB_object *argv[],
          const void *iargp, size_t iarglen,
          RDB_exec_context *ecp, RDB_transaction *txp,
          RDB_object *retvalp)
{
    int i;
    int ret;
    varmap_node vars;
    RDB_parse_statement *stmtlistp;
    char **argnamev;
    varmap_node *ovarmapp = current_varmapp;

    argnamev = RDB_alloc(argc * sizeof(char *), ecp);
    if (argnamev == NULL)
        return RDB_ERROR;

    stmtlistp = Duro_bin_to_stmts(iargp, iarglen, argc, ecp, txp, argnamev);
    if (stmtlistp == NULL) {
        free(argnamev);
        return RDB_ERROR;
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    current_varmapp = &vars;

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, argnamev[i], argv[i]) != RDB_OK) {
            free(argnamev);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }
    free(argnamev);

    ret = exec_stmtlist(stmtlistp, ecp, retvalp);
    current_varmapp = ovarmapp;
    /* !! destroy vars */
    RDB_destroy_hashmap(&vars.map);
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

int
Duro_invoke_dt_update_op(const char *name, int argc, RDB_object *argv[],
        RDB_bool updv[], const void *iargp, size_t iarglen,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    varmap_node vars;
    RDB_parse_statement *stmtlistp;
    char **argnamev;
    varmap_node *ovarmapp = current_varmapp;

    argnamev = RDB_alloc(argc * sizeof(char *), ecp);
    if (argnamev == NULL)
        return RDB_ERROR;

    stmtlistp = Duro_bin_to_stmts(iargp, iarglen, argc, ecp, txp, argnamev);
    if (stmtlistp == NULL) {
        free(argnamev);
        return RDB_ERROR;
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    current_varmapp = &vars;

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, argnamev[i], argv[i]) != RDB_OK) {
            free(argnamev);
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }
    free(argnamev);

    ret = exec_stmtlist(stmtlistp, ecp, NULL);   
    current_varmapp = ovarmapp;
    /* !! destroy vars */
    RDB_destroy_hashmap(&vars.map);
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

static int
exec_ro_op_def(RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_type **argtv;
    RDB_object code;
    size_t sercodelen;
    void *sercodep;

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    /* Serialize the defining code */
    RDB_init_obj(&code);
    if (Duro_op_to_binobj(&code, stmtp, ecp) != RDB_OK) {
        RDB_destroy_obj(&code, ecp);
        return RDB_ERROR;
    }

    argtv = RDB_alloc(stmtp->var.opdef.argc * sizeof(RDB_type *),
            ecp);
    if (argtv == NULL) {
        RDB_destroy_obj(&code, ecp);
        return RDB_ERROR;
    }

    for (i = 0; i < stmtp->var.opdef.argc; i++) {
        if (eval_parse_type(&stmtp->var.opdef.argv[i].type, ecp, &txnp->tx) != RDB_OK) {
            free(argtv);
            RDB_destroy_obj(&code, ecp);
            return RDB_ERROR;
        }
        argtv[i] = stmtp->var.opdef.argv[i].type.typ;
    }

    sercodelen = RDB_binary_length(&code);
    RDB_binary_get (&code, 0, sercodelen, ecp, &sercodep, NULL);

    if (_RDB_parse_interactive)
        printf("Creating operator %s\n", RDB_obj_string(&stmtp->var.opdef.opname));
    if (eval_parse_type(&stmtp->var.opdef.rtype, ecp, &txnp->tx) != RDB_OK) {
        free(argtv);
        RDB_destroy_obj(&code, ecp);
        return RDB_ERROR;
    }

    ret = RDB_create_ro_op(RDB_obj_string(&stmtp->var.opdef.opname),
            stmtp->var.opdef.argc, argtv, stmtp->var.opdef.rtype.typ,
            "libduro", "Duro_invoke_dt_ro_op",
            sercodep, sercodelen, ecp, &txnp->tx);
    free(argtv);
    RDB_destroy_obj(&code, ecp);
    return ret;
}

static int
exec_update_op_def(const RDB_parse_statement *stmtp, RDB_exec_context *ecp)
{
    int i;
    int ret;
    RDB_type **argtv;
    RDB_bool *updv;
    RDB_object code;
    size_t sercodelen;
    void *sercodep;

    if (txnp == NULL) {
        printf("Error: no transaction\n");
        return RDB_ERROR;
    }

    /* Serialize the defining code */
    RDB_init_obj(&code);
    if (Duro_op_to_binobj(&code, stmtp, ecp) != RDB_OK) {
        RDB_destroy_obj(&code, ecp);
        return RDB_ERROR;
    }

    argtv = RDB_alloc(stmtp->var.opdef.argc * sizeof(RDB_type *),
            ecp);
    if (argtv == NULL)
        return RDB_ERROR;
    updv = RDB_alloc(stmtp->var.opdef.argc * sizeof(RDB_bool), ecp);
    if (updv == NULL)
        return RDB_ERROR;
    for (i = 0; i < stmtp->var.opdef.argc; i++) {
        if (eval_parse_type(&stmtp->var.opdef.argv[i].type, ecp, &txnp->tx) != RDB_OK) {
            return RDB_ERROR;
        }
        argtv[i] = stmtp->var.opdef.argv[i].type.typ;
        updv[i] = stmtp->var.opdef.argv[i].upd;
    }

    sercodelen = RDB_binary_length(&code);
    RDB_binary_get (&code, 0, sercodelen, ecp, &sercodep, NULL);

    ret = RDB_create_update_op(RDB_obj_string(&stmtp->var.opdef.opname),
            stmtp->var.opdef.argc, argtv, updv,
            "libduro", "Duro_invoke_dt_update_op",
            sercodep, sercodelen, ecp, &txnp->tx);
    free(argtv);
    free(updv);
    return ret;
}

static int
exec_return(const RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    if (stmtp->var.retexp != NULL) {
        if (retvalp == NULL) {
            RDB_raise_invalid_argument("return argument not allowed", ecp);
            return RDB_ERROR;
        }
        if (RDB_evaluate(stmtp->var.retexp, &get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, retvalp) != RDB_OK)
            return RDB_ERROR;
    }
    return DURO_RETURN;
}

int
Duro_exec_stmt(RDB_parse_statement *stmtp, RDB_exec_context *ecp,
        RDB_object *retvalp)
{
    int ret;
    
    switch (stmtp->kind) {
        case RDB_STMT_CALL:
            ret = exec_call(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF:
            ret = exec_vardef(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF_REAL:
            ret = exec_vardef_real(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF_VIRTUAL:
            ret = exec_vardef_virtual(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DEF_PRIVATE:
            ret = exec_vardef_private(stmtp, ecp);
            break;
        case RDB_STMT_VAR_DROP:
            ret = exec_vardrop(stmtp, ecp);
            break;
        case RDB_STMT_NOOP:
            ret = RDB_OK;
            break;
        case RDB_STMT_IF:
            ret = exec_if(stmtp, ecp, retvalp);
            break;
        case RDB_STMT_FOR:
            ret = exec_for(stmtp, ecp, retvalp);
            break;
        case RDB_STMT_WHILE:
            ret = exec_while(stmtp, ecp, retvalp);
            break;
        case RDB_STMT_ASSIGN:
            ret = exec_assign(stmtp, ecp);
            break;
        case RDB_STMT_BEGIN_TX:
            ret = exec_begin_tx(stmtp, ecp);
            break;
        case RDB_STMT_COMMIT:
            ret = exec_commit(stmtp, ecp);
            break;
        case RDB_STMT_ROLLBACK:
            ret = exec_rollback(stmtp, ecp);
            break;
        case RDB_STMT_TYPE_DEF:
            ret = exec_deftype(stmtp, ecp);
            break;
        case RDB_STMT_TYPE_DROP:
            ret = exec_typedrop(stmtp, ecp);
            break;
        case RDB_STMT_RO_OP_DEF:
            ret = exec_ro_op_def(stmtp, ecp);
            break;
        case RDB_STMT_UPD_OP_DEF:
            ret = exec_update_op_def(stmtp, ecp);
            break;
        case RDB_STMT_OP_DROP:
            ret = exec_opdrop(stmtp, ecp);
            break;
        case RDB_STMT_RETURN:
            ret = exec_return(stmtp, ecp, retvalp);
            break;
        default:
            abort();
    }
    if (ret != RDB_OK) {
        if (err_line < 0) {
            err_line = stmtp->lineno;
        }
    }
    return ret;
}

int
Duro_process_stmt(RDB_exec_context *ecp)
{
    RDB_parse_statement *stmtp;
    RDB_object prompt;
    RDB_object *dbnameobjp = RDB_hashmap_get(&toplevel_vars.map, "CURRENT_DB");

    RDB_init_obj(&prompt);
    if (dbnameobjp != NULL && *RDB_obj_string(dbnameobjp) != '\0') {
        RDB_string_to_obj(&prompt, RDB_obj_string(dbnameobjp), ecp);
    } else {
        RDB_string_to_obj(&prompt, "no db", ecp);
    }
    RDB_append_string(&prompt, "> ", ecp);

    _RDB_parse_prompt = RDB_obj_string(&prompt);

    stmtp = RDB_parse_stmt(ecp);
    if (stmtp == NULL) {
        return RDB_ERROR;
    }
    assert(RDB_get_err(ecp) == NULL);
    if (Duro_exec_stmt(stmtp, ecp, NULL) != RDB_OK) {
        RDB_parse_del_stmt(stmtp, ecp);
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("statement execution failed, no error available", ecp);
        }
        return RDB_ERROR;
    }
    return RDB_parse_del_stmt(stmtp, ecp);
}
