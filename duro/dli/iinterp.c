/*
 * $Id$
 *
 * Copyright (C) 2007-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Statement execution functions.
 */

#include "iinterp.h"
#include "exparse.h"
#include "ioop.h"
#include <gen/hashmap.h>
#include <gen/hashmapit.h>
#include <rel/tostr.h>
#include <rel/rdb.h>
#include <rel/internal.h>
#include <rel/typeimpl.h>

#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

enum {
    DURO_MAX_LLEN = 64,
    DEFAULT_VARMAP_SIZE = 128
};

typedef struct tx_node {
    RDB_transaction tx;
    struct tx_node *parentp;
} tx_node;

typedef struct return_info {
    RDB_type *typ;  /* Return type */
    RDB_object *objp;   /* Return value */
} return_info;

/* Contains operators and global transient variables */
typedef struct module {
    /* Global transient variables */
    RDB_hashmap varmap;

    /* System-defined update operators (not stored), only used in sys_module */
    RDB_op_map upd_op_map;
} module;

typedef struct varmap_node {
    RDB_hashmap map;
    struct varmap_node *parentp;
} varmap_node;

/* Top-level module */
static module root_module;

/* System module, contains STDIN, STDOUT etc. */
static module sys_module;

extern int yylineno;

int err_line;

typedef struct yy_buffer_state *YY_BUFFER_STATE;

static sig_atomic_t interrupted;

/*
 * Points to the local variables in the current scope.
 * Linked list from inner to outer scope.
 */
static varmap_node *current_varmapp;

static RDB_environment *envp = NULL;

static tx_node *txnp = NULL;

static char *leave_targetname;

static int
add_varmap(RDB_exec_context *ecp)
{
    varmap_node *nodep = RDB_alloc(sizeof(varmap_node), ecp);
    if (nodep == NULL) {
        return RDB_ERROR;
    }
    RDB_init_hashmap(&nodep->map, DEFAULT_VARMAP_SIZE);
    nodep->parentp = current_varmapp;
    current_varmapp = nodep;
    return RDB_OK;
}

static int
drop_local_var(RDB_object *objp, RDB_exec_context *ecp)
{
    RDB_type *typ = RDB_obj_type(objp);

    /* Array and tuple types must be destroyed */
    if (!RDB_type_is_scalar(typ) && !RDB_type_is_relation(typ)) {
        if (RDB_drop_type(typ, ecp, NULL) != RDB_OK)
            return RDB_ERROR;
    }

    if (RDB_destroy_obj(objp, ecp) != RDB_OK)
        return RDB_ERROR;
    
    RDB_free(objp);
    return RDB_OK;
}

static void
destroy_varmap(RDB_hashmap *map)
{
    RDB_hashmap_iter it;
    char *name;
    RDB_object *objp;
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);
    RDB_init_hashmap_iter(&it, map);
    for(;;) {
        objp = RDB_hashmap_next(&it, &name);
        if (name == NULL)
            break;
        if (objp != NULL) {
            drop_local_var(objp, &ec);
        }
    }

    RDB_destroy_hashmap(map);
    RDB_destroy_exec_context(&ec);
}

static void
remove_varmap(void) {
    varmap_node *parentp = current_varmapp->parentp;
    destroy_varmap(&current_varmapp->map);
    RDB_free(current_varmapp);
    current_varmapp = parentp;
}

static RDB_object *
lookup_transient_var(const char *name, varmap_node *varmapp)
{
    RDB_object *objp;
    varmap_node *nodep = varmapp;

    /* Search in local vars */
    while (nodep != NULL) {
        objp = RDB_hashmap_get(&nodep->map, name);
        if (objp != NULL)
            return objp;
        nodep = nodep->parentp;
    }

    /* Search in global transient vars */
    objp = RDB_hashmap_get(&root_module.varmap, name);
    if (objp != NULL)
        return objp;

    /* Search in system module */
    return RDB_hashmap_get(&sys_module.varmap, name);
}

static RDB_object *
lookup_var(const char *name, RDB_exec_context *ecp)
{
    RDB_object *objp = lookup_transient_var(name, current_varmapp);
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
    return lookup_transient_var(name, (varmap_node *) maparg);
}

static RDB_type *
get_var_type(const char *name, void *maparg)
{
    RDB_object *objp = lookup_transient_var(name, (varmap_node *) maparg);
    return objp != NULL ? RDB_obj_type(objp) : NULL;
}

struct op_data {
    RDB_parse_node *stmtlistp;
    int argnamec;
    char **argnamev;
};

static void
free_opdata(RDB_operator *op)
{
    int i;
    RDB_exec_context ec;
    struct op_data *opdatap = RDB_op_u_data(op);

    /* Initialize temporary execution context */
    RDB_init_exec_context(&ec);

    /* Delete code */
    RDB_parse_del_nodelist(opdatap->stmtlistp, &ec);

    RDB_destroy_exec_context(&ec);

    /* Delete arguments */

    for (i = 0; i < opdatap->argnamec; i++) {
        RDB_free(opdatap->argnamev[i]);
    }
    RDB_free(opdatap->argnamev);
}

void
Duro_exit_interp(void)
{
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);

    destroy_varmap(&root_module.varmap);

    /* Destroy only the varmap, not the variables since they are global */
    RDB_destroy_hashmap(&sys_module.varmap);

    if (txnp != NULL) {
        RDB_rollback(&ec, &txnp->tx);

        if (_RDB_parse_interactive)
            printf("Transaction rolled back.\n");
    }
    if (envp != NULL)
        RDB_close_env(envp);
    RDB_destroy_exec_context(&ec);
}

static int
exit_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_exit_interp();
    exit(0);
}   

static int
exit_int_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_exit_interp();
    exit(RDB_obj_int(argv[0]));
}   

static int
connect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret = RDB_open_env(RDB_obj_string(argv[0]), &envp);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
disconnect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret = RDB_close_env(envp);
    envp = NULL;
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        envp = NULL;
        return RDB_ERROR;
    }

    /* What about CURRENT_DB !!? */

    return RDB_OK;
}

static int
create_db_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (envp == NULL) {
        RDB_raise_resource_not_found("no environment", ecp);
        return RDB_ERROR;
    }

    if (RDB_create_db_from_env(RDB_obj_string(argv[0]), envp, ecp) == NULL)
        return RDB_ERROR;
    return RDB_OK;
}   

static int
create_env_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    /* Create directory if does not exist */
    if (mkdir(RDB_obj_string(argv[0]),
            S_IREAD | S_IWRITE | S_IEXEC) == -1
            && errno != EEXIST) {
        RDB_raise_system(strerror(errno), ecp);
        return RDB_ERROR;
    }

    ret = RDB_open_env(RDB_obj_string(argv[0]), &envp);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}   

static int
system_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret = system(RDB_obj_string(argv[0]));
    if (ret == -1 || ret == 127) {
        RDB_errcode_to_error(errno, ecp, txp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(argv[1], (RDB_int) ret);
    return RDB_OK;
}

static int
trace_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int l = RDB_obj_int(argv[0]);
    if (envp == NULL) {
        RDB_raise_resource_not_found("Missing database environment", ecp);
        return RDB_ERROR;
    }
    if (l < 0) {
        RDB_raise_invalid_argument("Invalid trace level", ecp);
        return RDB_ERROR;
    }
    RDB_env_set_trace(envp, (unsigned) l);
    return RDB_OK;
}

static int
load_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_seq_item *seqitv = NULL;
    int seqitc = 0;

    if (argc < 2) {
        RDB_raise_invalid_argument("too few arguments for LOAD", ecp);
        return RDB_ERROR;
    }

    if (RDB_obj_type(argv[0]) == NULL
            || !RDB_type_is_array(RDB_obj_type(argv[0]))) {
        RDB_raise_type_mismatch("first argument must be array", ecp);
        goto error;
    }        

    if (argc > 2) {
        int i;
        
        if (argc % 2 != 0) {
            RDB_raise_invalid_argument("load", ecp);
            return RDB_ERROR;
        }
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * (argc - 2), ecp);
        if (seqitv == NULL)
            return RDB_ERROR;
        seqitc = (argc - 2) / 2;
        for (i = 0; i < seqitc; i++) {
            if (RDB_obj_type(argv[2 + i * 2]) != &RDB_STRING) {
                RDB_raise_type_mismatch("invalid order", ecp);
                goto error;
            }
            if (RDB_obj_type(argv[2 + i * 2 + 1]) != &RDB_STRING) {
                RDB_raise_type_mismatch("invalid order", ecp);
                goto error;
            }
            seqitv[i].attrname = RDB_obj_string(argv[2 + i * 2]);
            seqitv[i].asc = (RDB_bool) (strcmp(RDB_obj_string(argv[2 + i * 2 + 1]), "ASC") == 0);
        }
    }

    if (RDB_table_to_array(argv[0], argv[1], seqitc, seqitv, 0, ecp, txp) != RDB_OK)
        goto error;
    RDB_free(seqitv);
    return RDB_OK;

error:
    RDB_free(seqitv);
    return RDB_ERROR;
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
    static RDB_parameter exit_int_params[1];
    static RDB_parameter connect_params[1];
    static RDB_parameter create_db_params[1];
    static RDB_parameter create_env_params[1];
    static RDB_parameter system_params[2];
    static RDB_parameter trace_params[2];

    RDB_object *objp;

    exit_int_params[0].typ = &RDB_INTEGER;
    exit_int_params[0].update = RDB_FALSE;
    connect_params[0].typ = &RDB_STRING;
    connect_params[0].update = RDB_FALSE;
    create_db_params[0].typ = &RDB_STRING;
    create_db_params[0].update = RDB_FALSE;
    create_env_params[0].typ = &RDB_STRING;
    create_env_params[0].update = RDB_FALSE;
    system_params[0].typ = &RDB_STRING;
    system_params[0].update = RDB_FALSE;
    system_params[1].typ = &RDB_INTEGER;
    system_params[1].update = RDB_TRUE;
    trace_params[0].typ = &RDB_INTEGER;
    trace_params[0].update = RDB_FALSE;

    RDB_init_hashmap(&root_module.varmap, DEFAULT_VARMAP_SIZE);
    RDB_init_hashmap(&sys_module.varmap, DEFAULT_VARMAP_SIZE);
    current_varmapp = NULL;

    RDB_init_op_map(&sys_module.upd_op_map);

    if (RDB_put_upd_op(&sys_module.upd_op_map, "EXIT", 0, NULL, &exit_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&sys_module.upd_op_map, "EXIT", 1, exit_int_params, &exit_int_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&sys_module.upd_op_map, "CONNECT", 1, connect_params, &connect_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&sys_module.upd_op_map, "DISCONNECT", 0, NULL, &disconnect_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&sys_module.upd_op_map, "CREATE_DB", 1, create_db_params,
            &create_db_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&sys_module.upd_op_map, "CREATE_ENV", 1, create_env_params,
            &create_env_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&sys_module.upd_op_map, "SYSTEM", 2, system_params, &system_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&sys_module.upd_op_map, "TRACE", 1, trace_params,
            &trace_op, ecp) != RDB_OK)
        goto error;

    if (_RDB_add_io_ops(&sys_module.upd_op_map, ecp) != RDB_OK)
        goto error;

    if (RDB_put_upd_op(&sys_module.upd_op_map, "LOAD", -1, NULL, &load_op, ecp)
            != RDB_OK)
        goto error;

    objp = new_obj(ecp);
    if (objp == NULL)
        goto error;

    if (RDB_string_to_obj(objp, dbname, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_hashmap_put(&sys_module.varmap, "CURRENT_DB", objp) != RDB_OK) {
        RDB_destroy_obj(objp, ecp);
        RDB_raise_no_memory(ecp);
        goto error;
    }

    if (RDB_hashmap_put(&sys_module.varmap, "STDIN", &DURO_STDIN_OBJ) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    if (RDB_hashmap_put(&sys_module.varmap, "STDOUT", &DURO_STDOUT_OBJ) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    if (RDB_hashmap_put(&sys_module.varmap, "STDERR", &DURO_STDERR_OBJ) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    return RDB_OK;

error:
    RDB_destroy_op_map(&sys_module.upd_op_map);
    return RDB_ERROR;
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

    objv = RDB_alloc(sizeof(RDB_object) * rep->compc, ecp);
    if (objv == NULL) {
        return RDB_ERROR;
    }
    for (i = 0; i < rep->compc; i++)
        RDB_init_obj(&objv[i]);
    objpv = RDB_alloc(sizeof(RDB_object *) * rep->compc, ecp);
    if (objpv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Get selector arguments */
    for (i = 0; i < rep->compc; i++) {
        ret = init_obj(&objv[i], rep->compv[i].typ, ecp, txp);
        if (ret != RDB_OK)
            goto cleanup;
        objpv[i] = &objv[i];
    }

    /* Call selector */
    ret = RDB_call_ro_op_by_name(rep->name, rep->compc, objpv, ecp, txp, objp);

cleanup:
    for (i = 0; i < rep->compc; i++)
        RDB_destroy_obj(&objv[i], ecp);
    RDB_free(objv);
    RDB_free(objpv);

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
        RDB_float_to_obj(objp, 0.0);
    } else if (typ == &RDB_STRING) {
        return RDB_string_to_obj(objp, "", ecp);
    } else if (typ == &RDB_BINARY) {
        return RDB_binary_set(objp, 0, NULL, (size_t) 0, ecp);
    } else if (RDB_type_is_tuple(typ)) {
        for (i = 0; i < typ->var.tuple.attrc; i++) {
            if (RDB_tuple_set(objp, typ->var.tuple.attrv[i].name,
                    NULL, ecp) != RDB_OK)
                return RDB_ERROR;
            if (init_obj(RDB_tuple_get(objp, typ->var.tuple.attrv[i].name),
                    typ->var.tuple.attrv[i].typ, ecp, txp) != RDB_OK)
                return RDB_ERROR;
        }
        typ = RDB_dup_nonscalar_type(typ, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        RDB_obj_set_typeinfo(objp, typ);
    } else if (RDB_type_is_array(typ)) {
        typ = RDB_dup_nonscalar_type(typ, ecp);
        if (typ == NULL)
            return RDB_ERROR;
        RDB_obj_set_typeinfo(objp, typ);
    } else {
        /* Invoke selector */
        if (typ->var.scalar.repc > 0) {
            return init_obj_by_selector(objp, &typ->var.scalar.repv[0],
                    ecp, txp);
        }
    }
    return RDB_OK;
}

static int
exec_vardef(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_object *objp;
    RDB_type *typ = NULL;
    RDB_expression *initexp = NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_transaction *txp = txnp != NULL ? &txnp->tx : NULL;

    /* Init value without type? */
    if (nodep->nextp->kind == RDB_NODE_TOK && nodep->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txp);
        if (initexp == NULL)
            return RDB_ERROR;
    } else {
        typ = RDB_parse_node_to_type(nodep->nextp, &get_var_type,
                current_varmapp, ecp, txp);
        if (typ == NULL)
            return RDB_ERROR;
        if (RDB_type_is_relation(typ)) {
            RDB_raise_syntax("relation type not permitted", ecp);
            return RDB_ERROR;
        }
        if (nodep->nextp->nextp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, txp);
            if (initexp == NULL)
                return RDB_ERROR;
        }
    }

    /*
     * Check if the variable already exists
     */
    if (current_varmapp != NULL && RDB_hashmap_get(&current_varmapp->map, varname) != NULL) {
        RDB_raise_element_exists(varname, ecp);
        return RDB_ERROR;
    }

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        return RDB_ERROR;
    }
    RDB_init_obj(objp);

    if (initexp == NULL) {
        if (init_obj(objp, typ, ecp, txp) != RDB_OK) {
            goto error;
        }
    } else {
        if (RDB_evaluate(initexp, &get_var,
                current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL,
                objp) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(objp) != NULL) {
            /* Check type if type was given */
            if (typ != NULL &&
                    !RDB_type_equals(typ, RDB_obj_type(objp))) {
                RDB_raise_type_mismatch("", ecp);
                goto error;
            }
        } else {
            /* No type available (tuple or array) - set type */
            typ = RDB_expr_type(initexp, get_var_type,
                    current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
            if (typ == NULL)
                goto error;
            typ = RDB_dup_nonscalar_type(typ, ecp);
            if (typ == NULL)
                goto error;
            RDB_obj_set_typeinfo(objp, typ);
        }
    }

    if (current_varmapp != NULL) {
        /* We're in local scope */
        if (RDB_hashmap_put(&current_varmapp->map, varname, objp) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        /* Global scope */
        if (RDB_hashmap_put(&root_module.varmap, varname, objp) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }
    return RDB_OK;

error:
    RDB_destroy_obj(objp, ecp);
    RDB_free(objp);
    return RDB_ERROR;
}

static int
node_to_key(RDB_parse_node *nodep, RDB_exec_context *ecp, RDB_string_vec *vp)
{
    RDB_parse_node *np;

    vp->strc = (RDB_parse_nodelist_length(nodep) + 1) / 2;
    vp->strv = RDB_alloc(sizeof (char *) * vp->strc, ecp);
    if (vp->strv == NULL)
        return RDB_ERROR;

    np = nodep->val.children.firstp;
    if (np != NULL) {
        int i = 0;
        for(;;) {
            vp->strv[i++] = (char *) RDB_parse_node_ID(np);
            np = np->nextp;
            if (np == NULL)
                break;
            np = np->nextp;
        }
    }
    return RDB_OK;
}

static RDB_string_vec *
keylist_to_keyv(RDB_parse_node *nodep, int *keycp, RDB_exec_context *ecp)
{
    RDB_string_vec *keyv;
    RDB_parse_node *np;
    int i;

    /* if (np->val.children.firstp == NULL) ... */

    *keycp = 1;

    np = nodep;
    while (np->val.children.firstp->kind == RDB_NODE_INNER) {
        np = nodep->val.children.firstp;
        (*keycp)++;
    }

    keyv = RDB_alloc(sizeof(RDB_string_vec) * (*keycp), ecp);
    if (keyv == NULL) {
        return NULL;
    }

    for (i = 0; i < *keycp; i++) {
        keyv[i].strv = NULL;
    }

    i = *keycp - 1;
    np = nodep;
    while (np->val.children.firstp->kind == RDB_NODE_INNER) {
        if (node_to_key(np->val.children.firstp->nextp->nextp->nextp, ecp, &keyv[i]) != RDB_OK)
            goto error;
        np = np->val.children.firstp;
        i--;
    }
    if (node_to_key(np->val.children.firstp->nextp->nextp, ecp, &keyv[0]) != RDB_OK)
        goto error;

    return keyv;

error:
    for (i = 0; i < *keycp; i++) {
        RDB_free(keyv[i].strv);
    }
    RDB_free(keyv);
    return NULL;
}

static int
exec_vardef_real(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_string_vec *keyv;
    RDB_bool freekey;
    const char *varname;
    RDB_object tb;
    int keyc;
    int i;
    RDB_parse_node *keylistnodep;
    RDB_type *tbtyp = NULL;
    RDB_object *tbp = NULL;
    RDB_expression *initexp = NULL;

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    varname = RDB_expr_var_name(nodep->exp);

    /* Init value without type? */
    if (nodep->nextp->nextp->kind == RDB_NODE_TOK && nodep->nextp->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, &txnp->tx);
        if (initexp == NULL)
            return RDB_ERROR;
        keylistnodep = nodep->nextp->nextp->nextp->nextp;
    } else {
        tbtyp = RDB_parse_node_to_type(nodep->nextp->nextp, &get_var_type,
                current_varmapp, ecp, &txnp->tx);
        if (tbtyp == NULL)
            return RDB_ERROR;
        if (nodep->nextp->nextp->nextp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->nextp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp, &txnp->tx);
            if (initexp == NULL)
                return RDB_ERROR;
            keylistnodep = nodep->nextp->nextp->nextp->nextp->nextp;
        } else {
            keylistnodep = nodep->nextp->nextp->nextp;
        }
    }

    if (initexp != NULL) {
        RDB_init_obj(&tb);
        if (RDB_evaluate(initexp, get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, &tb) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(&tb) == NULL) {
            RDB_raise_type_mismatch("relation type required", ecp);
            goto error;
        }
    }

    if (tbtyp == NULL) {
        tbtyp = RDB_dup_nonscalar_type(RDB_obj_type(&tb), ecp);
    }
    if (tbtyp == NULL)
        goto error;

    if (keylistnodep->kind == RDB_NODE_INNER) {
        keyv = keylist_to_keyv(keylistnodep, &keyc, ecp);
        if (keyv == NULL)
            goto error;
    } else {
        /* Get keys from expression */
        keyc = _RDB_infer_keys(initexp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR)
            return RDB_ERROR;
    }

    tbp = RDB_create_table_from_type(varname, tbtyp, keyc, keyv, ecp, &txnp->tx);
    if (tbp == NULL) {
        goto error;
    }

    if (initexp != NULL) {
        if (RDB_move_tuples(tbp, &tb, ecp, &txnp->tx) == (RDB_int) RDB_ERROR)
            goto error;
    }

    if (_RDB_parse_interactive)
        printf("Table %s created.\n", varname);

    if (initexp != NULL) {
        RDB_destroy_obj(&tb, ecp);
    }

    if (freekey) {
        for (i = 0; i < keyc; i++) {
            RDB_free(keyv[i].strv);
        }
        RDB_free(keyv);
    }
    return RDB_OK;

error:
    {
        RDB_exec_context ec;

        RDB_init_exec_context(&ec);
        if (tbp != NULL) {
            RDB_drop_table(tbp, &ec, &txnp->tx);
        } else if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp)) {
            RDB_drop_type(tbtyp, &ec, NULL);
        }
        if (initexp != NULL) {
            RDB_destroy_obj(&tb, &ec);
        }
        RDB_destroy_exec_context(&ec);
    }
    if (freekey) {
        for (i = 0; i < keyc; i++) {
            RDB_free(keyv[i].strv);
        }
        RDB_free(keyv);
    }
    return RDB_ERROR;
}

static int
exec_vardef_virtual(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_expression *defexp;
    const char *varname = RDB_expr_var_name(nodep->exp);

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    defexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, &txnp->tx);
    if (defexp == NULL)
        return RDB_ERROR;

    RDB_expression *texp = RDB_dup_expr(defexp, ecp);
    if (texp == NULL)
        return RDB_ERROR;

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
exec_vardef_private(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_object *tbp;
    RDB_string_vec *keyv;
    RDB_object tb;
    RDB_parse_node *keylistnodep;
    int keyc = 0;
    RDB_bool freekey = RDB_TRUE;
    RDB_expression *initexp = NULL;
    RDB_transaction *txp = txnp != NULL ? &txnp->tx : NULL;
    RDB_type *tbtyp = NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);

    /* Init value without type? */
    if (nodep->nextp->nextp->kind == RDB_NODE_TOK
    		&& nodep->nextp->nextp->val.token == TOK_INIT) {
        /* No type - get INIT value */
        initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp, ecp, txp);
        if (initexp == NULL)
            return RDB_ERROR;
        keylistnodep = nodep->nextp->nextp->nextp->nextp;
        RDB_init_obj(&tb);
        if (RDB_evaluate(initexp, get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, &tb) != RDB_OK) {
            goto error;
        }
        if (RDB_obj_type(&tb) == NULL) {
            RDB_raise_type_mismatch("relation type required", ecp);
            goto error;
        }
        tbtyp = RDB_obj_type(&tb);
        tbtyp = RDB_dup_nonscalar_type(tbtyp, ecp);
        if (tbtyp == NULL)
            goto error;
    } else {
        tbtyp = RDB_parse_node_to_type(nodep->nextp->nextp, &get_var_type,
                current_varmapp, ecp, txp);
        if (tbtyp == NULL)
            return RDB_ERROR;
        tbtyp = RDB_dup_nonscalar_type(tbtyp, ecp);
        if (tbtyp == NULL)
            goto error;
        if (nodep->nextp->nextp->nextp->kind == RDB_NODE_TOK
                && nodep->nextp->nextp->nextp->val.token == TOK_INIT) {
            /* Get INIT value */
            initexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp, txp);
            if (initexp == NULL)
                return RDB_ERROR;
            keylistnodep = nodep->nextp->nextp->nextp->nextp->nextp;
        } else {
            keylistnodep = nodep->nextp->nextp->nextp;
        }
    }

    if (!RDB_type_is_relation(tbtyp)) {
        RDB_raise_syntax("relation type required", ecp);
        goto error;
    }

    tbp = RDB_alloc(sizeof(RDB_object), ecp);
    if (tbp == NULL) {
        goto error;
    }

    if (keylistnodep->kind == RDB_NODE_INNER) {
        keyv = keylist_to_keyv(keylistnodep, &keyc, ecp);
        if (keyv == NULL)
            goto error;
    } else {
        /* Get keys from expression */
        keyc = _RDB_infer_keys(initexp, ecp, &keyv, &freekey);
        if (keyc == RDB_ERROR) {
            keyv = NULL;
            goto error;
        }
    }

    RDB_init_obj(tbp);
    if (RDB_init_table_from_type(tbp, varname, tbtyp, keyc, keyv, ecp)
            != RDB_OK) {
        RDB_destroy_obj(tbp, ecp);
        RDB_free(tbp);
        goto error;
    }

    if (initexp != NULL) {
        if (RDB_evaluate(initexp, get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, &tb) != RDB_OK) {
            goto error;
        }
        if (RDB_copy_obj(tbp, &tb, ecp) != RDB_OK) {
            goto error;
        }
    }

    if (current_varmapp != NULL) {
        if (RDB_hashmap_put(&current_varmapp->map, varname, tbp) != RDB_OK) {
            RDB_destroy_obj(tbp, ecp);
            RDB_free(tbp);
            RDB_raise_no_memory(ecp);
            goto error;
        }
    } else {
        if (RDB_hashmap_put(&root_module.varmap, varname, tbp) != RDB_OK) {
            RDB_destroy_obj(tbp, ecp);
            RDB_free(tbp);
            RDB_raise_no_memory(ecp);
            goto error;
        }
    }

    if (_RDB_parse_interactive)
        printf("Local table %s created.\n", varname);

    if (initexp != NULL) {
        RDB_destroy_obj(&tb, ecp);
    }
    return RDB_OK;

error:
    if (tbtyp != NULL && !RDB_type_is_scalar(tbtyp))
        RDB_drop_type(tbtyp, ecp, NULL);
    if (initexp != NULL) {
        RDB_destroy_obj(&tb, ecp);
    }
    return RDB_ERROR;
}

static int
exec_vardrop(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *objp = NULL;

    /* Try to look up local variable */
    if (current_varmapp != NULL)
        objp = RDB_hashmap_get(&current_varmapp->map, varname);
    if (objp == NULL)
        objp = RDB_hashmap_get(&root_module.varmap, varname);
    if (objp != NULL) {
        /* If transient variable found */
        if (current_varmapp != NULL) {
            if (RDB_hashmap_put(&current_varmapp->map, varname, NULL) != RDB_OK)
                return RDB_ERROR;
        } else {
            if (RDB_hashmap_put(&root_module.varmap, varname, NULL) != RDB_OK)
                return RDB_ERROR;
        }
        return drop_local_var(objp, ecp);
    }

    /*
     * Delete persistent table
     */
    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
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

/*
 * Call update operator. nodep points to the operator name token.
 */
static int
exec_call(const RDB_parse_node *nodep, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    int ret;
    int i;
    int argc;
    RDB_expression *exp;
    RDB_type *argtv[DURO_MAX_LLEN];
    RDB_object *argpv[DURO_MAX_LLEN];
    RDB_object argv[DURO_MAX_LLEN];
    RDB_operator *op;
    RDB_parse_node *argp = nodep->nextp->nextp->val.children.firstp;
    const char *opname = RDB_parse_node_ID(nodep);

    /*
     * Get argument types
     */
    argc = 0;
    while (argp != NULL) {
        if (argc > 0)
            argp = argp->nextp;
        exp = RDB_parse_node_expr(argp, ecp, txp);
        if (exp == NULL)
            return RDB_ERROR;
        argtv[argc] = RDB_expr_type(exp, &get_var_type, current_varmapp, ecp,
                txp);
        if (argtv[argc] == NULL)
            return RDB_ERROR;

        argp = argp->nextp;
        argc++;
    }

    /*
     * Get operator
     */
    op = RDB_get_op(&sys_module.upd_op_map, opname, argc, argtv, ecp);
    if (op == NULL) {
        /*
         * If there is transaction and no environment, RDB_get_update_op_e()
         * will fail
         */
        if (txp == NULL && envp == NULL) {
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);

        op = RDB_get_update_op_e(opname, argc, argtv, envp, ecp, txp);
        if (op == NULL) {
            return RDB_ERROR;
        }
    }

    for (i = 0; i < argc; i++) {
        argpv[i] = NULL;
    }

    argp = nodep->nextp->nextp->val.children.firstp;
    i = 0;
    while (argp != NULL) {
        RDB_parameter *paramp;

        if (i > 0)
            argp = argp->nextp;
        exp = RDB_parse_node_expr(argp, ecp, txp);
        if (exp == NULL)
            return RDB_ERROR;
        paramp = RDB_get_parameter(op, i);
        /*
         * paramp may be NULL for n-ary operators
         */
        if (paramp != NULL && paramp->update) {
            const char *varname = RDB_expr_var_name(exp);
            if (varname != NULL) {
                argpv[i] = lookup_var(varname, ecp);
            } else if (exp->kind == RDB_EX_TBP) {
                argpv[i] = exp->var.tbref.tbp;
            }

            /*
             * If it's an update argument and the argument is not a variable,
             * raise an error
             */
            if (argpv[i] == NULL /* && op->updv != NULL && op->updv[i] */) {
                RDB_raise_invalid_argument(
                        "update argument must be a variable", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        /* If the expression has not been resolved as a variable, evaluate it */
        if (argpv[i] == NULL) {
            RDB_init_obj(&argv[i]);
            if (RDB_evaluate(exp, &get_var, current_varmapp, ecp,
                        txp, &argv[i]) != RDB_OK) {
                ret = RDB_ERROR;
                goto cleanup;
            }
            /* Set type if missing */
            if (RDB_obj_type(&argv[i]) == NULL) {
                RDB_type *typ = RDB_expr_type(exp, &get_var_type,
                        current_varmapp, ecp, txp);
                if (typ == NULL) {
                    ret = RDB_ERROR;
                    goto cleanup;
                }
                RDB_obj_set_typeinfo(&argv[i], typ);
            }
            argpv[i] = &argv[i];
        }
        argp = argp->nextp;
        i++;
    }

    /* Invoke function */
    ret = RDB_call_update_op(op, argc, argpv, ecp, txp);

cleanup:
    for (i = 0; i < argc; i++) {
        RDB_parameter *paramp = RDB_get_parameter(op, i);
        if ((argpv[i] != NULL) && (paramp == NULL || !paramp->update)
                && (argpv[i]->kind != RDB_OB_TABLE
                    || !RDB_table_is_persistent(argpv[i])))
            RDB_destroy_obj(&argv[i], ecp);
    }
    return ret;
}

static RDB_database *
get_db(RDB_exec_context *ecp)
{
    char *dbname;
    RDB_object *dbnameobjp = RDB_hashmap_get(&sys_module.varmap, "CURRENT_DB");
    if (dbnameobjp == NULL) {
        RDB_raise_resource_not_found("no database", ecp);
        return NULL;
    }
    dbname = RDB_obj_string(dbnameobjp);
    if (*dbname == '\0') {
        RDB_raise_resource_not_found("no database", ecp);
        return NULL;
    }
    if (envp == NULL) {
        RDB_raise_resource_not_found("no connection", ecp);
        return NULL;
    }

    return RDB_get_db_from_env(dbname, envp, ecp);
}

/*
 * Call update operator. nodep points to the operator name token.
 * If there is no transaction but an db environment is available,
 * start a transaction and try again.
 */
static int
exec_call_retry(const RDB_parse_node *nodep, RDB_exec_context *ecp) {
    int ret;
    RDB_transaction tx;
    RDB_database *dbp;

    ret = exec_call(nodep, ecp, txnp != NULL ? &txnp->tx : NULL);
    /*
     * Success or error different from OPERATOR_NOT_FOUND_ERROR
     * -> return
     */
    if (ret == RDB_OK
            || RDB_obj_type(RDB_get_err(ecp)) != &RDB_OPERATOR_NOT_FOUND_ERROR)
        return ret;
    /*
     * If a transaction is already active or no environment is
     * available, give up
     */
    if (txnp != NULL || envp == NULL)
        return ret;
    /*
     * Start transaction and retry.
     * If this succeeds, the operator will be in memory next time
     * so no transaction will be needed.
     */
    dbp = get_db(ecp);
    if (dbp == NULL) {
        dbp = RDB_get_sys_db(envp, ecp);
        if (dbp == NULL)
            return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
        return RDB_ERROR;
    ret = exec_call(nodep, ecp, &tx);
    if (ret != RDB_OK) {
        RDB_commit(ecp, &tx);
        return ret;
    }
    return RDB_commit(ecp, &tx);
}

static int
exec_load(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_object srctb;
    RDB_parse_node *itemp;
    int i;
    int ret;
    RDB_expression *exp;
    RDB_object **argpv = NULL;
    RDB_object *itemv = NULL;
    int itemc = RDB_parse_nodelist_length(nodep->nextp->nextp->nextp->nextp->nextp) * 2;

    argpv = RDB_alloc((2 + itemc) * sizeof (RDB_object *), ecp);
    if (argpv == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    if (itemc > 0) {
        itemv = RDB_alloc(itemc * sizeof (RDB_object), ecp);
        if (itemv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

    exp = RDB_parse_node_expr(nodep, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    argpv[0] = lookup_var(RDB_expr_var_name(exp), ecp);
    if (argpv[0] == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    exp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    RDB_init_obj(&srctb);
    if (RDB_evaluate(exp, &get_var, current_varmapp, ecp,
            txnp != NULL ? &txnp->tx : NULL, &srctb) != RDB_OK) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    argpv[1] = &srctb;

    itemp = nodep->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
    i = 0;
    while (itemp != NULL) {
        RDB_init_obj(&itemv[i]);
        exp = RDB_parse_node_expr(itemp->val.children.firstp, ecp,
                txnp != NULL ? &txnp->tx : NULL);
        if (exp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        ret = RDB_string_to_obj(&itemv[i], RDB_expr_var_name(exp), ecp);
        if (ret != RDB_OK)
            goto cleanup;
        argpv[2 + i] = &itemv[i];

        RDB_init_obj(&itemv[i + 1]);
        ret = RDB_string_to_obj(&itemv[i + 1],
                itemp->val.children.firstp->nextp->val.token == TOK_ASC ? "ASC" : "DESC", ecp);
        if (ret != RDB_OK)
            goto cleanup;
        argpv[2 + i + 1] = &itemv[i + 1];
        i += 2;
        itemp = itemp->nextp;
    }

    ret = load_op(2 + itemc, argpv, NULL, ecp,
            txnp != NULL ? &txnp->tx : NULL);

cleanup:
    if (itemv != NULL) {
        for (i = 0; i < itemc; i++)
            RDB_destroy_obj(&itemv[i], ecp);
        RDB_free(itemv);
    }
    RDB_free(argpv);
    return ret;
}

static int
Duro_exec_stmt(RDB_parse_node *, RDB_exec_context *, return_info *);

static int
exec_stmts(RDB_parse_node *stmtp, RDB_exec_context *ecp,
        return_info *retinfop)
{
    int ret;
    do {
        ret = Duro_exec_stmt(stmtp, ecp, retinfop);
        if (ret != RDB_OK)
            return ret;
        stmtp = stmtp->nextp;
    } while (stmtp != NULL);
    return RDB_OK;
}

static int
exec_if(RDB_parse_node *nodep, RDB_exec_context *ecp,
        return_info *retinfop)
{
    RDB_bool b;
    int ret;
    RDB_expression *condp = RDB_parse_node_expr(nodep, ecp,
            txnp != NULL ? &txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    if (RDB_evaluate_bool(condp, &get_var, current_varmapp, ecp, NULL, &b)
            != RDB_OK) {
        return RDB_ERROR;
    }
    if (add_varmap(ecp) != RDB_OK)
        return RDB_ERROR;
    if (b) {
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp,
                ecp, retinfop);
    } else if (nodep->nextp->nextp->nextp->val.token == TOK_ELSE) {
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->val.children.firstp,
                ecp, retinfop);
    } else {
        ret = RDB_OK;
    }

    remove_varmap();
    return ret;
}

static int
exec_case(RDB_parse_node *nodep, RDB_exec_context *ecp,
        return_info *retinfop)
{
    RDB_parse_node *whenp;
    int ret;

    if (nodep->kind == RDB_NODE_TOK) {
        /* Skip semicolon */
        nodep = nodep->nextp;
    }

    whenp = nodep->val.children.firstp;
    while (whenp != NULL) {
        RDB_bool b;
        RDB_expression *condp = RDB_parse_node_expr(whenp->val.children.firstp->nextp, ecp,
                txnp != NULL ? &txnp->tx : NULL);
        if (condp == NULL)
            return RDB_ERROR;

        if (RDB_evaluate_bool(condp, &get_var, current_varmapp, ecp, NULL, &b)
                != RDB_OK) {
            return RDB_ERROR;
        }
        if (b) {
            if (add_varmap(ecp) != RDB_OK)
                return RDB_ERROR;
            ret = exec_stmts(whenp->val.children.firstp->nextp->nextp->nextp->val.children.firstp,
                    ecp, retinfop);
            remove_varmap();
            return ret;
        }
        whenp = whenp->nextp;
    }
    if (nodep->nextp->nextp->kind == RDB_NODE_INNER) {
        /* ELSE branch */
        if (add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp,
                ecp, retinfop);
        remove_varmap();
        return ret;
    }
    return RDB_OK;
}

static int
exec_while(RDB_parse_node *nodep, RDB_parse_node *labelp, RDB_exec_context *ecp,
        return_info *retinfop)
{
    int ret;
    RDB_bool b;
    RDB_expression *condp = RDB_parse_node_expr(nodep, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    for(;;) {
        if (RDB_evaluate_bool(condp, &get_var, current_varmapp,
                ecp, NULL, &b) != RDB_OK)
            return RDB_ERROR;
        if (!b)
            return RDB_OK;
        if (add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp, ecp, retinfop);
        if (ret != RDB_OK) {
            if (ret == DURO_LEAVE) {
                /*
                 * If the statement name matches the LEAVE target,
                 * exit loop with RDB_OK
                 */
                if (labelp != NULL
                        && strcmp(labelp->exp->var.varname, leave_targetname) == 0) {
                    ret = RDB_OK;
                }
            }
            remove_varmap();
            return ret;
        }
        remove_varmap();

        /*
         * Check interrupt flag - this allows the user to break out of loops,
         * with Control-C
         */
        if (interrupted) {
            interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            return RDB_ERROR;
        }
    }
}

static int
exec_for(const RDB_parse_node *nodep, const RDB_parse_node *labelp,
		RDB_exec_context *ecp, return_info *retinfop)
{
    int ret;
    RDB_object endval;
    RDB_expression *fromexp, *toexp;
    RDB_object *varp = lookup_transient_var(nodep->exp->var.varname, current_varmapp);
    if (varp == NULL) {
        RDB_raise_name(nodep->exp->var.varname, ecp);
        return RDB_ERROR;
    }

    if (RDB_obj_type(varp) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("loop variable must be INTEGER", ecp);
        return RDB_ERROR;
    }

    fromexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
            txnp != NULL ? &txnp->tx : NULL);
    if (fromexp == NULL)
        return RDB_ERROR;
    if (RDB_evaluate(fromexp, &get_var,
            current_varmapp, ecp, NULL, varp) != RDB_OK) {
        return RDB_ERROR;
    }
    toexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp,
            txnp != NULL ? &txnp->tx : NULL);
    if (toexp == NULL)
        return RDB_ERROR;
    RDB_init_obj(&endval);
    if (RDB_evaluate(toexp, &get_var,
            current_varmapp, ecp, NULL, &endval) != RDB_OK) {
        goto error;
    }
    if (RDB_obj_type(&endval) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("expression must be INTEGER", ecp);
        goto error;
    }

    while (varp->var.int_val <= endval.var.int_val) {
        if (add_varmap(ecp) != RDB_OK)
            goto error;

        /* Execute statements */
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp,
                ecp, retinfop);
        if (ret != RDB_OK) {
            if (ret == DURO_LEAVE) {
                /*
                 * If the statement name matches the LEAVE target,
                 * exit loop with RDB_OK
                 */
                if (labelp != NULL
                        && strcmp(labelp->exp->var.varname, leave_targetname) == 0) {
                    ret = RDB_OK;
                }
            }
            remove_varmap();
            RDB_destroy_obj(&endval, ecp);
            return ret;
        }
        remove_varmap();

        /*
         * Check for user interrupt
         */
        if (interrupted) {
            interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            goto error;
        }
        varp->var.int_val++;
    }
    RDB_destroy_obj(&endval, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&endval, ecp);
    return RDB_ERROR;
}

static RDB_object *
resolve_target(const RDB_expression *exp, RDB_exec_context *ecp)
{
    const char *varname;
    const char *opname = RDB_expr_op_name(exp);

    if (opname != NULL) {
        if (strcmp(opname, "[]") == 0
                && exp->var.op.args.firstp != NULL
                && exp->var.op.args.firstp->nextp != NULL
                && exp->var.op.args.firstp->nextp->nextp == NULL) {
            /* Resolve array subscription */
            RDB_int idx;

            /* Get first argument, which must be an array */
            RDB_object *arrp = resolve_target(exp->var.op.args.firstp, ecp);
            if (arrp == NULL)
                return NULL;
            if (RDB_obj_type(arrp) == NULL
                    || !RDB_type_is_array(RDB_obj_type(arrp))) {
                RDB_raise_type_mismatch("not an array", ecp);
                return NULL;
            }

            /* Get second argument, which must be INTEGER */
            RDB_object idxobj;
            RDB_init_obj(&idxobj);
            if (RDB_evaluate(exp->var.op.args.firstp->nextp, get_var,
                    current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL,
                    &idxobj) != RDB_OK) {
                RDB_destroy_obj(&idxobj, ecp);
                return NULL;
            }
            if (RDB_obj_type(&idxobj) != &RDB_INTEGER) {
                RDB_raise_type_mismatch("array index must be INTEGER", ecp);
                RDB_destroy_obj(&idxobj, ecp);
                return NULL;
            }
            idx = RDB_obj_int(&idxobj);
            RDB_destroy_obj(&idxobj, ecp);
            return RDB_array_get(arrp, idx, ecp);
        }
        RDB_raise_syntax("invalid assignment target", ecp);
        return NULL;
    }

    /* Resolve variable name */
    varname = RDB_expr_var_name(exp);
    if (varname != NULL) {
        return lookup_var(varname, ecp);
    }
    RDB_raise_syntax("invalid assignment target", ecp);
    return NULL;
}

static int
node_to_copy(RDB_ma_copy *copyp, RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_expression *srcexp;
    RDB_expression *dstexp = RDB_parse_node_expr(nodep, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (dstexp == NULL) {
        return RDB_ERROR;
    }

    copyp->dstp = resolve_target(dstexp, ecp);
    if (copyp->dstp == NULL) {
        return RDB_ERROR;
    }

    srcexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (srcexp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_evaluate(srcexp, get_var, current_varmapp, ecp,
            txnp != NULL ? &txnp->tx : NULL, copyp->srcp) != RDB_OK) {
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
tuple_update_to_copy(RDB_ma_copy *copyp, RDB_parse_node *nodep,
        RDB_exec_context *ecp)
{
    RDB_expression *srcexp;
    RDB_expression *srcvarexp;
    RDB_parse_node *ap;

    if (nodep->nextp->val.token == TOK_WHERE) {
        RDB_raise_syntax("WHERE not allowed with tuple UPDATE", ecp);
        goto error;
    }

    /*
     * Get destination
     */
    copyp->dstp = resolve_target(nodep->exp, ecp);
    if (copyp->dstp == NULL) {
        return RDB_ERROR;
    }

    /*
     * Get source value by creating an UPDATE expression
     * and evaluating it
     */
    srcexp = RDB_ro_op("UPDATE", ecp);
    if (srcexp == NULL)
        return RDB_ERROR;
    srcvarexp = RDB_dup_expr(nodep->exp, ecp);
    if (srcvarexp == NULL)
        goto error;
    RDB_add_arg(srcexp, srcvarexp);

    ap = nodep->nextp->nextp->val.children.firstp;
    for(;;) {
        RDB_expression *exp = RDB_string_to_expr(
                RDB_expr_var_name(RDB_parse_node_expr(
                        ap->val.children.firstp, ecp, txnp != NULL ? &txnp->tx : NULL)),
                ecp);
        if (exp == NULL)
            goto error;
        RDB_add_arg(srcexp, exp);

        exp = RDB_parse_node_expr(
                ap->val.children.firstp->nextp->nextp, ecp, txnp != NULL ? &txnp->tx : NULL);
        if (exp == NULL)
            goto error;
        exp = RDB_dup_expr(exp, ecp);
        if (exp == NULL)
            goto error;
        RDB_add_arg(srcexp, exp);

        ap = ap->nextp;
        if (ap == NULL)
            break;

        /* Skip comma */
        ap = ap->nextp;
    }

    if (RDB_evaluate(srcexp, get_var, current_varmapp, ecp,
            txnp != NULL ? &txnp->tx : NULL, copyp->srcp) != RDB_OK) {
        return RDB_ERROR;
    }

    return RDB_OK;

error:
    RDB_drop_expr(srcexp, ecp);
    return RDB_ERROR;
}

static int
node_to_insert(RDB_ma_insert *insp, RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_expression *srcexp;
    RDB_expression *dstexp = RDB_parse_node_expr(nodep, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (dstexp == NULL) {
        return RDB_ERROR;
    }

    insp->tbp = resolve_target(dstexp, ecp);
    if (insp->tbp == NULL) {
        return RDB_ERROR;
    }
    /* Only tables are allowed as target */
    if (insp->tbp->typ == NULL
            || !RDB_type_is_relation(insp->tbp->typ)) {
        RDB_raise_type_mismatch("INSERT target must be relation", ecp);
        return RDB_ERROR;
    }

    srcexp = RDB_parse_node_expr(nodep->nextp, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (srcexp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_evaluate(srcexp, &get_var, current_varmapp, ecp,
            txnp != NULL ? &txnp->tx : NULL, insp->objp) != RDB_OK) {
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
node_to_update(RDB_ma_update *updp, RDB_object *dstp, RDB_parse_node *nodep,
        RDB_exec_context *ecp)
{
	RDB_parse_node *np;
	RDB_parse_node *aafnp;
	int i;

    updp->tbp = dstp;

    if (nodep->nextp->val.token == TOK_WHERE) {
		updp->condp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txnp != NULL ? &txnp->tx : NULL);
		if (nodep == NULL)
			return RDB_ERROR;
		np = nodep->nextp->nextp->nextp->nextp->val.children.firstp;
	} else {
		updp->condp = NULL;
		np = nodep->nextp->nextp->val.children.firstp;
	}

	for (i = 0; i < updp->updc; i++) {
		if (i > 0)
			np = np->nextp;
		aafnp = np->val.children.firstp;
		updp->updv[i].name = RDB_expr_var_name(aafnp->exp);
		updp->updv[i].exp = RDB_parse_node_expr(aafnp->nextp->nextp, ecp,
		        txnp != NULL ? &txnp->tx : NULL);
		if (updp->updv[i].exp == NULL)
			return RDB_ERROR;
		np = np->nextp;
	}
	return RDB_OK;
}

static int
node_to_delete(RDB_ma_delete *delp, RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_expression *dstexp = RDB_parse_node_expr(nodep, ecp,
            txnp != NULL ? &txnp->tx : NULL);
    if (dstexp == NULL) {
        return RDB_ERROR;
    }

    delp->tbp = resolve_target(dstexp, ecp);
    if (delp->tbp == NULL) {
        return RDB_ERROR;
    }
    /* Only tables are allowed as target */
    if (delp->tbp->typ == NULL
            || !RDB_type_is_relation(delp->tbp->typ)) {
        RDB_raise_type_mismatch("INSERT target must be relation", ecp);
        return RDB_ERROR;
    }

    if (nodep->nextp == NULL) {
    	delp->condp = NULL;
    } else {
    	delp->condp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
    	        txnp != NULL ? &txnp->tx : NULL);
    	if (delp->condp == NULL)
    		return RDB_ERROR;
    }

    return RDB_OK;
}

static const RDB_expression *
length_assign(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
	RDB_expression *exp;
	const char *opname;
    RDB_parse_node *tnodep = nodep->val.children.firstp;
	if (tnodep->kind == RDB_NODE_TOK)
		return NULL;

	exp = RDB_parse_node_expr(tnodep, ecp, txnp != NULL ? &txnp->tx : NULL);
	opname = RDB_expr_op_name(exp);
	if (opname == NULL || strcmp(opname, "LENGTH") != 0)
		return NULL;
	exp = exp->var.op.args.firstp;
	if (exp == NULL && exp->nextp != NULL)
		return NULL;
	return exp;
}

static int
exec_assign(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int i;
    int cnt;
    const RDB_expression *arrexp;
    RDB_ma_copy copyv[DURO_MAX_LLEN];
    RDB_ma_insert insv[DURO_MAX_LLEN];
    RDB_ma_update updv[DURO_MAX_LLEN];
    RDB_ma_delete delv[DURO_MAX_LLEN];
    RDB_object srcobjv[DURO_MAX_LLEN];
    RDB_attr_update attrupdv[DURO_MAX_LLEN];
    int copyc = 0;
    int insc = 0;
    int updc = 0;
    int delc = 0;
    int srcobjc = 0;
    int attrupdc = 0;

    /*
     * Special handling for setting array length
     */
    arrexp = length_assign(nodep, ecp);
    if (arrexp != NULL) {
    	if (nodep->nextp != NULL) {
    		RDB_raise_syntax("only single assignment of array length permitted", ecp);
    		return RDB_ERROR;
    	}
    	RDB_type *arrtyp;
    	RDB_expression *srcexp;
    	RDB_object *arrp = resolve_target(arrexp, ecp);
    	if (arrp == NULL)
    		return RDB_ERROR;

        arrtyp = RDB_obj_type(arrp);
        if (arrtyp != NULL && RDB_type_is_array(arrtyp)) {
            RDB_object lenobj;
            RDB_int len;
            RDB_int olen = RDB_array_length(arrp, ecp);
            if (olen < 0)
                return RDB_ERROR;

            RDB_init_obj(&lenobj);
            srcexp = RDB_parse_node_expr(nodep->val.children.firstp->nextp->nextp, ecp,
                    txnp != NULL ? &txnp->tx : NULL);
            if (srcexp == NULL)
            	return RDB_ERROR;
            if (RDB_evaluate(srcexp, get_var,
                    current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL,
                    &lenobj) != RDB_OK) {
                RDB_destroy_obj(&lenobj, ecp);
                return RDB_ERROR;
            }
            if (RDB_obj_type(&lenobj) != &RDB_INTEGER) {
                RDB_raise_type_mismatch("array length must be INTEGER", ecp);
                RDB_destroy_obj(&lenobj, ecp);
                return RDB_ERROR;
            }
            len = RDB_obj_int(&lenobj);
            RDB_destroy_obj(&lenobj, ecp);
            if (RDB_set_array_length(arrp, len, ecp) != RDB_OK) {
                return RDB_ERROR;
            }

            /* Initialize new elements */
            if (len > olen) {
                int i;
                RDB_type *basetyp = RDB_base_type(arrtyp);
                for (i = olen + 1; i < len; i++) {
                    RDB_object *elemp = RDB_array_get(arrp, i, ecp);
                    if (elemp == NULL)
                        return RDB_ERROR;

                    if (init_obj(elemp, basetyp, ecp,
                            txnp != NULL ? &txnp->tx : NULL) != RDB_OK)
                        return RDB_ERROR;
                }
            }

            return RDB_OK;
        }
    }

    /*
     * Convert assignments so they can be passed to RDB_multi_assign()
     */
    for(;;) {
        RDB_object *dstp;
        RDB_expression *dstexp;
        RDB_parse_node *firstp = nodep->val.children.firstp;

        if (firstp->kind == RDB_NODE_TOK) {
        	switch(firstp->val.token) {
                case TOK_INSERT:
                    if (srcobjc >= DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many assigments", ecp);
                        return RDB_ERROR;
                    }

                    RDB_init_obj(&srcobjv[srcobjc++]);
                    insv[insc].objp = &srcobjv[srcobjc - 1];
                    if (node_to_insert(&insv[insc++], firstp->nextp, ecp) != RDB_OK) {
                        goto error;
                    }
                    break;
                case TOK_UPDATE:
                    if (updc >= DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many updates", ecp);
                        return RDB_ERROR;
                    }

                    /* 3rd node must be a token, either WHERE or { */
                	if (firstp->nextp->nextp->val.token == TOK_WHERE) {
                		/* WHERE condition is present */
                		updv[updc].updc = (RDB_parse_nodelist_length(
									firstp->nextp->nextp->nextp->nextp->nextp) + 1) / 2;
                	} else {
                		updv[updc].updc = (RDB_parse_nodelist_length(
									firstp->nextp->nextp->nextp) + 1) / 2;
                	}
                	if (attrupdc + updv[updc].updc > DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many assigments", ecp);
                        goto error;
                	}

                	dstexp = RDB_parse_node_expr(firstp->nextp, ecp,
                	        txnp != NULL ? &txnp->tx : NULL);
                    if (dstexp == NULL) {
                        goto error;
                    }

                    dstp = resolve_target(dstexp, ecp);
                    if (dstp == NULL) {
                        return RDB_ERROR;
                    }

                    /* Only tables and tuples are allowed as target */
                    if (dstp->typ == NULL
                            || !(RDB_type_is_relation(dstp->typ)
                                    || RDB_type_is_tuple(dstp->typ))) {
                        RDB_raise_type_mismatch(
                                "UPDATE target must be tuple or relation", ecp);
                        return RDB_ERROR;
                    }
                    if (dstp->typ != NULL && RDB_type_is_tuple(dstp->typ)) {
                        /*
                         * Tuple update
                         */
                        if (srcobjc >= DURO_MAX_LLEN) {
                            RDB_raise_not_supported("too many assigments", ecp);
                            return RDB_ERROR;
                        }

                        RDB_init_obj(&srcobjv[srcobjc++]);
                        copyv[copyc].srcp = &srcobjv[srcobjc - 1];
                        if (tuple_update_to_copy(&copyv[copyc++], firstp->nextp, ecp) != RDB_OK) {
                            goto error;
                        }
                    } else {
                        if (updc >= DURO_MAX_LLEN) {
                            RDB_raise_not_supported("too many updates", ecp);
                            return RDB_ERROR;
                        }
                        updv[updc].updv = &attrupdv[attrupdc];
                        if (node_to_update(&updv[updc++], dstp, firstp->nextp, ecp)
                                != RDB_OK) {
                            goto error;
                        }
                        attrupdc += updv[updc].updc;
                    }
                	break;
				case TOK_DELETE:
                    if (delc >= DURO_MAX_LLEN) {
                        RDB_raise_not_supported("too many deletes", ecp);
                        return RDB_ERROR;
                    }
                    if (node_to_delete(&delv[delc++], firstp->nextp, ecp) != RDB_OK) {
                        goto error;
                    }
                    break;
            }
        } else {
            if (srcobjc >= DURO_MAX_LLEN) {
                RDB_raise_not_supported("too many assigments", ecp);
                return RDB_ERROR;
            }

            RDB_init_obj(&srcobjv[srcobjc++]);
            copyv[copyc].srcp = &srcobjv[srcobjc - 1];
            if (node_to_copy(&copyv[copyc++], firstp, ecp) != RDB_OK) {
                goto error;
            }
        }

        if (nodep->nextp == NULL)
            break;

        /* Skip comma */
        nodep = nodep->nextp->nextp;
    }

    /*
     * Execute assignments
     */
    cnt = RDB_multi_assign(insc, insv, updc, updv, delc, delv, copyc, copyv,
            ecp, txnp != NULL ? &txnp->tx : NULL);
    if (cnt == (RDB_int) RDB_ERROR)
        goto error;

    if (_RDB_parse_interactive) {
        if (cnt == 1) {
            printf("1 element affected.\n");
        } else {
            printf("%d elements affected.\n", (int) cnt);
        }
    }

    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);

    return RDB_OK;

error:
    for (i = 0; i < srcobjc; i++)
        RDB_destroy_obj(&srcobjv[i], ecp);
    return RDB_ERROR;
}

static int
exec_begin_tx(RDB_exec_context *ecp)
{
    RDB_database *dbp = get_db(ecp);
    if (dbp == NULL)
        return RDB_ERROR;

    if (txnp != NULL) {
        tx_node *ntxnp = RDB_alloc(sizeof(tx_node), ecp);
        if (ntxnp == NULL) {
            return RDB_ERROR;
        }

        if (RDB_begin_tx(ecp, &ntxnp->tx, dbp, &txnp->tx) != RDB_OK) {
            RDB_free(ntxnp);
            return RDB_ERROR;
        }
        ntxnp->parentp = txnp;
        txnp = ntxnp;

        if (_RDB_parse_interactive)
            printf("Subtransaction started.\n");
        return RDB_OK;
    }


    txnp = RDB_alloc(sizeof(tx_node), ecp);
    if (txnp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &txnp->tx, dbp, NULL) != RDB_OK) {
        RDB_free(txnp);
        txnp = NULL;
        return RDB_ERROR;
    }
    txnp->parentp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction started.\n");
    return RDB_OK;
}

static int
exec_commit(RDB_exec_context *ecp)
{
    tx_node *ptxnp;

    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (RDB_commit(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    ptxnp = txnp->parentp;
    RDB_free(txnp);
    txnp = ptxnp;

    if (_RDB_parse_interactive)
        printf("Transaction committed.\n");
    return RDB_OK;
}

static int
exec_rollback(RDB_exec_context *ecp)
{
    if (txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (RDB_rollback(ecp, &txnp->tx) != RDB_OK)
        return RDB_ERROR;

    RDB_free(txnp);
    txnp = NULL;

    if (_RDB_parse_interactive)
        printf("Transaction rolled back.\n");
    return RDB_OK;
}

static int
parserep_to_rep(const RDB_parse_node *nodep, RDB_possrep *rep,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
	int i;
	RDB_parse_node *np;

	if (nodep->val.children.firstp->nextp->kind == RDB_NODE_EXPR) {
		rep->name = (char *) RDB_expr_var_name(nodep->val.children.firstp->nextp->exp);
		nodep = nodep->val.children.firstp->nextp->nextp->nextp;
	} else {
		rep->name = NULL;
		nodep = nodep->val.children.firstp->nextp->nextp;
	}
	rep->compc = (RDB_parse_nodelist_length(nodep) + 1) / 3;

    rep->compv = RDB_alloc(rep->compc * sizeof(RDB_attr), ecp);
    if (rep->compv == NULL)
        return RDB_ERROR;
    np = nodep->val.children.firstp;
    for (i = 0; i < rep->compc; i++) {
        rep->compv[i].name = (char *) RDB_expr_var_name(np->exp);
        np = np->nextp;
        rep->compv[i].typ = (RDB_type *) RDB_parse_node_to_type(np, &get_var_type,
                current_varmapp, ecp, txnp != NULL ? &txnp->tx : NULL);
        if (rep->compv[i].typ == NULL)
            return RDB_ERROR;
        np = np->nextp;
        if (np != NULL) /* Skip comma */
        	np = np->nextp;
    }
    return RDB_OK;
}

static int
exec_typedef(const RDB_parse_node *stmtp, RDB_exec_context *ecp)
{
    int ret;
    int i, j;
    int repc;
    RDB_transaction tmp_tx;
    RDB_possrep *repv;
    RDB_parse_node *nodep;
    RDB_expression *constraintp = NULL;

    if (txnp == NULL) {
        RDB_database *sysdbp;

        if (envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        sysdbp = RDB_get_sys_db(envp, ecp);
        if (sysdbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, sysdbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    repc = RDB_parse_nodelist_length(stmtp->nextp);
    repv = RDB_alloc(repc * sizeof(RDB_possrep), ecp);
    if (repv == NULL)
        goto error;
    nodep = stmtp->nextp->val.children.firstp;
    for (i = 0; i < repc; i++) {
        if (parserep_to_rep(nodep, &repv[i], ecp, txnp != NULL ? &txnp->tx : &tmp_tx) != RDB_OK)
            goto error;
        nodep = nodep->nextp;
    }

    if (stmtp->nextp->nextp->kind != RDB_NODE_TOK) {
        constraintp = RDB_parse_node_expr(stmtp->nextp->nextp->nextp, ecp,
                txnp != NULL ? &txnp->tx : NULL);
        if (constraintp == NULL)
        	goto error;
    }

    if (RDB_define_type(RDB_expr_var_name(stmtp->exp),
            repc, repv, constraintp, ecp, txnp != NULL ? &txnp->tx : &tmp_tx) != RDB_OK)
        goto error;

    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_drop_type(repv[i].compv[j].typ, ecp, NULL);
        }
    }
    RDB_free(repv);
    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf("Type %s defined.\n", RDB_expr_var_name(stmtp->exp));
    return ret;

error:
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_drop_type(repv[i].compv[j].typ, ecp, NULL);
        }
    }
    RDB_free(repv);
    return RDB_ERROR;
}

static const char *
var_of_type(RDB_hashmap *mapp, RDB_type *typ)
{
    RDB_hashmap_iter it;
    char *namp;
    RDB_object *objp;

    RDB_init_hashmap_iter(&it, mapp);
    for(;;) {
        objp = RDB_hashmap_next(&it, &namp);
        if (namp == NULL)
            break;
        if (objp != NULL) {
            RDB_type *vtyp = RDB_obj_type(objp);

            /* If the types are equal, return variable name */
            if (vtyp != NULL && RDB_type_equals(vtyp, typ))
                return namp;
        }
    }
    return NULL;
}

/*
 * Checks if *typ is in use by a transient variable.
 * If it is, returns the name of the variable, otherwise NULL.
 */
static const char *
type_in_use(RDB_type *typ)
{
    const char *typenamp;
    if (current_varmapp != NULL) {
        varmap_node *varnodep = current_varmapp;
        do {
            typenamp = var_of_type(&varnodep->map, typ);
            if (typenamp != NULL)
                return typenamp;
            varnodep = varnodep->parentp;
        } while (varnodep != NULL);
    }
    return var_of_type(&root_module.varmap, typ);
}

static int
exec_typedrop(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    RDB_type *typ;

    if (txnp == NULL) {
        RDB_database *sysdbp;

        if (envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        sysdbp = RDB_get_sys_db(envp, ecp);
        if (sysdbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, sysdbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    typ = RDB_get_type(RDB_expr_var_name(nodep->exp), ecp,
            txnp != NULL ? &txnp->tx : &tmp_tx);
    if (typ == NULL)
        goto error;

    /*
     * Check if a transien variable of that type exists
     */
    if (type_in_use(typ)) {
        RDB_raise_in_use("enable to drop type because a variable with that type exists", ecp);
        goto error;
    }

    ret = RDB_drop_type(typ, ecp, txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if (ret == RDB_OK && _RDB_parse_interactive)
        printf("Type %s dropped.\n", RDB_expr_var_name(nodep->exp));
    return ret;

error:
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_typeimpl(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;

    if (txnp == NULL) {
        RDB_database *sysdbp;

        if (envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        sysdbp = RDB_get_sys_db(envp, ecp);
        if (sysdbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, sysdbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    ret = RDB_implement_type(RDB_expr_var_name(nodep->exp),
            NULL, (RDB_int) -1, ecp, txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if (ret == RDB_OK && _RDB_parse_interactive)
        printf("Type %s implemented.\n", RDB_expr_var_name(nodep->exp));
    return ret;

error:
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

int
Duro_dt_invoke_ro_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int i;
    int ret;
    varmap_node vars;
    RDB_parse_node *codestmtp, *attrnodep;
    struct op_data *opdatap;
    return_info retinfo;
    varmap_node *ovarmapp = current_varmapp;

    if (interrupted) {
        interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    /* Try to get cached statements */
    opdatap = op->u_data;
    if (opdatap == NULL) {
        /*
         * Not available - parse code
         */
        char **argnamev = RDB_alloc(argc * sizeof(char *), ecp);
        if (argnamev == NULL)
            return RDB_ERROR;

        codestmtp = RDB_parse_stmt_string((char *) RDB_operator_iargp(op), ecp);
        if (codestmtp == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }

        attrnodep = codestmtp->val.children.firstp->nextp->nextp->nextp->val.children.firstp;
        for (i = 0; i < argc; i++) {
            /* Skip comma */
            if (i > 0)
                attrnodep = attrnodep->nextp;
            argnamev[i] = (char *) RDB_expr_var_name(RDB_parse_node_expr(attrnodep, ecp,
                    txnp != NULL ? &txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(struct op_data), ecp);
        if (opdatap == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }
        opdatap->stmtlistp = codestmtp->val.children.firstp->nextp->nextp->nextp->nextp
                ->nextp->nextp->nextp->nextp->val.children.firstp;
        opdatap->argnamec = argc;
        opdatap->argnamev = argnamev;

        RDB_set_op_u_data(op, opdatap);
        RDB_set_op_cleanup_fn(op, &free_opdata);
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    current_varmapp = &vars;

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, opdatap->argnamev[i], argv[i])
                != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    retinfo.objp = retvalp;
    retinfo.typ = RDB_return_type(op);
    ret = exec_stmts(opdatap->stmtlistp, ecp, &retinfo);

    /*
     * Keep arguments from being destroyed
     */
    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, opdatap->argnamev[i], NULL) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    current_varmapp = ovarmapp;
    destroy_varmap(&vars.map);

    switch (ret) {
        case RDB_OK:
            RDB_raise_syntax("end of operator reached without RETURN", ecp);
            ret = RDB_ERROR;
            break;
        case DURO_LEAVE:
            RDB_raise_syntax("unmatched LEAVE", ecp);
            ret = RDB_ERROR;
            break;
        case DURO_RETURN:
            ret = RDB_OK;
            break;
    }
    return ret;
}

int
Duro_dt_invoke_update_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    int i;
    varmap_node vars;
    RDB_parse_node *codestmtp;
    RDB_parse_node *attrnodep;
    varmap_node *ovarmapp = current_varmapp;
    struct op_data *opdatap;

    if (interrupted) {
        interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    /* Try to get cached statements */
    opdatap = RDB_op_u_data(op);
    if (opdatap == NULL) {
        char **argnamev = RDB_alloc(argc * sizeof(char *), ecp);
        if (argnamev == NULL)
            return RDB_ERROR;

        /*
         * Not available - parse cod
         */
        codestmtp = RDB_parse_stmt_string((char *) RDB_operator_iargp(op), ecp);
        if (codestmtp == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }

        attrnodep = codestmtp->val.children.firstp->nextp->nextp->nextp->val.children.firstp;
        for (i = 0; i < argc; i++) {
            /* Skip comma */
            if (i > 0)
                attrnodep = attrnodep->nextp;
            argnamev[i] = (char *) RDB_expr_var_name(RDB_parse_node_expr(attrnodep, ecp,
                    txnp != NULL ? &txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(struct op_data), ecp);
        if (opdatap == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }
        opdatap->stmtlistp = codestmtp->val.children.firstp->nextp->nextp->nextp->nextp
                    ->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
        opdatap->argnamec = argc;
        opdatap->argnamev = argnamev;

        RDB_set_op_u_data(op, opdatap);
        RDB_set_op_cleanup_fn(op, &free_opdata);
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    current_varmapp = &vars;

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, opdatap->argnamev[i],
                argv[i]) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    ret = exec_stmts(opdatap->stmtlistp, ecp, NULL);

    /*
     * Keep arguments from being destroyed
     */
    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&current_varmapp->map, opdatap->argnamev[i], NULL)
                != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    current_varmapp = ovarmapp;
    destroy_varmap(&vars.map);

    /* Catch LEAVE */
    if (ret == DURO_LEAVE) {
        RDB_raise_syntax("unmatched LEAVE", ecp);
        ret = RDB_ERROR;
    }
    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

static int
exec_opdef(RDB_parse_node *parentp, RDB_exec_context *ecp)
{
    RDB_bool ro;
    RDB_object code;
    RDB_parse_node *attrnodep;
    RDB_transaction tmp_tx;
    RDB_parameter *paramv = NULL;
    RDB_type *rtyp;
    char *sercodep;
    int ret;
    int i;
    int argc;
    const char *opname;
    RDB_parse_node *stmtp = parentp->val.children.firstp->nextp;

    argc = (int) RDB_parse_nodelist_length(stmtp->nextp->nextp) / 2;
    opname = RDB_expr_var_name(stmtp->exp);

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (txnp == NULL) {
        RDB_database *sysdbp;

        if (envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        sysdbp = RDB_get_sys_db(envp, ecp);
        if (sysdbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, sysdbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /* Convert defining code back */
    RDB_init_obj(&code);
    if (Duro_parse_node_to_obj_string(&code, parentp, ecp,
            txnp != NULL ? &txnp->tx : &tmp_tx) != RDB_OK) {
        goto error;
    }

    paramv = RDB_alloc(argc * sizeof(RDB_parameter), ecp);
    if (paramv == NULL) {
        goto error;
    }

    attrnodep = stmtp->nextp->nextp->val.children.firstp;
    for (i = 0; i < argc; i++) {
        /* Skip comma */
        if (i > 0)
            attrnodep = attrnodep->nextp;

        paramv[i].typ = RDB_parse_node_to_type(attrnodep->nextp,
                &get_var_type, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : &tmp_tx);
        if (paramv[i].typ == NULL)
            goto error;
        attrnodep = attrnodep->nextp->nextp;
    }

    sercodep = RDB_obj_string(&code);

    ro = (RDB_bool) (stmtp->nextp->nextp->nextp->nextp->val.token == TOK_RETURNS);

    if (ro) {
        rtyp = RDB_parse_node_to_type(stmtp->nextp->nextp->nextp->nextp->nextp,
                &get_var_type, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : &tmp_tx);
        if (rtyp == NULL)
            goto error;
        ret = RDB_create_ro_op(opname, argc, paramv, rtyp,
#ifdef _WIN32
                "duro",
#else
                "libduro",
#endif
                "Duro_dt_invoke_ro_op",
                sercodep, strlen(sercodep) + 1, ecp,
                txnp != NULL ? &txnp->tx : &tmp_tx);
        if (ret != RDB_OK)
            goto error;
    } else {
        attrnodep = stmtp->nextp->nextp->val.children.firstp;
        for (i = 0; i < argc; i++) {
            /* Skip comma */
            if (i > 0)
                attrnodep = attrnodep->nextp;

            paramv[i].update = (RDB_bool) (RDB_parse_node_var_name_idx(
						stmtp->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp,
						RDB_expr_var_name(attrnodep->exp)) != -1);
            attrnodep = attrnodep->nextp->nextp;
        }

        ret = RDB_create_update_op(opname,
                argc, paramv,
#ifdef _WIN32
                "duro",
#else
                "libduro",
#endif
                "Duro_dt_invoke_update_op",
                sercodep, strlen(sercodep) + 1, ecp, txnp != NULL ? &txnp->tx : &tmp_tx);
        if (ret != RDB_OK)
            goto error;
    }

    RDB_free(paramv);
    RDB_destroy_obj(&code, ecp);
    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf(ro ? "Read-only operator %s created.\n" : "Update operator %s created.\n", opname);
    return ret;

error:
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    RDB_free(paramv);
    RDB_destroy_obj(&code, ecp);
    return RDB_ERROR;
}

static int
exec_opdrop(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;

    /* !! delete from opmap */

    /*
     * If a transaction is not active, start transaction if a database environment
     * is available
     */
    if (txnp == NULL) {
        RDB_database *sysdbp;

        if (envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        sysdbp = RDB_get_sys_db(envp, ecp);
        if (sysdbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, sysdbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    if (RDB_drop_op(RDB_expr_var_name(nodep->exp), ecp,
            txnp == NULL ? &tmp_tx : &txnp->tx) != RDB_OK) {
        if (txnp == NULL)
            RDB_rollback(ecp, &tmp_tx);
        return RDB_ERROR;
    }

    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if (ret == RDB_OK && _RDB_parse_interactive)
        printf("Operator %s dropped.\n", RDB_expr_var_name(nodep->exp));
    return ret;
}

static int
exec_return(RDB_parse_node *stmtp, RDB_exec_context *ecp,
        return_info *retinfop)
{    
    if (stmtp->kind != RDB_NODE_TOK) {
        RDB_expression *retexp;
        RDB_type *rtyp;

        if (retinfop == NULL) {
            RDB_raise_syntax("invalid RETURN", ecp);
            return RDB_ERROR;
        }

        retexp = RDB_parse_node_expr(stmtp, ecp, txnp != NULL ? &txnp->tx : NULL);
        if (retexp == NULL)
            return RDB_ERROR;

        /*
         * Typecheck
         */
        rtyp = RDB_expr_type(retexp, get_var_type, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL);
        if (rtyp == NULL)
            return RDB_ERROR;
        if (!RDB_type_equals(rtyp, retinfop->typ)) {
            RDB_raise_type_mismatch("invalid return type", ecp);
            return RDB_ERROR;
        }

        /*
         * Evaluate expression
         */
        if (RDB_evaluate(retexp, &get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, retinfop->objp) != RDB_OK)
            return RDB_ERROR;
    } else {
        if (retinfop != NULL) {
            RDB_raise_invalid_argument("return argument expected", ecp);
            return RDB_ERROR;
        }
    }
    return DURO_RETURN;
}

static int
exec_raise(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_object *errp = RDB_raise_err(ecp);
    assert(errp != NULL);

    exp = RDB_parse_node_expr(nodep, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (exp == NULL)
        return RDB_ERROR;

    RDB_evaluate(exp, &get_var, current_varmapp, ecp,
                txnp != NULL ? &txnp->tx : NULL, errp);
    return RDB_ERROR;
}

static int
exec_catch(const RDB_parse_node *catchp, const RDB_type *errtyp,
        RDB_exec_context *ecp, return_info *retinfop)
{
    int ret;
    RDB_object *objp;

    if (add_varmap(ecp) != RDB_OK)
        return RDB_ERROR;

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        goto error;
    }
    RDB_init_obj(objp);
    
    /*
     * Create and initialize local variable
     */
    if (RDB_hashmap_put(&current_varmapp->map, RDB_expr_var_name(catchp->exp), objp)
            != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }
    if (RDB_copy_obj(objp, RDB_get_err(ecp), ecp) != RDB_OK) {
        goto error;
    }

    RDB_clear_err(ecp);

    ret = exec_stmts(catchp->nextp->nextp->kind == RDB_NODE_TOK ?
    		catchp->nextp->nextp->nextp->val.children.firstp :
			catchp->nextp->nextp->val.children.firstp,
			ecp, retinfop);
    remove_varmap();
    return ret;

error:
    remove_varmap();
    return RDB_ERROR;    
}

static int
exec_try(const RDB_parse_node *nodep, RDB_exec_context *ecp, return_info *retinfop)
{
	int ret;
	RDB_parse_node *catchp;

    /* Evaluate all types in the catch clauses
    while (catchp != NULL) {
    	if (catchp->val.children.firstp->nextp->nextp->kind == RDB_NODE_TOK)

        if (catchp->type.exp != NULL && catchp->type.typ == NULL) {
            if (eval_parse_type(&catchp->type, ecp,
                    txnp != NULL ? &txnp->tx : NULL) != RDB_OK)
                return RDB_ERROR;
        }
        catchp = catchp->nextp;
    }
    */

    /*
     * Execute try body
     */
    if (add_varmap(ecp) != RDB_OK)
        return RDB_ERROR;
    ret = exec_stmts(nodep->val.children.firstp, ecp, retinfop);
    remove_varmap();

    if (ret == RDB_ERROR) {
        /*
         * Try to find a catch clause to handle the error
         */
        RDB_object *errp = RDB_get_err(ecp);
        RDB_type *errtyp = RDB_obj_type(errp);

        if (errtyp != NULL) {
        	RDB_type *typ;

            catchp = nodep->nextp->val.children.firstp;

            while (catchp != NULL) {
                if (catchp->val.children.firstp->nextp->nextp->kind
                		!= RDB_NODE_TOK) {
                    /* Catch clause with type */
                    typ = RDB_parse_node_to_type(
                    		catchp->val.children.firstp->nextp->nextp,
                    		&get_var_type, current_varmapp, ecp,
                    		txnp != NULL ? &txnp->tx : NULL);
                    if (typ == NULL)
                    	return RDB_ERROR;
                    if (RDB_type_equals(errtyp, typ)) {
                        return exec_catch(catchp->val.children.firstp->nextp,
                        		typ, ecp, retinfop);
                    }
                } else {
                    /* Catch clause without type */                    
                    return exec_catch(catchp->val.children.firstp->nextp,
                            errtyp, ecp, retinfop);
                }
                catchp = catchp->nextp;
            }
        }
    }
    return ret;
}

static int
exec_constrdef(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    const char *constrname = RDB_expr_var_name(nodep->exp);
    RDB_expression *constrexp;

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (txnp == NULL) {
        RDB_database *dbp = get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    constrexp = RDB_parse_node_expr(nodep->nextp, ecp, txnp != NULL ? &txnp->tx : NULL);
    if (constrexp == NULL)
        goto error;
    constrexp = RDB_dup_expr(constrexp, ecp);
    if (constrexp == NULL)
        goto error;
    ret = RDB_create_constraint(constrname, constrexp, ecp,
    		txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf("Constraint %s created.\n", constrname);
    return ret;

error:
    if (constrexp != NULL)
    	RDB_drop_expr(constrexp, ecp);
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_constrdrop(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    const char *constrname = RDB_expr_var_name(nodep->exp);

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (txnp == NULL) {
        RDB_database *dbp = get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    ret = RDB_drop_constraint(constrname, ecp, txnp != NULL ? &txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && _RDB_parse_interactive)
        printf("Constraint %s dropped.\n", constrname);
    return ret;

error:
    if (txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_leave(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    leave_targetname = nodep->exp->var.varname;
    return DURO_LEAVE;
}

static int
Duro_exec_stmt(RDB_parse_node *stmtp, RDB_exec_context *ecp,
        return_info *retinfop)
{
    int ret = RDB_OK;
    RDB_parse_node *firstchildp;

    if (stmtp->kind != RDB_NODE_INNER) {
        RDB_raise_internal("interpreter encountered invalid node", ecp);
        return RDB_ERROR;
    }
    firstchildp = stmtp->val.children.firstp;
    if (firstchildp->kind == RDB_NODE_TOK) {
        switch (firstchildp->val.token) {
            case TOK_IF:
                ret = exec_if(firstchildp->nextp, ecp, retinfop);
                break;
            case TOK_CASE:
                ret = exec_case(firstchildp->nextp, ecp, retinfop);
                break;
            case TOK_FOR:
                ret = exec_for(firstchildp->nextp, NULL, ecp, retinfop);
                break;
            case TOK_WHILE:
                ret = exec_while(firstchildp->nextp, NULL, ecp, retinfop);
                break;
            case TOK_OPERATOR:
                ret = exec_opdef(stmtp, ecp);
                break;
            case TOK_TRY:
                ret = exec_try(firstchildp->nextp, ecp, retinfop);
                break;
            case TOK_LEAVE:
                ret = exec_leave(firstchildp->nextp, ecp);
                break;
            case ';':
                /* Empty statement */
                ret = RDB_OK;
                break;
            case TOK_CALL:
                ret = exec_call_retry(firstchildp->nextp, ecp);
                break;
            case TOK_VAR:
                if (firstchildp->nextp->nextp->kind == RDB_NODE_TOK) {
                    switch (firstchildp->nextp->nextp->val.token) {
                        case TOK_REAL:
                            ret = exec_vardef_real(firstchildp->nextp, ecp);
                            break;
                        case TOK_VIRTUAL:
                            ret = exec_vardef_virtual(firstchildp->nextp, ecp);
                            break;
                        case TOK_PRIVATE:
                            ret = exec_vardef_private(firstchildp->nextp, ecp);
                            break;
                        default:
                            ret = exec_vardef(firstchildp->nextp, ecp);
                    }
                } else {
                    ret = exec_vardef(firstchildp->nextp, ecp);
                }
                break;
            case TOK_DROP:
                switch (firstchildp->nextp->val.token) {
                    case TOK_VAR:
                        ret = exec_vardrop(firstchildp->nextp->nextp, ecp);
                        break;
                    case TOK_CONSTRAINT:
                        ret = exec_constrdrop(firstchildp->nextp->nextp, ecp);
                        break;
                    case TOK_TYPE:
                        ret = exec_typedrop(firstchildp->nextp->nextp, ecp);
                        break;
                    case TOK_OPERATOR:
                        ret = exec_opdrop(firstchildp->nextp->nextp, ecp);
                        break;
                }
                break;
            case TOK_BEGIN:
                if (firstchildp->nextp->kind == RDB_NODE_INNER) {
                    /* BEGIN ... END */
                    ret = exec_stmts(firstchildp->nextp->val.children.firstp,
                            ecp, retinfop);
                } else {
                    /* BEGIN TRANSACTION */
                    ret = exec_begin_tx(ecp);
                }
                break;
            case TOK_COMMIT:
                ret = exec_commit(ecp);
                break;
            case TOK_ROLLBACK:
                ret = exec_rollback(ecp);
                break;
            case TOK_TYPE:
                ret = exec_typedef(firstchildp->nextp, ecp);
                break;
            case TOK_RETURN:
                ret = exec_return(firstchildp->nextp, ecp, retinfop);
                break;
            case TOK_LOAD:
                ret = exec_load(firstchildp->nextp, ecp);
                break;
            case TOK_CONSTRAINT:
                ret = exec_constrdef(firstchildp->nextp, ecp);
                break;
            case TOK_RAISE:
                ret = exec_raise(firstchildp->nextp, ecp);
                break;
            case TOK_IMPLEMENT:
                ret = exec_typeimpl(firstchildp->nextp->nextp, ecp);
                break;
            default:
                RDB_raise_internal("invalid token", ecp);
                ret = RDB_ERROR;
        }
        if (ret == RDB_ERROR) {
            if (err_line < 0) {
                err_line = stmtp->lineno;
            }
        }
        return ret;
    }
    if (firstchildp->kind == RDB_NODE_EXPR) {
        if (firstchildp->nextp->val.token == '(') {
            /* Operator invocation */
            ret = exec_call_retry(firstchildp, ecp);
        } else {
            switch (firstchildp->nextp->nextp->val.token) {
                case TOK_WHILE:
                    ret = exec_while(firstchildp->nextp->nextp->nextp,
                            firstchildp, ecp, retinfop);
                    break;
                case TOK_FOR:
                    ret = exec_for(firstchildp->nextp->nextp->nextp,
                            firstchildp, ecp, retinfop);
                    break;
                default:
                    RDB_raise_internal("invalid token", ecp);
                    ret = RDB_ERROR;
            }
        }
        if (ret == RDB_ERROR && err_line < 0) {
            err_line = stmtp->lineno;
        }
        return ret;
    }
    if (firstchildp->kind != RDB_NODE_INNER) {
        RDB_raise_internal("interpreter encountered invalid node", ecp);
        return RDB_ERROR;
    }
    /* Assignment */
    ret = exec_assign(firstchildp->val.children.firstp, ecp);
    if (ret == RDB_ERROR && err_line < 0) {
        err_line = stmtp->lineno;
    }
    return ret;
}

int
Duro_process_stmt(RDB_exec_context *ecp)
{
    int ret;
    RDB_parse_node *stmtp;
    RDB_object prompt;
    RDB_object *dbnameobjp = RDB_hashmap_get(&sys_module.varmap, "CURRENT_DB");

    RDB_init_obj(&prompt);
    if (dbnameobjp != NULL && *RDB_obj_string(dbnameobjp) != '\0') {
        RDB_string_to_obj(&prompt, RDB_obj_string(dbnameobjp), ecp);
    } else {
        RDB_string_to_obj(&prompt, "no db", ecp);
    }
    RDB_append_string(&prompt, "> ", ecp);

    _RDB_parse_prompt = RDB_obj_string(&prompt);

    stmtp = RDB_parse_stmt(ecp);

    RDB_destroy_obj(&prompt, ecp);
    if (stmtp == NULL) {
        err_line = yylineno;
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    assert(RDB_get_err(ecp) == NULL);
    ret = Duro_exec_stmt(stmtp, ecp, NULL);

    if (ret != RDB_OK) {
        if (ret == DURO_RETURN) {
            RDB_raise_syntax("invalid RETURN", ecp);
            err_line = yylineno;
            return RDB_ERROR;
        }
        if (ret == DURO_LEAVE) {
            RDB_raise_syntax("unmatched LEAVE", ecp);
            err_line = yylineno;
            return RDB_ERROR;
        }
        RDB_parse_del_node(stmtp, ecp);
        if (RDB_get_err(ecp) == NULL) {
            RDB_raise_internal("statement execution failed, no error available", ecp);
        }
        return RDB_ERROR;
    }

    return RDB_parse_del_node(stmtp, ecp);
}

void
Duro_print_error(const RDB_object *errobjp)
{
    RDB_exec_context ec;
    RDB_object msgobj;
    RDB_type *errtyp = RDB_obj_type(errobjp);

    RDB_init_exec_context(&ec);
    RDB_init_obj(&msgobj);

    fputs(RDB_type_name(errtyp), stdout);

    if (RDB_obj_comp(errobjp, "MSG", &msgobj, &ec, NULL) == RDB_OK) {
        printf(": %s", RDB_obj_string(&msgobj));
    }

    fputs("\n", stdout);

    RDB_destroy_obj(&msgobj, &ec);
    RDB_destroy_exec_context(&ec);
}

void
Duro_dt_interrupt(void)
{
    interrupted = 1;
}

int
Duro_dt_execute(RDB_environment *dbenvp, char *dbname, char *infilename,
        RDB_exec_context *ecp)
{
    envp = dbenvp;
    interrupted = 0;
    FILE *infile = NULL;

    if (infilename != NULL) {
        infile = fopen(infilename, "r");
        if (infile == NULL) {
            RDB_raise_resource_not_found(infilename, ecp);
            return RDB_ERROR;
        }
    }

    if (Duro_init_exec(ecp, dbname) != RDB_OK) {
        goto error;
    }

    err_line = -1;

    _RDB_parse_interactive = (RDB_bool) isatty(fileno(infile != NULL ? infile : stdin));

    if (_RDB_parse_interactive) {
        printf("Duro D/T library version %d.%d\n", RDB_major_version(),
                RDB_minor_version());
    }
    _RDB_parse_init_buf(infile != NULL ? infile : stdin);

    for(;;) {
        if (Duro_process_stmt(ecp) != RDB_OK) {
            RDB_object *errobjp = RDB_get_err(ecp);
            if (errobjp != NULL) {
                if (!_RDB_parse_interactive) {
                    printf("error in statement at or near line %d: ", err_line);
                }
                if (_RDB_parse_interactive) {
                    Duro_print_error(errobjp);
                    _RDB_parse_init_buf(NULL);
                } else {
                    Duro_exit_interp();
                    goto error;
                }
                RDB_clear_err(ecp);
            } else {
                /* Exit on EOF  */
                puts("");
                Duro_exit_interp();
                if (infile != NULL) {
                    fclose(infile);
                    infile = NULL;
                }
                return RDB_OK;
            }
        }
    }

error:
    if (infile != NULL) {
        fclose(infile);
    }
    return RDB_ERROR;
}
