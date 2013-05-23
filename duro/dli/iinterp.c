/*
 * $Id$
 *
 * Copyright (C) 2007-2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Statement execution functions.
 */

#include "iinterp.h"
#include "interp_core.h"
#include "interp_assign.h"
#include "exparse.h"
#include "parse.h"
#include <gen/hashmap.h>
#include <gen/hashmapit.h>
#include <gen/releaseno.h>
#include <rel/rdb.h>
#include <rel/tostr.h>
#include <rel/typeimpl.h>
#include <rel/qresult.h>
#include <rel/optimize.h>
#include <rel/internal.h>

#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

extern int yylineno;

int err_line;

typedef struct yy_buffer_state *YY_BUFFER_STATE;

static RDB_object prompt;

static const char *leave_targetname;

static const char *impl_typename;

static RDB_object current_db_obj;
static RDB_object implicit_tx_obj;

void
Duro_exit_interp(void)
{
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);

    Duro_destroy_vars();

    if (RDB_parse_get_interactive()) {
        RDB_destroy_obj(&prompt, &ec);
    }

    if (Duro_txnp != NULL) {
        RDB_rollback(&ec, &Duro_txnp->tx);

        if (RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }
    if (Duro_envp != NULL)
        RDB_close_env(Duro_envp);
    RDB_destroy_exec_context(&ec);
}

/*
 * Operator exit() without arguments
 */
static int
exit_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_exit_interp();
    exit(0);
}   

/*
 * Operator exit() with argument
 */
static int
exit_int_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int exitcode = RDB_obj_int(argv[0]);
    Duro_exit_interp();
    exit(exitcode);
}   

static int
connect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    /*
     * Try opening the environment without RDB_CREATE first
     * to attach to existing memory pool
     */
    int ret = RDB_open_env(RDB_obj_string(argv[0]), &Duro_envp, 0);
    if (ret != RDB_OK) {
        /*
         * Retry with RDB_CREATE option, re-creating necessary files
         * and running recovery
         */
        int ret = RDB_open_env(RDB_obj_string(argv[0]), &Duro_envp,
                RDB_CREATE);
        if (ret != RDB_OK) {
            RDB_errcode_to_error(ret, ecp, txp);
            Duro_envp = NULL;
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
disconnect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *dbnameobjp;
    int ret;

    if (Duro_envp == NULL) {
        RDB_raise_resource_not_found("no database environment", ecp);
        return RDB_ERROR;
    }

    if (current_foreachp != NULL) {
        RDB_raise_in_use("disconnect() not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    /* If a transaction is active, abort it */
    if (Duro_txnp != NULL) {
        ret = RDB_rollback(ecp, &Duro_txnp->tx);
        Duro_txnp = NULL;
        if (ret != RDB_OK)
            return ret;

        if (RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }

    /* Close DB environment */
    ret = RDB_close_env(Duro_envp);
    Duro_envp = NULL;
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        Duro_envp = NULL;
        return RDB_ERROR;
    }

    /* If CURRENT_DB was set, set it to empty string */
    dbnameobjp = RDB_hashmap_get(&Duro_sys_module.varmap, "current_db");
    if (dbnameobjp == NULL || *RDB_obj_string(dbnameobjp) == '\0') {
        return RDB_OK;
    }
    return RDB_string_to_obj(dbnameobjp, "", ecp);
}

static int
create_db_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (Duro_envp == NULL) {
        RDB_raise_resource_not_found("no environment", ecp);
        return RDB_ERROR;
    }

    if (RDB_create_db_from_env(RDB_obj_string(argv[0]), Duro_envp, ecp) == NULL)
        return RDB_ERROR;
    return RDB_OK;
}

static int
create_env_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;

    /* Create directory if does not exist */
#ifdef _WIN32
    ret = _mkdir(RDB_obj_string(argv[0]));
#else
    ret = mkdir(RDB_obj_string(argv[0]),
            S_IRUSR | S_IWUSR | S_IXUSR
            | S_IRGRP | S_IWGRP | S_IXGRP);
#endif
    if (ret == -1 && errno != EEXIST) {
        RDB_raise_system(strerror(errno), ecp);
        return RDB_ERROR;
    }

    ret = RDB_open_env(RDB_obj_string(argv[0]), &Duro_envp, RDB_CREATE);
    if (ret != RDB_OK) {
        RDB_errcode_to_error(ret, ecp, txp);
        Duro_envp = NULL;
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
    if (Duro_envp == NULL) {
        RDB_raise_resource_not_found("Missing database environment", ecp);
        return RDB_ERROR;
    }
    if (l < 0) {
        RDB_raise_invalid_argument("Invalid trace level", ecp);
        return RDB_ERROR;
    }
    RDB_env_set_trace(Duro_envp, (unsigned) l);
    return RDB_OK;
}

int
Duro_init_interp(RDB_exec_context *ecp, const char *dbname)
{
    static RDB_parameter exit_int_params[1];
    static RDB_parameter connect_params[1];
    static RDB_parameter create_db_params[1];
    static RDB_parameter create_env_params[1];
    static RDB_parameter system_params[2];
    static RDB_parameter trace_params[2];

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

    Duro_init_vars();

    leave_targetname = NULL;
    impl_typename = NULL;
    current_foreachp = NULL;

    RDB_init_op_map(&Duro_sys_module.upd_op_map);

    RDB_init_obj(&current_db_obj);
    RDB_init_obj(&implicit_tx_obj);

    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "exit", 0, NULL, &exit_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "exit", 1, exit_int_params, &exit_int_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "connect", 1, connect_params, &connect_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "disconnect", 0, NULL, &disconnect_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "create_db", 1, create_db_params,
            &create_db_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "create_env", 1, create_env_params,
            &create_env_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "system", 2, system_params, &system_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&Duro_sys_module.upd_op_map, "trace", 1, trace_params,
            &trace_op, ecp) != RDB_OK)
        goto error;

    /* Create current_db and implicit_tx in system module */

    if (RDB_string_to_obj(&current_db_obj, dbname, ecp) != RDB_OK) {
        goto error;
    }

    RDB_bool_to_obj(&implicit_tx_obj, RDB_FALSE);

    if (RDB_hashmap_put(&Duro_sys_module.varmap, "current_db", &current_db_obj)
            != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    if (RDB_hashmap_put(&Duro_sys_module.varmap, "implicit_tx", &implicit_tx_obj)
            != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    return RDB_OK;

error:
    RDB_destroy_obj(&current_db_obj, ecp);
    RDB_destroy_obj(&implicit_tx_obj, ecp);

    RDB_destroy_op_map(&Duro_sys_module.upd_op_map);
    return RDB_ERROR;
}

static int
evaluate_retry_bool(RDB_expression *exp, RDB_exec_context *ecp, RDB_bool *resultp)
{
    int ret;
    RDB_object result;

    RDB_init_obj(&result);
    ret = Duro_evaluate_retry(exp, ecp, &result);
    if (ret != RDB_OK) {
        RDB_destroy_obj(&result, ecp);
        return ret;
    }
    *resultp = RDB_obj_bool(&result);
    RDB_destroy_obj(&result, ecp);
    return RDB_OK;
}

/*
 * Call update operator. nodep points to the operator name token.
 */
static int
exec_call(const RDB_parse_node *nodep, RDB_exec_context *ecp)
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
        exp = RDB_parse_node_expr(argp, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        if (exp == NULL)
            return RDB_ERROR;
        argtv[argc] = Duro_expr_type_retry(exp, ecp);
        if (argtv[argc] == NULL)
            return RDB_ERROR;

        argp = argp->nextp;
        argc++;
    }

    /*
     * Get operator
     */
    op = RDB_get_op(&Duro_sys_module.upd_op_map, opname, argc, argtv, ecp);
    if (op == NULL) {
        RDB_bool type_mismatch =
                (RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_MISMATCH_ERROR);

        /*
         * If there is transaction and no environment, RDB_get_update_op_e()
         * will fail
         */
        if (Duro_txnp == NULL && Duro_envp == NULL) {
            return RDB_ERROR;
        }
        RDB_clear_err(ecp);

        op = RDB_get_update_op_e(opname, argc, argtv, Duro_envp, ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        /*
         * If the operator was not found and no transaction is running start a transaction
         * for reading the operator from the catalog only
         */
        if (op == NULL && RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR
                && Duro_txnp == NULL) {
            RDB_transaction tx;
            RDB_database *dbp = Duro_get_db(ecp);
            if (dbp == NULL)
                return RDB_ERROR;
            if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
                return RDB_ERROR;
            op = RDB_get_update_op(opname, argc, argtv, ecp, &tx);
            if (RDB_commit(ecp, &tx) != RDB_OK)
                return RDB_ERROR;
        }
        if (op == NULL) {
            /*
             * If the error is operator_not_found_error but the
             * previous lookup returned type_mismatch_error,
             * raise type_mismatch_error
             */
            if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR
                    && type_mismatch) {
                RDB_raise_type_mismatch(opname, ecp);
            }
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
        const char *varname;

        if (i > 0)
            argp = argp->nextp;
        exp = RDB_parse_node_expr(argp, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        if (exp == NULL)
            return RDB_ERROR;
        paramp = RDB_get_parameter(op, i);
        /*
         * paramp may be NULL for n-ary operators
         */
        varname = RDB_expr_var_name(exp);
        if (varname != NULL) {
            /*
             * If the expression is a variable, look it up
             * (If the parameter is not an update parameter,
             * the callee must not modify the variable)
             */
            argpv[i] = Duro_lookup_var(varname, ecp);
            if (argpv[i] == NULL) {
                ret = RDB_ERROR;
                goto cleanup;
            }
        } else {
            if (exp->kind == RDB_EX_TBP) {
                /* Special handling of table refs */
                argpv[i] = exp->def.tbref.tbp;
            } else if (paramp != NULL && paramp->update) {
                /*
                 * If it's an update argument and the argument is not a variable,
                 * raise an error
                 */
                RDB_raise_invalid_argument(
                        "update argument must be a variable", ecp);
                ret = RDB_ERROR;
                goto cleanup;
            }
        }

        /* If the expression has not been resolved as a variable, evaluate it */
        if (argpv[i] == NULL) {
            RDB_init_obj(&argv[i]);
            if (Duro_evaluate_retry(exp, ecp, &argv[i]) != RDB_OK) {
                ret = RDB_ERROR;
                goto cleanup;
            }
            /* Set type if missing */
            if (RDB_obj_type(&argv[i]) == NULL) {
                RDB_type *typ = RDB_expr_type(exp, &Duro_get_var_type,
                        NULL, Duro_envp, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
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
    ret = RDB_call_update_op(op, argc, argpv, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);

cleanup:
    for (i = 0; i < argc; i++) {
        if (argpv[i] == &argv[i])
            RDB_destroy_obj(&argv[i], ecp);
    }
    return ret;
}

static int
nodes_to_seqitv(RDB_seq_item *seqitv, RDB_parse_node *nodep,
        RDB_exec_context *ecp)
{
    RDB_expression *exp;
    int i = 0;

    if (nodep != NULL) {
        for (;;) {
            /* Get attribute name */
            exp = RDB_parse_node_expr(nodep->val.children.firstp, ecp,
                    Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
            if (exp == NULL) {
                return RDB_ERROR;
            }
            seqitv[i].attrname = (char *) RDB_expr_var_name(exp);

            /* Get ascending/descending info */
            seqitv[i].asc = (RDB_bool)
                    (nodep->val.children.firstp->nextp->val.token == TOK_ASC);

            nodep = nodep->nextp;
            if (nodep == NULL)
                break;

            /* Skip comma */
            nodep = nodep->nextp;

            i++;
        }
    }
    return RDB_OK;
}

static int
exec_load(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    RDB_object srctb;
    RDB_object *srctbp;
    int ret;
    RDB_expression *tbexp;
    RDB_object *dstp;
    const char *srcvarname;
    int seqitc;
    RDB_parse_node *seqitnodep;
    RDB_seq_item *seqitv = NULL;

    RDB_init_obj(&srctb);

    tbexp = RDB_parse_node_expr(nodep, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (tbexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    dstp = Duro_lookup_var(RDB_expr_var_name(tbexp), ecp);
    if (dstp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    tbexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (tbexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /*
     * If the expression is a variable reference, look up the variable,
     * otherwise evaluate the expression
     */
    srcvarname = RDB_expr_var_name(tbexp);
    if (srcvarname != NULL) {
        srctbp = Duro_lookup_var(srcvarname, ecp);
        if (srctbp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    } else {
        if (Duro_evaluate_retry(tbexp, ecp, &srctb) != RDB_OK) {
            ret = RDB_ERROR;
            goto cleanup;
        }
        srctbp = &srctb;
    }

    seqitnodep = nodep->nextp->nextp->nextp->nextp->nextp;
    seqitc = (RDB_parse_nodelist_length(seqitnodep) + 1) / 2;
    if (seqitc > 0) {
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * seqitc, ecp);
        if (seqitv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }
    ret = nodes_to_seqitv(seqitv, seqitnodep->val.children.firstp, ecp);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    ret = RDB_table_to_array(dstp, srctbp, seqitc, seqitv, 0, ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : NULL);

cleanup:
    if (seqitv != NULL)
        RDB_free(seqitv);
    RDB_destroy_obj(&srctb, ecp);
    return ret;
}

static int
exec_explain(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_expression *exp;
    RDB_object strobj;
    int seqitc;
    RDB_seq_item *seqitv = NULL;
    RDB_expression *optexp = NULL;

    if (Duro_txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&strobj);

    exp = RDB_parse_node_expr(nodep, ecp, &Duro_txnp->tx);
    if (exp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    seqitc = RDB_parse_nodelist_length(nodep->nextp->nextp->nextp);
    if (seqitc > 0) {
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * seqitc, ecp);
        if (seqitv == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    }

    /* Perform type checking */
    if (RDB_expr_type(exp, NULL, NULL, Duro_envp, ecp, &Duro_txnp->tx) == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    ret = nodes_to_seqitv(seqitv,
            nodep->nextp->nextp->nextp->val.children.firstp, ecp);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    /* Optimize */
    optexp = RDB_optimize_expr(exp, seqitc, seqitv, NULL, ecp, &Duro_txnp->tx);
    if (optexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Convert tree to string */
    ret = RDB_expr_to_str(&strobj, optexp, ecp, &Duro_txnp->tx, RDB_SHOW_INDEX);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    ret = puts(RDB_obj_string(&strobj));
    if (ret == EOF) {
        RDB_errcode_to_error(errno, ecp, &Duro_txnp->tx);
    } else {
        ret = RDB_OK;
    }
    fflush(stdout);

cleanup:
    if (seqitv != NULL)
        RDB_free(seqitv);
    if (optexp != NULL) {
        RDB_del_expr(optexp, ecp);
    }
    RDB_destroy_obj(&strobj, ecp);
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
            Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    if (evaluate_retry_bool(condp, ecp, &b)
            != RDB_OK) {
        return RDB_ERROR;
    }
    if (Duro_add_varmap(ecp) != RDB_OK)
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

    Duro_remove_varmap();
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
                Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        if (condp == NULL)
            return RDB_ERROR;

        if (evaluate_retry_bool(condp, ecp, &b)
                != RDB_OK) {
            return RDB_ERROR;
        }
        if (b) {
            if (Duro_add_varmap(ecp) != RDB_OK)
                return RDB_ERROR;
            ret = exec_stmts(whenp->val.children.firstp->nextp->nextp->nextp->val.children.firstp,
                    ecp, retinfop);
            Duro_remove_varmap();
            return ret;
        }
        whenp = whenp->nextp;
    }
    if (nodep->nextp->nextp->kind == RDB_NODE_INNER) {
        /* ELSE branch */
        if (Duro_add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp,
                ecp, retinfop);
        Duro_remove_varmap();
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
    RDB_expression *condp = RDB_parse_node_expr(nodep, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    for(;;) {
        if (evaluate_retry_bool(condp, ecp, &b) != RDB_OK)
            return RDB_ERROR;
        if (!b)
            return RDB_OK;
        if (Duro_add_varmap(ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp, ecp, retinfop);
        if (ret != RDB_OK) {
            if (ret == DURO_LEAVE) {
                /*
                 * If the statement name matches the LEAVE target,
                 * exit loop with RDB_OK
                 */
                if (labelp != NULL
                        && strcmp(RDB_expr_var_name(labelp->exp),
                                leave_targetname) == 0) {
                    /* Target name matches label */
                    ret = RDB_OK;
                } else if (leave_targetname == NULL) {
                    /* No label and target is NULL */
                    ret = RDB_OK;
                }
            }
            Duro_remove_varmap();
            break;
        }
        Duro_remove_varmap();

        /*
         * Check interrupt flag - this allows the user to break out of loops,
         * with Control-C
         */
        if (Duro_interrupted) {
            Duro_interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            return RDB_ERROR;
        }
    }
    return ret;
}

static int
exec_for(const RDB_parse_node *nodep, const RDB_parse_node *labelp,
		RDB_exec_context *ecp, return_info *retinfop)
{
    int ret;
    RDB_object endval;
    RDB_expression *fromexp, *toexp;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *varp = Duro_lookup_transient_var(varname);
    if (varp == NULL) {
        RDB_raise_name(RDB_expr_var_name(nodep->exp), ecp);
        return RDB_ERROR;
    }

    if (RDB_obj_type(varp) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("loop variable must be of type integer", ecp);
        return RDB_ERROR;
    }

    fromexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (fromexp == NULL)
        return RDB_ERROR;
    if (Duro_evaluate_retry(fromexp, ecp, varp) != RDB_OK) {
        return RDB_ERROR;
    }
    toexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (toexp == NULL)
        return RDB_ERROR;
    RDB_init_obj(&endval);
    if (Duro_evaluate_retry(toexp, ecp, &endval) != RDB_OK) {
        goto error;
    }
    if (RDB_obj_type(&endval) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("expression must be of type integer", ecp);
        goto error;
    }

    while (RDB_obj_int(varp) <= RDB_obj_int(&endval)) {
        if (Duro_add_varmap(ecp) != RDB_OK)
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
                        && strcmp(RDB_expr_var_name(labelp->exp),
                                leave_targetname) == 0) {
                    ret = RDB_OK;
                } else if (leave_targetname == NULL) {
                    /* No label and target is NULL */
                    ret = RDB_OK;
                }
            }
            Duro_remove_varmap();
            RDB_destroy_obj(&endval, ecp);
            return ret;
        }
        Duro_remove_varmap();

        /* Check if the variable has been dropped */
        varp = Duro_lookup_transient_var(varname);
        if (varp == NULL) {
            RDB_raise_name(varname, ecp);
            goto error;
        }

        /*
         * Check for user interrupt
         */
        if (Duro_interrupted) {
            Duro_interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            goto error;
        }
        varp->val.int_val++;
    }
    RDB_destroy_obj(&endval, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&endval, ecp);
    return RDB_ERROR;
}

static int
exec_foreach(const RDB_parse_node *nodep, const RDB_parse_node *labelp,
        RDB_exec_context *ecp, return_info *retinfop)
{
    int ret;
    RDB_expression *tbexp;
    RDB_type *tbtyp;
    int seqitc;
    foreach_iter it;
    RDB_parse_node *seqitnodep;
    RDB_seq_item *seqitv = NULL;
    const char *srcvarname;
    RDB_object tb;
    RDB_transaction *txp = Duro_txnp != NULL ? &Duro_txnp->tx : NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *varp = Duro_lookup_transient_var(varname);
    if (varp == NULL) {
        RDB_raise_name(varname, ecp);
        return RDB_ERROR;
    }

    it.qrp = NULL;

    tbexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txp);
    if (tbexp == NULL)
        return RDB_ERROR;

    tbtyp = Duro_expr_type_retry(tbexp, ecp);
    if (tbtyp == NULL)
        return RDB_ERROR;
    if (!RDB_type_is_relation(tbtyp)) {
        RDB_raise_type_mismatch("table required", ecp);
        return RDB_ERROR;
    }
    if (!RDB_type_equals(RDB_obj_type(varp), RDB_base_type(tbtyp))) {
        RDB_raise_type_mismatch("type of loop variable does not match table type",
                ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&tb);
    /*
     * If the expression is a variable reference, look up the variable,
     * otherwise evaluate the expression
     */
    srcvarname = RDB_expr_var_name(tbexp);
    if (srcvarname != NULL) {
        it.tbp = Duro_lookup_var(srcvarname, ecp);
        if (it.tbp == NULL)
            goto error;
    } else {
        if (Duro_evaluate_retry(tbexp, ecp, &tb) != RDB_OK) {
            goto error;
        }
        it.tbp = &tb;
    }

    seqitnodep = nodep->nextp->nextp->nextp->nextp->nextp;
    seqitc = (RDB_parse_nodelist_length(seqitnodep) + 1) / 2;
    if (seqitc > 0) {
        seqitv = RDB_alloc(sizeof(RDB_seq_item) * seqitc, ecp);
        if (seqitv == NULL)
            goto error;
    }
    seqitnodep = nodep->nextp->nextp->nextp->nextp->nextp;
    if (nodes_to_seqitv(seqitv, seqitnodep->val.children.firstp, ecp)
                    != RDB_OK) {
        goto error;
    }
    it.qrp = RDB_table_iterator(it.tbp, seqitc, seqitv, ecp, txp);
    if (it.qrp == NULL)
        goto error;

    it.prevp = current_foreachp;
    current_foreachp = &it;

    while (RDB_next_tuple(it.qrp, varp, ecp, txp) == RDB_OK) {
        if (Duro_add_varmap(ecp) != RDB_OK)
            goto error;

        /* Execute statements */
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->nextp
                ->nextp->nextp->nextp->val.children.firstp,
                ecp, retinfop);
        Duro_remove_varmap();
        if (ret == DURO_LEAVE) {
            /*
             * If the statement name matches the LEAVE target,
             * exit loop with RDB_OK
             */
            if (labelp != NULL
                    && strcmp(RDB_expr_var_name(labelp->exp),
                            leave_targetname) == 0) {
                ret = RDB_OK;
            } else if (leave_targetname == NULL) {
                /* No label and target is NULL, exit loop */
                ret = RDB_OK;
            }
            break;
        }
        if (ret != RDB_OK)
            goto error;

        /* Check if the variable has been dropped */
        varp = Duro_lookup_transient_var(varname);
        if (varp == NULL) {
            RDB_raise_name(varname, ecp);
            goto error;
        }

        /*
         * Check for user interrupt
         */
        if (Duro_interrupted) {
            Duro_interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            goto error;
        }
    }

    /* Set to previous FOREACH iterator (or NULL) */
    current_foreachp = it.prevp;

    RDB_free(seqitv);
    if (RDB_del_qresult(it.qrp, ecp, txp) != RDB_OK) {
        RDB_destroy_obj(&tb, ecp);
        return RDB_ERROR;
    }

    RDB_destroy_obj(&tb, ecp);

    /* Ignore not_found */
    if (ret == RDB_ERROR) {
        if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
            return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    return RDB_OK;

error:
    if (it.qrp != NULL) {
        RDB_del_qresult(it.qrp, ecp, txp);
        current_foreachp = it.prevp;
    }
    RDB_destroy_obj(&tb, ecp);
    RDB_free(seqitv);
    return RDB_ERROR;
}

static int
do_begin_tx(RDB_exec_context *ecp)
{
    RDB_database *dbp = Duro_get_db(ecp);
    if (dbp == NULL)
        return RDB_ERROR;

    if (Duro_txnp != NULL) {
        /* Start subtransaction */
        tx_node *ntxnp = RDB_alloc(sizeof(tx_node), ecp);
        if (ntxnp == NULL) {
            return RDB_ERROR;
        }

        if (RDB_begin_tx(ecp, &ntxnp->tx, dbp, &Duro_txnp->tx) != RDB_OK) {
            RDB_free(ntxnp);
            return RDB_ERROR;
        }
        ntxnp->parentp = Duro_txnp;
        Duro_txnp = ntxnp;

        return RDB_OK;
    }

    Duro_txnp = RDB_alloc(sizeof(tx_node), ecp);
    if (Duro_txnp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &Duro_txnp->tx, dbp, NULL) != RDB_OK) {
        RDB_free(Duro_txnp);
        Duro_txnp = NULL;
        return RDB_ERROR;
    }
    Duro_txnp->parentp = NULL;

    return RDB_OK;
}

static int
exec_begin_tx(RDB_exec_context *ecp)
{
    int subtx = (Duro_txnp != NULL);

    if (do_begin_tx(ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf(subtx ? "Subtransaction started.\n"
                : "Transaction started.\n");

    return RDB_OK;
}

static int
do_commit(RDB_exec_context *ecp)
{
    tx_node *ptxnp;

    if (Duro_txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (current_foreachp != NULL) {
        RDB_raise_in_use("COMMIT not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    if (RDB_commit(ecp, &Duro_txnp->tx) != RDB_OK)
        return RDB_ERROR;

    ptxnp = Duro_txnp->parentp;
    RDB_free(Duro_txnp);
    Duro_txnp = ptxnp;

    return RDB_OK;
}

static int
exec_commit(RDB_exec_context *ecp)
{
    if (do_commit(ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Transaction committed.\n");

    return RDB_OK;
}

static int
do_rollback(RDB_exec_context *ecp)
{
    if (Duro_txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (current_foreachp != NULL) {
        RDB_raise_in_use("ROLLBACK not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    if (RDB_rollback(ecp, &Duro_txnp->tx) != RDB_OK)
        return RDB_ERROR;

    RDB_free(Duro_txnp);
    Duro_txnp = NULL;

    return RDB_OK;
}

static int
exec_rollback(RDB_exec_context *ecp)
{
    if (do_rollback(ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
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
        rep->compv[i].typ = (RDB_type *) RDB_parse_node_to_type(np,
                &Duro_get_var_type, NULL, ecp, txp);
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
    RDB_expression *initexp;
    RDB_expression *constraintp = NULL;

    if (Duro_txnp == NULL) {
        RDB_database *dbp;

        if (Duro_envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        dbp = Duro_get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    repc = RDB_parse_nodelist_length(stmtp->nextp);
    repv = RDB_alloc(repc * sizeof(RDB_possrep), ecp);
    if (repv == NULL)
        goto error;
    nodep = stmtp->nextp->val.children.firstp;
    for (i = 0; i < repc; i++) {
        if (parserep_to_rep(nodep, &repv[i], ecp, Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx) != RDB_OK)
            goto error;
        nodep = nodep->nextp;
    }

    if (stmtp->nextp->nextp->val.token == TOK_CONSTRAINT) {
        constraintp = RDB_parse_node_expr(stmtp->nextp->nextp->nextp, ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        if (constraintp == NULL)
        	goto error;
        initexp = RDB_parse_node_expr(stmtp->nextp->nextp->nextp->nextp->nextp, ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        if (initexp == NULL)
            goto error;
    } else {
        initexp = RDB_parse_node_expr(stmtp->nextp->nextp->nextp, ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        if (initexp == NULL)
            goto error;
    }

    if (RDB_define_type(RDB_expr_var_name(stmtp->exp),
            repc, repv, constraintp, initexp, ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx) != RDB_OK)
        goto error;

    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_del_nonscalar_type(repv[i].compv[j].typ, ecp);
        }
    }
    RDB_free(repv);
    if (Duro_txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Type %s defined.\n", RDB_expr_var_name(stmtp->exp));
    return ret;

error:
    if (Duro_txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (repv[i].compv[j].typ != NULL
                    && !RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_del_nonscalar_type(repv[i].compv[j].typ, ecp);
        }
    }
    RDB_free(repv);
    return RDB_ERROR;
}

static int
exec_typedrop(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    RDB_type *typ;

    /*
     * DROP TYPE is not allowed in user-defined operators,
     * to prevent a type used by an operator from being dropped
     * while the operator is being executed
     */
    if (Duro_inner_op != NULL) {
        RDB_raise_syntax("DROP TYPE not permitted in user-defined operators",
                ecp);
        return RDB_ERROR;
    }

    if (Duro_txnp == NULL) {
        RDB_database *dbp;

        if (Duro_envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        dbp = Duro_get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    typ = RDB_get_type(RDB_expr_var_name(nodep->exp), ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
    if (typ == NULL)
        goto error;

    /*
     * Check if a transient variable of that type exists
     */
    if (Duro_type_in_use(typ)) {
        RDB_raise_in_use("enable to drop type because a variable with that type exists", ecp);
        goto error;
    }

    if (RDB_drop_typeimpl_ops(typ, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx)
            != RDB_OK)
        goto error;

    if (RDB_drop_type(RDB_type_name(typ), ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx) != RDB_OK)
        goto error;

    if (Duro_txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Type %s dropped.\n", RDB_expr_var_name(nodep->exp));
    return ret;

error:
    if (Duro_txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_typeimpl(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    RDB_type *ityp = NULL;

    if (Duro_txnp == NULL) {
        RDB_database *dbp;

        if (Duro_envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        dbp = Duro_get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    if (nodep->nextp->val.token == TOK_AS) {
        return_info retinfo;

        ityp = Duro_parse_node_to_type_retry(nodep->nextp->nextp, ecp);
        if (ityp == NULL)
            return RDB_ERROR;
        impl_typename = RDB_expr_var_name(nodep->exp);
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->val.children.firstp,
                ecp, &retinfo);
        impl_typename = NULL;
        if (ret != RDB_OK)
            return RDB_ERROR;
    }

    ret = RDB_implement_type(RDB_expr_var_name(nodep->exp),
            ityp, (RDB_int) -1, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (Duro_txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Type %s implemented.\n", RDB_expr_var_name(nodep->exp));
    return ret;

error:
    if (Duro_txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static void
free_opdata(RDB_operator *op)
{
    int i;
    RDB_exec_context ec;
    struct Duro_op_data *opdatap = RDB_op_u_data(op);

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

int
Duro_dt_invoke_ro_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *retvalp)
{
    int i;
    int ret;
    varmap_node vars;
    RDB_parse_node *codestmtp, *attrnodep;
    struct Duro_op_data *opdatap;
    return_info retinfo;
    varmap_node *ovarmapp;
    int isselector;
    RDB_type *getter_utyp = NULL;
    RDB_operator *parent_op;

    if (Duro_interrupted) {
        Duro_interrupted = 0;
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

        codestmtp = RDB_parse_stmt_string(RDB_operator_source(op), ecp);
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
                    Duro_txnp != NULL ? &Duro_txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(struct Duro_op_data), ecp);
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
    ovarmapp = Duro_set_current_varmap(&vars);

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&vars.map, opdatap->argnamev[i], argv[i])
                != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    retinfo.objp = retvalp;
    retinfo.typ = RDB_return_type(op);

    /*
     * Selectors and getters of user-defined types
     * require special treatment, because they are accessing
     * the actual type
     */

    /*
     * If the operator is a selector, the type of the return expression
     * is the type that is used as the actual representation
     */
    isselector = RDB_is_selector(op)
            && (retinfo.typ->def.scalar.arep != NULL);
    if (isselector) {
        retinfo.typ = retinfo.typ->def.scalar.arep;
    }

    /*
     * If the operator is a getter, set the argument type
     * to the type that is used as the actual representation
     */
    if (RDB_is_getter(op)
            && (RDB_obj_type(argv[0])->def.scalar.arep != NULL)) {
        getter_utyp = RDB_obj_type(argv[0]);
        RDB_obj_set_typeinfo(argv[0], getter_utyp->def.scalar.arep);
    }

    parent_op = Duro_inner_op;
    Duro_inner_op = op;
    ret = exec_stmts(opdatap->stmtlistp, ecp, &retinfo);
    Duro_inner_op = parent_op;

    /* Set type of return value to the user-defined type */
    if (isselector) {
        RDB_obj_set_typeinfo(retinfo.objp, op->rtyp);
    }

    if (getter_utyp != NULL) {
        RDB_obj_set_typeinfo(argv[0], getter_utyp);
    }

    /*
     * Keep arguments from being destroyed
     */
    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&vars.map, opdatap->argnamev[i], NULL) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    Duro_set_current_varmap(ovarmapp);
    Duro_destroy_varmap(&vars.map);

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
    varmap_node *ovarmapp;
    struct Duro_op_data *opdatap;
    RDB_operator *parent_op;
    RDB_type *setter_utyp = NULL;

    if (Duro_interrupted) {
        Duro_interrupted = 0;
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
         * Not available - parse code
         */
        codestmtp = RDB_parse_stmt_string(RDB_operator_source(op), ecp);
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
                    Duro_txnp != NULL ? &Duro_txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(struct Duro_op_data), ecp);
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
    ovarmapp = Duro_set_current_varmap(&vars);

    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&vars.map, opdatap->argnamev[i],
                argv[i]) != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    /*
     * If the operator is a setter, set the type of the first argument
     * to the type that is used as the actual representation
     */
    if (RDB_is_setter(op)
            && (RDB_obj_type(argv[0])->def.scalar.arep != NULL)) {
        setter_utyp = RDB_obj_type(argv[0]);
        RDB_obj_set_typeinfo(argv[0], setter_utyp->def.scalar.arep);
    }

    parent_op = Duro_inner_op;
    Duro_inner_op = op;
    ret = exec_stmts(opdatap->stmtlistp, ecp, NULL);
    Duro_inner_op = parent_op;

    if (setter_utyp != NULL) {
        RDB_obj_set_typeinfo(argv[0], setter_utyp);
    }

    /*
     * Keep arguments from being destroyed
     */
    for (i = 0; i < argc; i++) {
        if (RDB_hashmap_put(&vars.map, opdatap->argnamev[i], NULL)
                != RDB_OK) {
            RDB_raise_no_memory(ecp);
            return RDB_ERROR;
        }
    }

    Duro_set_current_varmap(ovarmapp);
    Duro_destroy_varmap(&vars.map);

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
    RDB_type *rtyp;
    int ret;
    int i;
    const char *opname;
    RDB_object opnameobj; /* Only used when the name is modified */
    RDB_parse_node *stmtp = parentp->val.children.firstp->nextp;
    RDB_parameter *paramv = NULL;
    int paramc = (int) RDB_parse_nodelist_length(stmtp->nextp->nextp) / 2;

    RDB_init_obj(&opnameobj);
    opname = RDB_expr_var_name(stmtp->exp);

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (Duro_txnp == NULL) {
        RDB_database *dbp;

        if (Duro_envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        dbp = Duro_get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    /* Convert defining code back */
    RDB_init_obj(&code);
    if (Duro_parse_node_to_obj_string(&code, parentp, ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx) != RDB_OK) {
        goto error;
    }

    paramv = RDB_alloc(paramc * sizeof(RDB_parameter), ecp);
    if (paramv == NULL) {
        goto error;
    }

    attrnodep = stmtp->nextp->nextp->val.children.firstp;
    for (i = 0; i < paramc; i++) {
        /* Skip comma */
        if (i > 0)
            attrnodep = attrnodep->nextp;

        paramv[i].typ = RDB_parse_node_to_type(attrnodep->nextp,
                &Duro_get_var_type, NULL, ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
        if (paramv[i].typ == NULL)
            goto error;
        attrnodep = attrnodep->nextp->nextp;
    }

    ro = (RDB_bool)
            (stmtp->nextp->nextp->nextp->nextp->val.token == TOK_RETURNS);

    if (ro) {
        rtyp = RDB_parse_node_to_type(stmtp->nextp->nextp->nextp->nextp->nextp,
                &Duro_get_var_type, NULL, ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
        if (rtyp == NULL)
            goto error;

        if (impl_typename != NULL) {
            /*
             * We're inside a IMPLEMENT TYPE; ... END IMPLEMENT block.
             * Only selector and getters allowed
             */
            if (strstr(opname, "get_") == opname) {
                /* Prepend operator name with <typename>_ */
                if (RDB_string_to_obj(&opnameobj, impl_typename, ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, "_", ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, opname, ecp) != RDB_OK)
                    goto error;
                opname = RDB_obj_string(&opnameobj);
            } else if (!RDB_type_is_scalar(rtyp)
                    || strcmp(RDB_type_name(rtyp), impl_typename) != 0) {
                /* Not a selector */
                RDB_raise_syntax("invalid operator", ecp);
                goto error;
            }
        }

        ret = RDB_create_ro_op(opname, paramc, paramv, rtyp,
#ifdef _WIN32
                "duro",
#else
                "libduro",
#endif
                "Duro_dt_invoke_ro_op",
                RDB_obj_string(&code), ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
        if (ret != RDB_OK)
            goto error;
    } else {
        if (impl_typename != NULL) {
            /* Only setters allowed */
            if (strstr(opname, "set_") == opname) {
                /* Prepend operator name with <typename>_ */
                if (RDB_string_to_obj(&opnameobj, impl_typename, ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, "_", ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, opname, ecp) != RDB_OK)
                    goto error;
                opname = RDB_obj_string(&opnameobj);
            } else {
                RDB_raise_syntax("invalid operator", ecp);
                goto error;
            }
        }

        for (i = 0; i < paramc; i++)
            paramv[i].update = RDB_FALSE;

        /*
         * Set paramv[].update
         */
        attrnodep = stmtp->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
        while (attrnodep != NULL) {
            const char *updname = RDB_expr_var_name(attrnodep->exp);
            RDB_parse_node *paramnodep = stmtp->nextp->nextp->val.children.firstp;

            for (i = 0; i < paramc; i++) {
                if (strcmp(RDB_expr_var_name(paramnodep->exp), updname) == 0) {
                    paramv[i].update = RDB_TRUE;
                    break;
                }
                paramnodep = paramnodep->nextp->nextp;
                if (paramnodep != NULL)
                    paramnodep = paramnodep->nextp;
            }
            if (i == paramc) {
                RDB_raise_invalid_argument("invalid update parameter", ecp);
                goto error;
            }
            attrnodep = attrnodep->nextp;
            if (attrnodep != NULL)
                attrnodep = attrnodep->nextp;
        }

        ret = RDB_create_update_op(opname,
                paramc, paramv,
#ifdef _WIN32
                "duro",
#else
                "libduro",
#endif
                "Duro_dt_invoke_update_op",
                RDB_obj_string(&code), ecp,
                Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
        if (ret != RDB_OK)
            goto error;
    }

    RDB_free(paramv);
    RDB_destroy_obj(&code, ecp);
    if (Duro_txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf(ro ? "Read-only operator %s created.\n" : "Update operator %s created.\n", opname);
    RDB_destroy_obj(&opnameobj, ecp);
    return ret;

error:
    if (Duro_txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    RDB_free(paramv);
    RDB_destroy_obj(&code, ecp);
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
exec_opdrop(const RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;

    /*
     * DROP OPERATOR is not allowed in user-defined operators,
     * to prevent an operator from being dropped while it is executed
     */
    if (Duro_inner_op != NULL) {
        RDB_raise_syntax("DROP OPERATOR not permitted in user-defined operators",
                ecp);
        return RDB_ERROR;
    }

    /* !! delete from opmap */

    /*
     * If a transaction is not active, start transaction if a database environment
     * is available
     */
    if (Duro_txnp == NULL) {
        RDB_database *dbp;

        if (Duro_envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            return RDB_ERROR;
        }

        dbp = Duro_get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    if (RDB_drop_op(RDB_expr_var_name(nodep->exp), ecp,
            Duro_txnp == NULL ? &tmp_tx : &Duro_txnp->tx) != RDB_OK) {
        if (Duro_txnp == NULL)
            RDB_rollback(ecp, &tmp_tx);
        return RDB_ERROR;
    }

    if (Duro_txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
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

        retexp = RDB_parse_node_expr(stmtp, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
        if (retexp == NULL)
            return RDB_ERROR;

        /*
         * Typecheck
         */
        rtyp = Duro_expr_type_retry(retexp, ecp);
        if (rtyp == NULL)
            return RDB_ERROR;
        if (!RDB_type_equals(rtyp, retinfop->typ)) {
            RDB_raise_type_mismatch("invalid return type", ecp);
            return RDB_ERROR;
        }

        /*
         * Evaluate expression
         */
        if (Duro_evaluate_retry(retexp, ecp, retinfop->objp) != RDB_OK)
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

    exp = RDB_parse_node_expr(nodep, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (exp == NULL)
        return RDB_ERROR;

    Duro_evaluate_retry(exp, ecp, errp);
    return RDB_ERROR;
}

static int
exec_catch(const RDB_parse_node *catchp, const RDB_type *errtyp,
        RDB_exec_context *ecp, return_info *retinfop)
{
    int ret;
    RDB_object *objp;

    if (Duro_add_varmap(ecp) != RDB_OK)
        return RDB_ERROR;

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        goto error;
    }
    RDB_init_obj(objp);

    /*
     * Create and initialize local variable
     */
    if (Duro_put_var(RDB_expr_var_name(catchp->exp), objp, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_copy_obj(objp, RDB_get_err(ecp), ecp) != RDB_OK) {
        goto error;
    }

    RDB_clear_err(ecp);
    err_line = -1;

    ret = exec_stmts(catchp->nextp->nextp->kind == RDB_NODE_TOK ?
    		catchp->nextp->nextp->nextp->val.children.firstp :
			catchp->nextp->nextp->val.children.firstp,
			ecp, retinfop);
    Duro_remove_varmap();
    return ret;

error:
    Duro_remove_varmap();
    return RDB_ERROR;    
}

static int
exec_try(const RDB_parse_node *nodep, RDB_exec_context *ecp, return_info *retinfop)
{
	int ret;
	RDB_parse_node *catchp;

    /*
     * Execute try body
     */
    if (Duro_add_varmap(ecp) != RDB_OK)
        return RDB_ERROR;
    ret = exec_stmts(nodep->val.children.firstp, ecp, retinfop);
    Duro_remove_varmap();

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
                    typ = Duro_parse_node_to_type_retry(
                    		catchp->val.children.firstp->nextp->nextp, ecp);
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
    if (Duro_txnp == NULL) {
        RDB_database *dbp = Duro_get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    constrexp = RDB_parse_node_expr(nodep->nextp, ecp,
            Duro_txnp != NULL ? &Duro_txnp->tx : NULL);
    if (constrexp == NULL)
        goto error;
    constrexp = RDB_dup_expr(constrexp, ecp);
    if (constrexp == NULL)
        goto error;
    ret = RDB_create_constraint(constrname, constrexp, ecp,
    		Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (Duro_txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Constraint %s created.\n", constrname);
    return ret;

error:
    if (constrexp != NULL)
    	RDB_del_expr(constrexp, ecp);
    if (Duro_txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_indexdef(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    int i;
    const char *indexname = RDB_expr_var_name(nodep->exp);
    RDB_object *tbp;
    int idxcompc = (RDB_parse_nodelist_length(nodep->nextp->nextp->nextp) + 1) / 2;
    RDB_seq_item *idxcompv;
    RDB_parse_node *attrnodep;

    if (Duro_txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp = RDB_get_table(RDB_expr_var_name(nodep->nextp->exp),
            ecp, &Duro_txnp->tx);
    if (tbp == NULL) {
        return RDB_ERROR;
    }

    idxcompv = RDB_alloc(sizeof(RDB_seq_item) * idxcompc, ecp);
    if (idxcompv == NULL)
        return RDB_ERROR;

    attrnodep = nodep->nextp->nextp->nextp->val.children.firstp;
    i = 0;
    for(;;) {
        idxcompv[i].attrname = (char *) RDB_expr_var_name(attrnodep->exp);
        idxcompv[i].asc = RDB_TRUE;
        attrnodep = attrnodep->nextp;
        if (attrnodep == NULL)
            break;
        /* Skip comma */
        attrnodep = attrnodep->nextp;
        i++;
    }

    ret = RDB_create_table_index(indexname, tbp, idxcompc,
            idxcompv, RDB_ORDERED, ecp, &Duro_txnp->tx);
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Index %s created.\n", indexname);
    RDB_free(idxcompv);
    return ret;
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
    if (Duro_txnp == NULL) {
        RDB_database *dbp = Duro_get_db(ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    ret = RDB_drop_constraint(constrname, ecp, Duro_txnp != NULL ? &Duro_txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (Duro_txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Constraint %s dropped.\n", constrname);
    return ret;

error:
    if (Duro_txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_indexdrop(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    const char *indexname = RDB_expr_var_name(nodep->exp);

    if (Duro_txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    ret = RDB_drop_table_index(indexname, ecp, &Duro_txnp->tx);
    if (ret != RDB_OK)
        goto error;

    if (RDB_parse_get_interactive())
        printf("Index %s dropped.\n", indexname);
    return ret;

error:
    if (Duro_txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_leave(RDB_parse_node *nodep, RDB_exec_context *ecp)
{
    if (nodep->kind == RDB_NODE_TOK) {
        leave_targetname = NULL;
    } else {
        leave_targetname = RDB_expr_var_name(nodep->exp);
    }
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
            case TOK_FOREACH:
                ret = exec_foreach(firstchildp->nextp, NULL, ecp, retinfop);
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
                ret = exec_call(firstchildp->nextp, ecp);
                break;
            case TOK_VAR:
                if (firstchildp->nextp->nextp->kind == RDB_NODE_TOK) {
                    switch (firstchildp->nextp->nextp->val.token) {
                        case TOK_REAL:
                            ret = Duro_exec_vardef_real(firstchildp->nextp, ecp);
                            break;
                        case TOK_VIRTUAL:
                            ret = Duro_exec_vardef_virtual(firstchildp->nextp, ecp);
                            break;
                        case TOK_PRIVATE:
                            ret = Duro_exec_vardef_private(firstchildp->nextp, ecp);
                            break;
                        default:
                            ret = Duro_exec_vardef(firstchildp->nextp, ecp);
                    }
                } else {
                    ret = Duro_exec_vardef(firstchildp->nextp, ecp);
                }
                break;
            case TOK_DROP:
                switch (firstchildp->nextp->val.token) {
                    case TOK_VAR:
                        ret = Duro_exec_vardrop(firstchildp->nextp->nextp, ecp);
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
                    case TOK_INDEX:
                        ret = exec_indexdrop(firstchildp->nextp->nextp, ecp);
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
            case TOK_INDEX:
                ret = exec_indexdef(firstchildp->nextp, ecp);
                break;
            case TOK_EXPLAIN:
                if (firstchildp->nextp->nextp->nextp == NULL) {
                    ret = Duro_exec_explain_assign(firstchildp->nextp, ecp);
                } else {
                    ret = exec_explain(firstchildp->nextp, ecp);
                }
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
            ret = exec_call(firstchildp, ecp);
        } else {
            /* Loop with label */
            switch (firstchildp->nextp->nextp->val.token) {
                case TOK_WHILE:
                    ret = exec_while(firstchildp->nextp->nextp->nextp,
                            firstchildp, ecp, retinfop);
                    break;
                case TOK_FOR:
                    ret = exec_for(firstchildp->nextp->nextp->nextp,
                            firstchildp, ecp, retinfop);
                    break;
                case TOK_FOREACH:
                    ret = exec_foreach(firstchildp->nextp->nextp->nextp,
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
    ret = Duro_exec_assign(firstchildp, ecp);
    if (ret == RDB_ERROR && err_line < 0) {
        err_line = stmtp->lineno;
    }
    return ret;
}

/*
 * Check if *stmt is a COMMIT, ROLLBACK, or BEGIN TX
 */
static RDB_bool
is_tx_stmt(RDB_parse_node *stmtp) {
    RDB_parse_node *firstchildp = stmtp->val.children.firstp;

    return firstchildp->kind == RDB_NODE_TOK
            && (firstchildp->val.token == TOK_COMMIT
                || firstchildp->val.token == TOK_ROLLBACK
                || (firstchildp->val.token == TOK_BEGIN
                    && firstchildp->nextp->kind == RDB_NODE_TOK));
}

static int
Duro_exec_stmt_impl_tx(RDB_parse_node *stmtp, RDB_exec_context *ecp)
{
    /*
     * No implicit transaction if the statement is a BEGIN TX, COMMIT,
     * or ROLLBACK.
     */
    RDB_bool implicit_tx = RDB_obj_bool(&implicit_tx_obj)
                    && !is_tx_stmt(stmtp) && (Duro_txnp == NULL);

    /* No implicit tx if no database is available. */
    if (implicit_tx) {
        if (Duro_get_db(ecp) == NULL) {
            RDB_clear_err(ecp);
            implicit_tx = RDB_FALSE;
        }
    }

    if (implicit_tx) {
        if (do_begin_tx(ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (Duro_exec_stmt(stmtp, ecp, NULL) != RDB_OK) {
        if (implicit_tx) {
            do_rollback(ecp);
        }
        return RDB_ERROR;
    }

    if (implicit_tx) {
        return do_commit(ecp);
    }
    return RDB_OK;
}

int
Duro_process_stmt(RDB_exec_context *ecp)
{
    int ret;
    RDB_parse_node *stmtp;
    RDB_object *dbnameobjp = RDB_hashmap_get(&Duro_sys_module.varmap, "current_db");

    if (RDB_parse_get_interactive()) {
        /* Build prompt */
        if (dbnameobjp != NULL && *RDB_obj_string(dbnameobjp) != '\0') {
            ret = RDB_string_to_obj(&prompt, RDB_obj_string(dbnameobjp), ecp);
        } else {
            ret = RDB_string_to_obj(&prompt, "no db", ecp);
        }
        if (ret != RDB_OK)
            return ret;
        RDB_append_string(&prompt, "> ", ecp);
    }

    stmtp = RDB_parse_stmt(ecp);

    if (stmtp == NULL) {
        err_line = yylineno;
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    ret = Duro_exec_stmt_impl_tx(stmtp, ecp);

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

    fputs (RDB_type_name(errtyp), stderr);

    if (RDB_obj_comp(errobjp, "msg", &msgobj, NULL, &ec, NULL) == RDB_OK) {
        fprintf(stderr, ": %s", RDB_obj_string(&msgobj));
    }

    fputs ("\n", stderr);

    RDB_destroy_obj(&msgobj, &ec);
    RDB_destroy_exec_context(&ec);
}

void
Duro_dt_interrupt(void)
{
    Duro_interrupted = 1;
}

/**
 * Read statements from file infilename and execute them.
 * If infilename is NULL, read from standard input.
 * If the input is a terminal, a function for reading lines must be
 * provided using RDB_parse_set_readline_fn().
 */
int
Duro_dt_execute(RDB_environment *dbenvp, const char *infilename,
        RDB_exec_context *ecp)
{
    FILE *infile = NULL;
    Duro_envp = dbenvp;
    Duro_interrupted = 0;

    if (infilename != NULL) {
        infile = fopen(infilename, "r");
        if (infile == NULL) {
            RDB_raise_resource_not_found(infilename, ecp);
            return RDB_ERROR;
        }
    }

    /* Initialize error line */
    err_line = -1;

    if (isatty(fileno(infile != NULL ? infile : stdin))) {
        RDB_parse_set_interactive(RDB_TRUE);

        /* Prompt is only needed in interactive mode */
        RDB_init_obj(&prompt);

        printf("Duro D/T library version %s\n", RDB_release_number);

        puts("Implicit transactions enabled.");
        RDB_bool_to_obj(&implicit_tx_obj, RDB_TRUE);
    } else {
        RDB_parse_set_interactive(RDB_FALSE);
    }

    RDB_parse_init_buf(infile != NULL ? infile : stdin);

    for(;;) {
        if (Duro_process_stmt(ecp) != RDB_OK) {
            RDB_object *errobjp = RDB_get_err(ecp);
            if (errobjp != NULL) {
                if (RDB_parse_get_interactive()) {
                    Duro_print_error(errobjp);
                    RDB_parse_init_buf(NULL);
                } else {
                    fprintf(stderr, "error in statement at or near line %d: ", err_line);
                    goto error;
                }
                RDB_clear_err(ecp);
            } else {
                /* Exit on EOF  */
                puts("");
                if (infile != NULL) {
                    fclose(infile);
                    infile = NULL;
                }
                RDB_parse_destroy_buf();
                return RDB_OK;
            }
        }
    }

error:
    if (infile != NULL) {
        fclose(infile);
    }
    RDB_parse_destroy_buf();
    return RDB_ERROR;
}

const char*
Duro_dt_prompt(void)
{
    return RDB_obj_string(&prompt);
}
