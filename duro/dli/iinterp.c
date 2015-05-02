/*
 * Statement interpretation functions.
 *
 * Copyright (C) 2007, 2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Statement execution functions.
 */

#include "iinterp.h"
#include "interp_core.h"
#include "interp_assign.h"
#include "interp_eval.h"
#include "ioop.h"
#include "exparse.h"
#include "parse.h"
#include <gen/hashmap.h>
#include <gen/hashmapit.h>
#include <gen/releaseno.h>
#include <gen/strfns.h>
#include <rel/rdb.h>
#include <rel/tostr.h>
#include <rel/typeimpl.h>
#include <rel/qresult.h>
#include <rel/optimize.h>

#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

extern int yylineno;

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *txt);
void yy_delete_buffer(YY_BUFFER_STATE);

/** @page update-ops Built-in system and connection operators

@section system-ops System operators

OPERATOR exit() UPDATES {};

Exits the process with status code 0.

OPERATOR exit(status int) UPDATES {};

Exits the process with status code \a status.

OPERATOR system(command string) UPDATES {};

Executes command \a command.

@section connection-ops Connection operators

OPERATOR connect(envname string) UPDATES {};

Connects to the database environment \a envname.
If connecting fails, try to connect with the Berkeley DB DB_RECOVER flag.

OPERATOR connect(envname string, recover boolean) UPDATES {};

Connects to the database environment \a envname.
If \a recover is true, connect with the Berkeley DB DB_RECOVER flag.

OPERATOR disconnect() UPDATES {};

Closes the database connection and sets current_db to the empty string.

OPERATOR create_db(dbname string) UPDATES {};

Create a database named \a dbname.

*/

/*
 * Operator exit() without arguments
 */
static int
exit_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");
    Duro_destroy_interp(interp);
    exit(0);
}   

/*
 * Operator exit() with argument
 */
static int
exit_int_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");
    int exitcode = RDB_obj_int(argv[0]);
    Duro_destroy_interp(interp);
    exit(exitcode);
}   

static int
system_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret = system(RDB_obj_string(argv[0]));
    if (ret == -1 || ret == 127) {
        RDB_handle_errcode(errno, ecp, txp);
        return RDB_ERROR;
    }
    RDB_int_to_obj(argv[1], (RDB_int) ret);
    return RDB_OK;
}

static int
connect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    /* If a connection exists, close it */
    if (interp->envp != NULL) {
        RDB_close_env(interp->envp);
    }

    /*
     * Try opening the environment without RDB_RECOVER first
     * to attach to existing memory pool
     */

    ret = RDB_open_env(RDB_obj_string(argv[0]), &interp->envp, 0);
    if (ret != RDB_OK) {
        /*
         * Retry with RDB_RECOVER option, re-creating necessary files
         * and running recovery
         */
        ret = RDB_open_env(RDB_obj_string(argv[0]), &interp->envp,
                RDB_RECOVER);
        if (ret != RDB_OK) {
            RDB_handle_errcode(ret, ecp, txp);
            interp->envp = NULL;
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
connect_recover_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->envp != NULL) {
        RDB_close_env(interp->envp);
    }

    ret = RDB_open_env(RDB_obj_string(argv[0]), &interp->envp,
            RDB_obj_bool(argv[1]) ? RDB_RECOVER : 0);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        interp->envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
disconnect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object *dbnameobjp;
    int ret;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->envp == NULL) {
        RDB_raise_resource_not_found("no database environment", ecp);
        return RDB_ERROR;
    }

    if (interp->current_foreachp != NULL) {
        RDB_raise_in_use("disconnect() not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    /* If a transaction is active, abort it */
    if (interp->txnp != NULL) {
        ret = RDB_rollback(ecp, &interp->txnp->tx);
        interp->txnp = NULL;
        if (ret != RDB_OK)
            return ret;

        if (RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }

    /* Close DB environment */
    ret = RDB_close_env(interp->envp);
    interp->envp = NULL;
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        interp->envp = NULL;
        return RDB_ERROR;
    }

    /* If CURRENT_DB was set, set it to empty string */
    dbnameobjp = RDB_hashmap_get(&interp->sys_varmap, "current_db");
    if (dbnameobjp == NULL || *RDB_obj_string(dbnameobjp) == '\0') {
        return RDB_OK;
    }
    return RDB_string_to_obj(dbnameobjp, "", ecp);
}

static int
create_env_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->envp != NULL) {
        RDB_close_env(interp->envp);
    }

    /* Create directory if does not exist */
#ifdef _WIN32
    ret = _mkdir(RDB_obj_string(argv[0]));
#else /* POSIX */
    ret = mkdir(RDB_obj_string(argv[0]),
            S_IRUSR | S_IWUSR | S_IXUSR
            | S_IRGRP | S_IWGRP | S_IXGRP);
#endif
    if (ret == -1 && errno != EEXIST) {
        RDB_errno_to_error(errno, ecp);
        return RDB_ERROR;
    }

    ret = RDB_create_env(RDB_obj_string(argv[0]), &interp->envp);
    if (ret != RDB_OK) {
        RDB_handle_errcode(ret, ecp, txp);
        interp->envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
create_db_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->envp == NULL) {
        RDB_raise_resource_not_found("no environment", ecp);
        return RDB_ERROR;
    }

    if (RDB_create_db_from_env(RDB_obj_string(argv[0]), interp->envp, ecp) == NULL)
        return RDB_ERROR;
    return RDB_OK;
}

static int
trace_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");
    int l = RDB_obj_int(argv[0]);
    if (interp->envp == NULL) {
        RDB_raise_resource_not_found("Missing database environment", ecp);
        return RDB_ERROR;
    }
    if (l < 0) {
        RDB_raise_invalid_argument("Invalid trace level", ecp);
        return RDB_ERROR;
    }
    RDB_env_set_trace(interp->envp, (unsigned) l);
    return RDB_OK;
}

/* Add I/O operators and variables */
static int
add_io(Duro_interp *interp, RDB_exec_context *ecp) {
    if (RDB_add_io_ops(&interp->sys_upd_op_map, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    if (RDB_hashmap_put(&interp->sys_varmap, "stdin", &DURO_STDIN_OBJ) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    if (RDB_hashmap_put(&interp->sys_varmap, "stdout", &DURO_STDOUT_OBJ) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    if (RDB_hashmap_put(&interp->sys_varmap, "stderr", &DURO_STDERR_OBJ) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
evaluate_retry_bool(RDB_expression *exp, Duro_interp *interp,
        RDB_exec_context *ecp, RDB_bool *resultp)
{
    int ret;
    RDB_object result;

    RDB_init_obj(&result);
    ret = Duro_evaluate_retry(exp, interp, ecp, &result);
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
exec_call(const RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int i;
    RDB_expression *exp;
    RDB_type *argtv[DURO_MAX_LLEN];
    RDB_object *argpv[DURO_MAX_LLEN];
    RDB_object argv[DURO_MAX_LLEN];
    RDB_operator *op;
    RDB_parse_node *argp = nodep->nextp->nextp->val.children.firstp;
    const char *opname;
    RDB_object qop_nameobj;
    int argc = 0;

    RDB_init_obj(&qop_nameobj);

    if (nodep->kind == RDB_NODE_INNER) {
        if (RDB_parse_node_modname(nodep->val.children.firstp, &qop_nameobj, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&qop_nameobj, ".", ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(&qop_nameobj,
                RDB_expr_var_name(nodep->val.children.firstp->nextp->nextp->exp),
                ecp) != RDB_OK)
            goto error;
        opname = RDB_obj_string(&qop_nameobj);
    } else {
        opname = RDB_expr_var_name(nodep->exp);
    }

    /*
     * Get argument types
     */
    while (argp != NULL) {
        if (argc > 0)
            argp = argp->nextp;
        exp = RDB_parse_node_expr(argp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (exp == NULL)
            goto error;
        argtv[argc] = Duro_expr_type_retry(exp, interp, ecp);
        if (argtv[argc] == NULL)
            goto error;

        argp = argp->nextp;
        argpv[argc] = NULL;
        argc++;
    }

    /*
     * Get operator
     */
    op = RDB_get_op(&interp->sys_upd_op_map, opname, argc, argtv, ecp);
    if (op == NULL) {
        RDB_bool type_mismatch =
                (RDB_obj_type(RDB_get_err(ecp)) == &RDB_TYPE_MISMATCH_ERROR);

        /*
         * If there is transaction and no environment, RDB_get_update_op_e()
         * will fail
         */
        if (interp->txnp == NULL && interp->envp == NULL) {
            goto error;
        }
        RDB_clear_err(ecp);

        op = RDB_get_update_op_e(opname, argc, argtv, interp->envp, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        /*
         * If the operator was not found and no transaction is running start a transaction
         * for reading the operator from the catalog only
         */
        if (op == NULL && RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR
                && interp->txnp == NULL) {
            RDB_transaction tx;
            RDB_database *dbp = Duro_get_db(interp, ecp);
            if (dbp == NULL)
                goto error;
            if (RDB_begin_tx(ecp, &tx, dbp, NULL) != RDB_OK)
                goto error;
            op = RDB_get_update_op(opname, argc, argtv, ecp, &tx);
            if (RDB_commit(ecp, &tx) != RDB_OK)
                goto error;
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
            goto error;
        }
    }

    argp = nodep->nextp->nextp->val.children.firstp;
    i = 0;
    while (argp != NULL) {
        RDB_parameter *paramp;
        const char *varname;

        if (i > 0)
            argp = argp->nextp;
        exp = RDB_parse_node_expr(argp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (exp == NULL)
            goto error;
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
            argpv[i] = Duro_lookup_var(varname, interp, ecp);
            if (argpv[i] == NULL) {
                goto error;
            }
        } else {
            if (RDB_expr_is_table_ref(exp)) {
                /* Special handling of table refs */
                argpv[i] = RDB_expr_obj(exp);
            } else if (paramp != NULL && paramp->update) {
                /*
                 * If it's an update argument and the argument is not a variable,
                 * raise an error
                 */
                RDB_raise_invalid_argument(
                        "update argument must be a variable", ecp);
                goto error;
            }
        }

        /* If the expression has not been resolved as a variable, evaluate it */
        if (argpv[i] == NULL) {
            RDB_init_obj(&argv[i]);
            if (Duro_evaluate_retry(exp, interp, ecp, &argv[i]) != RDB_OK) {
                goto error;
            }
            /* Set type if missing */
            if (RDB_obj_type(&argv[i]) == NULL) {
                RDB_type *typ = RDB_expr_type(exp, &Duro_get_var_type,
                        NULL, interp->envp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
                if (typ == NULL) {
                    goto error;
                }
                RDB_obj_set_typeinfo(&argv[i], typ);
            }
            argpv[i] = &argv[i];
        }
        argp = argp->nextp;
        i++;
    }

    /* Invoke function */
    if (RDB_call_update_op(op, argc, argpv, ecp, interp->txnp != NULL ?
            &interp->txnp->tx : NULL) != RDB_OK)
        goto error;

    for (i = 0; i < argc; i++) {
        if (argpv[i] == &argv[i])
            RDB_destroy_obj(&argv[i], ecp);
    }
    RDB_destroy_obj(&qop_nameobj, ecp);
    return RDB_OK;

error:
    for (i = 0; i < argc; i++) {
        if (argpv[i] == &argv[i])
            RDB_destroy_obj(&argv[i], ecp);
    }
    RDB_destroy_obj(&qop_nameobj, ecp);
    return RDB_ERROR;
}

static int
nodes_to_seqitv(RDB_seq_item *seqitv, RDB_parse_node *nodep,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    int i = 0;

    if (nodep != NULL) {
        for (;;) {
            /* Get attribute name */
            exp = RDB_parse_node_expr(nodep->val.children.firstp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : NULL);
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
exec_load(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
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

    tbexp = RDB_parse_node_expr(nodep, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (tbexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }
    dstp = Duro_lookup_var(RDB_expr_var_name(tbexp), interp, ecp);
    if (dstp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    tbexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
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
        srctbp = Duro_lookup_var(srcvarname, interp, ecp);
        if (srctbp == NULL) {
            ret = RDB_ERROR;
            goto cleanup;
        }
    } else {
        if (Duro_evaluate_retry(tbexp, interp, ecp, &srctb) != RDB_OK) {
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
    ret = nodes_to_seqitv(seqitv, seqitnodep->val.children.firstp, interp, ecp);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    ret = RDB_table_to_array(dstp, srctbp, seqitc, seqitv, 0, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);

cleanup:
    if (seqitv != NULL)
        RDB_free(seqitv);
    RDB_destroy_obj(&srctb, ecp);
    return ret;
}

static int
exec_explain(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_expression *exp;
    RDB_object strobj;
    int seqitc;
    RDB_expression *resexp = NULL;
    RDB_seq_item *seqitv = NULL;
    RDB_expression *optexp = NULL;
    RDB_transaction *txp = interp->txnp != NULL ? &interp->txnp->tx : NULL;

    RDB_init_obj(&strobj);

    exp = RDB_parse_node_expr(nodep, ecp, txp);
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

    /* Resolve local variables */
    resexp = RDB_expr_resolve_varnames(exp, &Duro_get_var, interp, ecp, txp);
    if (resexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    ret = nodes_to_seqitv(seqitv,
            nodep->nextp->nextp->nextp->val.children.firstp, interp, ecp);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    /* Optimize */
    optexp = RDB_optimize_expr(resexp, seqitc, seqitv, NULL, ecp, txp);
    if (optexp == NULL) {
        ret = RDB_ERROR;
        goto cleanup;
    }

    /* Convert tree to string */
    ret = RDB_expr_to_str(&strobj, optexp, ecp, txp, RDB_SHOW_INDEX);
    if (ret != RDB_OK) {
        goto cleanup;
    }

    ret = puts(RDB_obj_string(&strobj));
    if (ret == EOF) {
        RDB_handle_errcode(errno, ecp, txp);
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
    if (resexp != NULL) {
        RDB_del_expr(resexp, ecp);
    }
    RDB_destroy_obj(&strobj, ecp);
    return ret;
}

static int
Duro_exec_stmt(RDB_parse_node *, Duro_interp *interp, RDB_exec_context *,
        Duro_return_info *);

static int
exec_stmts(RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    int ret;
    while (stmtp != NULL) {
        ret = Duro_exec_stmt(stmtp, interp, ecp, retinfop);
        if (ret != RDB_OK)
            return ret;
        stmtp = stmtp->nextp;
    }
    return RDB_OK;
}

static int
exec_if(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    RDB_bool b;
    int ret;
    RDB_expression *condp = RDB_parse_node_expr(nodep, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    if (evaluate_retry_bool(condp, interp, ecp, &b)
            != RDB_OK) {
        return RDB_ERROR;
    }
    if (Duro_add_varmap(interp, ecp) != RDB_OK)
        return RDB_ERROR;
    if (b) {
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
    } else if (nodep->nextp->nextp->nextp->val.token == TOK_ELSE) {
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
    } else {
        ret = RDB_OK;
    }

    Duro_remove_varmap(interp);
    return ret;
}

static int
exec_case(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
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
                interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (condp == NULL)
            return RDB_ERROR;

        if (evaluate_retry_bool(condp, interp, ecp, &b)
                != RDB_OK) {
            return RDB_ERROR;
        }
        if (b) {
            if (Duro_add_varmap(interp, ecp) != RDB_OK)
                return RDB_ERROR;
            ret = exec_stmts(whenp->val.children.firstp->nextp->nextp->nextp->val.children.firstp,
                    interp, ecp, retinfop);
            Duro_remove_varmap(interp);
            return ret;
        }
        whenp = whenp->nextp;
    }
    if (nodep->nextp->nextp->kind == RDB_NODE_INNER) {
        /* ELSE branch */
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
        Duro_remove_varmap(interp);
        return ret;
    }
    return RDB_OK;
}

static int
exec_while(RDB_parse_node *nodep, RDB_parse_node *labelp, Duro_interp *interp,
        RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_bool b;
    RDB_expression *condp = RDB_parse_node_expr(nodep, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (condp == NULL)
        return RDB_ERROR;

    for(;;) {
        if (evaluate_retry_bool(condp, interp, ecp, &b) != RDB_OK)
            return RDB_ERROR;
        if (!b)
            return RDB_OK;
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            return RDB_ERROR;
        ret = exec_stmts(nodep->nextp->nextp->val.children.firstp, interp, ecp, retinfop);
        if (ret != RDB_OK) {
            if (ret == DURO_LEAVE) {
                /*
                 * If the statement name matches the LEAVE target,
                 * exit loop with RDB_OK
                 */
                if (labelp != NULL
                        && strcmp(RDB_expr_var_name(labelp->exp),
                                interp->leave_targetname) == 0) {
                    /* Target name matches label */
                    ret = RDB_OK;
                } else if (interp->leave_targetname == NULL) {
                    /* No label and target is NULL */
                    ret = RDB_OK;
                }
            }
            Duro_remove_varmap(interp);
            break;
        }
        Duro_remove_varmap(interp);

        /*
         * Check interrupt flag - this allows the user to break out of loops
         * with Control-C
         */
        if (interp->interrupted) {
            interp->interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            return RDB_ERROR;
        }
    }
    return ret;
}

static int
exec_for(const RDB_parse_node *nodep, const RDB_parse_node *labelp,
        Duro_interp *interp, RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_object endval;
    RDB_expression *fromexp, *toexp;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *varp = Duro_lookup_transient_var(interp, varname);
    if (varp == NULL) {
        RDB_raise_name(RDB_expr_var_name(nodep->exp), ecp);
        return RDB_ERROR;
    }

    if (RDB_obj_type(varp) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("loop variable must be of type integer", ecp);
        return RDB_ERROR;
    }

    fromexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (fromexp == NULL)
        return RDB_ERROR;
    if (Duro_evaluate_retry(fromexp, interp, ecp, varp) != RDB_OK) {
        return RDB_ERROR;
    }
    toexp = RDB_parse_node_expr(nodep->nextp->nextp->nextp->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (toexp == NULL)
        return RDB_ERROR;
    RDB_init_obj(&endval);
    if (Duro_evaluate_retry(toexp, interp, ecp, &endval) != RDB_OK) {
        goto error;
    }
    if (RDB_obj_type(&endval) != &RDB_INTEGER) {
        RDB_raise_type_mismatch("expression must be of type integer", ecp);
        goto error;
    }

    while (RDB_obj_int(varp) <= RDB_obj_int(&endval)) {
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            goto error;

        /* Execute statements */
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
        if (ret != RDB_OK) {
            if (ret == DURO_LEAVE) {
                /*
                 * If the statement name matches the LEAVE target,
                 * exit loop with RDB_OK
                 */
                if (labelp != NULL
                        && strcmp(RDB_expr_var_name(labelp->exp),
                                interp->leave_targetname) == 0) {
                    ret = RDB_OK;
                } else if (interp->leave_targetname == NULL) {
                    /* No label and target is NULL */
                    ret = RDB_OK;
                }
            }
            Duro_remove_varmap(interp);
            RDB_destroy_obj(&endval, ecp);
            return ret;
        }
        Duro_remove_varmap(interp);

        /* Check if the variable has been dropped */
        varp = Duro_lookup_transient_var(interp, varname);
        if (varp == NULL) {
            RDB_raise_name(varname, ecp);
            goto error;
        }

        /*
         * Check for user interrupt
         */
        if (interp->interrupted) {
            interp->interrupted = 0;
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
        Duro_interp *interp, RDB_exec_context *ecp, Duro_return_info *retinfop)
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
    RDB_transaction *txp = interp->txnp != NULL ? &interp->txnp->tx : NULL;
    const char *varname = RDB_expr_var_name(nodep->exp);
    RDB_object *varp = Duro_lookup_transient_var(interp, varname);
    if (varp == NULL) {
        RDB_raise_name(varname, ecp);
        return RDB_ERROR;
    }

    it.qrp = NULL;

    tbexp = RDB_parse_node_expr(nodep->nextp->nextp, ecp, txp);
    if (tbexp == NULL)
        return RDB_ERROR;

    tbtyp = Duro_expr_type_retry(tbexp, interp, ecp);
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
        it.tbp = Duro_lookup_var(srcvarname, interp, ecp);
        if (it.tbp == NULL)
            goto error;
    } else {
        if (Duro_evaluate_retry(tbexp, interp, ecp, &tb) != RDB_OK) {
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
    if (nodes_to_seqitv(seqitv, seqitnodep->val.children.firstp, interp, ecp)
                    != RDB_OK) {
        goto error;
    }
    it.qrp = RDB_table_iterator(it.tbp, seqitc, seqitv, ecp, txp);
    if (it.qrp == NULL)
        goto error;

    it.prevp = interp->current_foreachp;
    interp->current_foreachp = &it;

    while (RDB_next_tuple(it.qrp, varp, ecp, txp) == RDB_OK) {
        if (Duro_add_varmap(interp, ecp) != RDB_OK)
            goto error;

        /* Execute statements */
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->nextp
                ->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, retinfop);
        Duro_remove_varmap(interp);
        if (ret == DURO_LEAVE) {
            /*
             * If the statement name matches the LEAVE target,
             * exit loop with RDB_OK
             */
            if (labelp != NULL
                    && strcmp(RDB_expr_var_name(labelp->exp),
                            interp->leave_targetname) == 0) {
                ret = RDB_OK;
            } else if (interp->leave_targetname == NULL) {
                /* No label and target is NULL, exit loop */
                ret = RDB_OK;
            }
            break;
        }
        if (ret != RDB_OK)
            goto error;

        /* Check if the variable has been dropped */
        varp = Duro_lookup_transient_var(interp, varname);
        if (varp == NULL) {
            RDB_raise_name(varname, ecp);
            goto error;
        }

        /*
         * Check for user interrupt
         */
        if (interp->interrupted) {
            interp->interrupted = 0;
            RDB_raise_system("interrupted", ecp);
            goto error;
        }
    }

    /* Set to previous FOREACH iterator (or NULL) */
    interp->current_foreachp = it.prevp;

    RDB_free(seqitv);
    if (RDB_del_table_iterator(it.qrp, ecp, txp) != RDB_OK) {
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
        RDB_del_table_iterator(it.qrp, ecp, txp);
        interp->current_foreachp = it.prevp;
    }
    RDB_destroy_obj(&tb, ecp);
    RDB_free(seqitv);
    return RDB_ERROR;
}

static int
do_begin_tx(Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_database *dbp = Duro_get_db(interp, ecp);
    if (dbp == NULL)
        return RDB_ERROR;

    if (interp->txnp != NULL) {
        /* Start subtransaction */
        tx_node *ntxnp = RDB_alloc(sizeof(tx_node), ecp);
        if (ntxnp == NULL) {
            return RDB_ERROR;
        }

        if (RDB_begin_tx(ecp, &ntxnp->tx, dbp, &interp->txnp->tx) != RDB_OK) {
            RDB_free(ntxnp);
            return RDB_ERROR;
        }
        ntxnp->parentp = interp->txnp;
        interp->txnp = ntxnp;

        return RDB_OK;
    }

    interp->txnp = RDB_alloc(sizeof(tx_node), ecp);
    if (interp->txnp == NULL) {
        return RDB_ERROR;
    }

    if (RDB_begin_tx(ecp, &interp->txnp->tx, dbp, NULL) != RDB_OK) {
        RDB_free(interp->txnp);
        interp->txnp = NULL;
        return RDB_ERROR;
    }
    interp->txnp->parentp = NULL;

    return RDB_OK;
}

static int
exec_begin_tx(Duro_interp *interp, RDB_exec_context *ecp)
{
    int subtx = (interp->txnp != NULL);

    if (do_begin_tx(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf(subtx ? "Subtransaction started.\n"
                : "Transaction started.\n");

    return RDB_OK;
}

static int
do_commit(Duro_interp *interp, RDB_exec_context *ecp)
{
    tx_node *ptxnp;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (interp->current_foreachp != NULL) {
        RDB_raise_in_use("COMMIT not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    if (RDB_commit(ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    ptxnp = interp->txnp->parentp;
    RDB_free(interp->txnp);
    interp->txnp = ptxnp;

    return RDB_OK;
}

static int
exec_commit(Duro_interp *interp, RDB_exec_context *ecp)
{
    if (do_commit(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    if (RDB_parse_get_interactive())
        printf("Transaction committed.\n");

    return RDB_OK;
}

static int
do_rollback(Duro_interp *interp, RDB_exec_context *ecp)
{
    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (interp->current_foreachp != NULL) {
        RDB_raise_in_use("ROLLBACK not allowed in FOREACH", ecp);
        return RDB_ERROR;
    }

    if (RDB_rollback(ecp, &interp->txnp->tx) != RDB_OK)
        return RDB_ERROR;

    RDB_free(interp->txnp);
    interp->txnp = NULL;

    return RDB_OK;
}

static int
exec_rollback(Duro_interp *interp, RDB_exec_context *ecp)
{
    if (do_rollback(interp, ecp) != RDB_OK)
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
        if (RDB_type_is_generic(rep->compv[i].typ)) {
            RDB_raise_syntax("generic type not permitted", ecp);
            // !! delete rep->compv
            return RDB_ERROR;
        }
        np = np->nextp;
        if (np != NULL) /* Skip comma */
        	np = np->nextp;
    }
    return RDB_OK;
}

static int
exec_typedef(const RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp)
{
    int i, j;
    int flags;
    int repc;
    RDB_possrep *repv;
    RDB_parse_node *nodep, *prnodep;
    RDB_expression *initexp;
    RDB_expression *constraintp = NULL;
    RDB_object nameobj;
    const char *namp;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&nameobj);

    if (stmtp->nextp->kind == RDB_NODE_TOK) {
        /* Token must be ORDERED */
        flags = RDB_TYPE_ORDERED;
        prnodep = stmtp->nextp->nextp;
    } else {
        flags = 0;
        prnodep = stmtp->nextp;
    }

    repc = RDB_parse_nodelist_length(prnodep);
    repv = RDB_alloc(repc * sizeof(RDB_possrep), ecp);
    if (repv == NULL)
        goto error;

    nodep = prnodep->val.children.firstp;
    for (i = 0; i < repc; i++) {
        if (parserep_to_rep(nodep, &repv[i], ecp, &interp->txnp->tx) != RDB_OK)
            goto error;
        nodep = nodep->nextp;
    }

    if (prnodep->nextp->val.token == TOK_CONSTRAINT) {
        constraintp = RDB_parse_node_expr(prnodep->nextp->nextp, ecp,
                &interp->txnp->tx);
        if (constraintp == NULL)
        	goto error;
        initexp = RDB_parse_node_expr(prnodep->nextp->nextp->nextp->nextp, ecp,
                &interp->txnp->tx);
        if (initexp == NULL)
            goto error;
    } else {
        initexp = RDB_parse_node_expr(prnodep->nextp->nextp, ecp,
                &interp->txnp->tx);
        if (initexp == NULL)
            goto error;
    }

    /* If we're within MODULE, prepend module name */
    if (*RDB_obj_string(&interp->module_name) != '\0') {
        if (Duro_module_q_id(&nameobj, RDB_expr_var_name(stmtp->exp), interp, ecp) != RDB_OK)
            goto error;
        namp = RDB_obj_string(&nameobj);
    } else {
        namp = RDB_expr_var_name(stmtp->exp);
    }

    if (RDB_define_type(namp, repc, repv, constraintp, initexp, flags, ecp,
            &interp->txnp->tx) != RDB_OK)
        goto error;

    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (!RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_del_nonscalar_type(repv[i].compv[j].typ, ecp);
        }
    }
    RDB_free(repv);
    if (RDB_parse_get_interactive())
        printf("Type %s defined.\n", RDB_expr_var_name(stmtp->exp));
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_OK;

error:
    for (i = 0; i < repc; i++) {
        for (j = 0; j < repv[i].compc; j++) {
            if (repv[i].compv[j].typ != NULL
                    && !RDB_type_is_scalar(repv[i].compv[j].typ))
                RDB_del_nonscalar_type(repv[i].compv[j].typ, ecp);
        }
    }
    RDB_free(repv);
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

static int
exec_typedrop(const RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    RDB_type *typ;
    RDB_object nameobj;
    const char *namp;

    /*
     * DROP TYPE is not allowed in user-defined operators,
     * to prevent a type used by an operator from being dropped
     * while the operator is being executed
     */
    if (interp->inner_op != NULL) {
        RDB_raise_syntax("DROP TYPE not permitted in user-defined operators",
                ecp);
        return RDB_ERROR;
    }

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&nameobj);

    /* If we're within MODULE, prepend module name */
    if (*RDB_obj_string(&interp->module_name) != '\0') {
        if (Duro_module_q_id(&nameobj, RDB_expr_var_name(nodep->exp), interp, ecp) != RDB_OK)
            goto error;
        namp = RDB_obj_string(&nameobj);
    } else {
        namp = RDB_expr_var_name(nodep->exp);
    }

    typ = RDB_get_type(namp, ecp, &interp->txnp->tx);
    if (typ == NULL)
        goto error;

    /*
     * Check if a transient variable of that type exists
     */
    if (Duro_type_in_use(interp, typ)) {
        RDB_raise_in_use("unable to drop type because a variable depends on it", ecp);
        goto error;
    }

    if (RDB_drop_typeimpl_ops(typ, ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    if (RDB_drop_type(RDB_type_name(typ), ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    if (RDB_parse_get_interactive())
        printf("Type %s dropped.\n", RDB_expr_var_name(nodep->exp));
    return RDB_destroy_obj(&nameobj, ecp);

error:
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

static int
exec_typeimpl(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_type *ityp = NULL;
    RDB_object nameobj;
    const char *namp;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    if (nodep->nextp->val.token == TOK_AS) {
        Duro_return_info retinfo;

        ityp = Duro_parse_node_to_type_retry(nodep->nextp->nextp, interp, ecp);
        if (ityp == NULL)
            return RDB_ERROR;
        interp->impl_typename = RDB_expr_var_name(nodep->exp);
        ret = exec_stmts(nodep->nextp->nextp->nextp->nextp->val.children.firstp,
                interp, ecp, &retinfo);
        interp->impl_typename = NULL;
        if (ret != RDB_OK)
            return ret;
    }

    RDB_init_obj(&nameobj);

    if (*RDB_obj_string(&interp->module_name) != '\0') {
        if (Duro_module_q_id(&nameobj, RDB_expr_var_name(nodep->exp), interp, ecp) != RDB_OK)
            goto error;
        namp = RDB_obj_string(&nameobj);
    } else {
        namp = RDB_expr_var_name(nodep->exp);
    }

    ret = RDB_implement_type(namp, ityp, RDB_SYS_REP, ecp, &interp->txnp->tx);
    if (ret != RDB_OK) {
        RDB_exec_context ec;
        RDB_type *typ;

        RDB_init_exec_context(&ec);
        typ = RDB_get_type(RDB_expr_var_name(nodep->exp), &ec, &interp->txnp->tx);
        if (typ != NULL) {
            RDB_drop_typeimpl_ops(typ, &ec, &interp->txnp->tx);
        }
        RDB_destroy_exec_context(&ec);
        goto error;
    }
    if (RDB_parse_get_interactive())
        printf("Type %s implemented.\n", RDB_expr_var_name(nodep->exp));
    return RDB_destroy_obj(&nameobj, ecp);

error:
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

static void
free_opdata(RDB_operator *op)
{
    RDB_exec_context ec;
    Duro_op_data *opdatap = RDB_operator_u_data(op);

    /* Initialize temporary execution context */
    RDB_init_exec_context(&ec);

    /* Delete code */
    RDB_parse_del_node(opdatap->rootp, &ec);

    RDB_destroy_exec_context(&ec);

    RDB_free(opdatap->argnamev);
    RDB_free(opdatap);
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
    Duro_op_data *opdatap;
    Duro_return_info retinfo;
    varmap_node *ovarmapp;
    int isselector;
    RDB_type *getter_utyp = NULL;
    RDB_operator *parent_op;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->interrupted) {
        interp->interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    /* Try to get cached statements */
    opdatap = RDB_operator_u_data(op);
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
                    interp->txnp != NULL ? &interp->txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(Duro_op_data), ecp);
        if (opdatap == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }
        opdatap->rootp = codestmtp;
        opdatap->stmtlistp = codestmtp->val.children.firstp->nextp->nextp->nextp->nextp
                ->nextp->nextp->nextp->nextp->val.children.firstp;
        opdatap->argnamec = argc;
        opdatap->argnamev = argnamev;

        RDB_set_operator_u_data(op, opdatap);
        RDB_set_op_cleanup_fn(op, &free_opdata);
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    ovarmapp = Duro_set_current_varmap(interp, &vars);

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

    parent_op = interp->inner_op;
    interp->inner_op = op;
    ret = exec_stmts(opdatap->stmtlistp, interp, ecp, &retinfo);
    interp->inner_op = parent_op;

    /* Set type of return value to the user-defined type */
    if (isselector) {
        RDB_obj_set_typeinfo(retinfo.objp, RDB_operator_type(op));
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

    Duro_set_current_varmap(interp, ovarmapp);
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
    if (ret == RDB_ERROR) {
        RDB_free(interp->err_opname);
        interp->err_opname = RDB_dup_str(RDB_operator_name(op));
        if (interp->err_opname == NULL) {
            RDB_raise_no_memory(ecp);
        }
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
    Duro_op_data *opdatap;
    RDB_operator *parent_op;
    RDB_type *setter_utyp = NULL;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->interrupted) {
        interp->interrupted = 0;
        RDB_raise_system("interrupted", ecp);
        return RDB_ERROR;
    }

    /* Try to get cached statements */
    opdatap = RDB_operator_u_data(op);
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
                    interp->txnp != NULL ? &interp->txnp->tx : NULL));
            attrnodep = attrnodep->nextp->nextp;
        }

        opdatap = RDB_alloc(sizeof(Duro_op_data), ecp);
        if (opdatap == NULL) {
            RDB_free(argnamev);
            return RDB_ERROR;
        }
        opdatap->rootp = codestmtp;
        opdatap->stmtlistp = codestmtp->val.children.firstp->nextp->nextp->nextp->nextp
                    ->nextp->nextp->nextp->nextp->nextp->nextp->val.children.firstp;
        opdatap->argnamec = argc;
        opdatap->argnamev = argnamev;

        RDB_set_operator_u_data(op, opdatap);
        RDB_set_op_cleanup_fn(op, &free_opdata);
    }

    RDB_init_hashmap(&vars.map, 256);
    vars.parentp = NULL;
    ovarmapp = Duro_set_current_varmap(interp, &vars);

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

    parent_op = interp->inner_op;
    interp->inner_op = op;
    ret = exec_stmts(opdatap->stmtlistp, interp, ecp, NULL);
    interp->inner_op = parent_op;

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

    Duro_set_current_varmap(interp, ovarmapp);
    Duro_destroy_varmap(&vars.map);

    /* Catch LEAVE */
    if (ret == DURO_LEAVE) {
        RDB_raise_syntax("unmatched LEAVE", ecp);
        ret = RDB_ERROR;
    }
    if (ret == RDB_ERROR) {
        RDB_free(interp->err_opname);
        interp->err_opname = RDB_dup_str(RDB_operator_name(op));
        if (interp->err_opname == NULL) {
            RDB_raise_no_memory(ecp);
        }
    }

    return ret == RDB_ERROR ? RDB_ERROR : RDB_OK;
}

/* Define read-only operator, prepend module name */
static int
create_ro_op(const char *name, int paramc, RDB_parameter paramv[], RDB_type *rtyp,
                 const char *libname, const char *symname,
                 const char *sourcep, Duro_interp *interp,
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object opnameobj;

    if (*RDB_obj_string(&interp->module_name) == '\0') {
        return RDB_create_ro_op(name, paramc, paramv, rtyp,
                libname, symname, sourcep, ecp, txp);
    }
    RDB_init_obj(&opnameobj);
    if (Duro_module_q_id(&opnameobj, name, interp, ecp) != RDB_OK)
        goto error;

    if (RDB_create_ro_op(RDB_obj_string(&opnameobj), paramc, paramv, rtyp,
            libname, symname, sourcep, ecp, txp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
ro_opdef_extern(const char *opname, RDB_type *rtyp,
        int paramc, RDB_parameter *paramv,
        const char *lang,
        const char *extname, Duro_interp *interp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    Duro_uop_info *creop_infop = Duro_dt_get_creop_info(interp, lang);
    if (creop_infop == NULL) {
        RDB_raise_not_supported("language not supported", ecp);
        return RDB_ERROR;
    }

    return create_ro_op(opname, paramc, paramv, rtyp,
            creop_infop->libname,
            creop_infop->ro_op_symname,
            extname, interp, ecp, txp);
}

/* Define  operator, prepend module name */
static int
create_update_op(const char *name, int paramc, RDB_parameter paramv[],
                 const char *libname, const char *symname,
                 const char *sourcep, Duro_interp *interp,
                 RDB_exec_context *ecp, RDB_transaction *txp)
{
    RDB_object opnameobj;

    if (*RDB_obj_string(&interp->module_name) == '\0') {
        return RDB_create_update_op(name, paramc, paramv,
                libname, symname, sourcep, ecp, txp);
    }
    RDB_init_obj(&opnameobj);
    if (RDB_string_to_obj(&opnameobj, RDB_obj_string(&interp->module_name), ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, ".", ecp) != RDB_OK)
        goto error;
    if (RDB_append_string(&opnameobj, name, ecp) != RDB_OK)
        goto error;

    if (RDB_create_update_op(RDB_obj_string(&opnameobj), paramc, paramv,
            libname, symname, sourcep, ecp, txp) != RDB_OK)
        goto error;
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
update_opdef_extern(const char *opname,
        int paramc, RDB_parameter *paramv,
        const char *lang,
        const char *extname, Duro_interp *interp, RDB_exec_context *ecp,
        RDB_transaction *txp)
{
    Duro_uop_info *creop_infop = Duro_dt_get_creop_info(interp, lang);
    if (creop_infop == NULL) {
        RDB_raise_not_supported("extern: language not supported", ecp);
        return RDB_ERROR;
    }

    return create_update_op(opname, paramc, paramv,
            creop_infop->libname,
            creop_infop->update_op_symname,
            extname, interp, ecp, txp);
}

static int
exec_opdef(RDB_parse_node *parentp, Duro_interp *interp, RDB_exec_context *ecp)
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
    RDB_object *lwsp;
    RDB_bool subtx = RDB_FALSE;
    RDB_parse_node *stmtp = parentp->val.children.firstp->nextp;
    RDB_parameter *paramv = NULL;
    int paramc = ((int) RDB_parse_nodelist_length(stmtp->nextp->nextp) + 1) / 3;

    RDB_init_obj(&code);
    RDB_init_obj(&opnameobj);

    opname = RDB_expr_var_name(stmtp->exp);

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp;

        if (interp->envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            goto error;
        }

        dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            goto error;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            goto error;
        }
        subtx = RDB_TRUE;
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
                interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        if (paramv[i].typ == NULL)
            goto error;
        attrnodep = attrnodep->nextp->nextp;
    }

    ro = (RDB_bool)
            (stmtp->nextp->nextp->nextp->nextp->val.token == TOK_RETURNS);

    /* Strip off leading whitespace and comments, restore it later */
    lwsp = parentp->val.children.firstp->whitecommp;
    parentp->val.children.firstp->whitecommp = NULL;

    /*
     * 'Un-parse' the defining code
     */
    if (Duro_parse_node_to_obj_string(&code, parentp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx) != RDB_OK) {
        parentp->val.children.firstp->whitecommp = lwsp;
        goto error;
    }
    parentp->val.children.firstp->whitecommp = lwsp;

    if (ro) {
        rtyp = RDB_parse_node_to_type(stmtp->nextp->nextp->nextp->nextp->nextp,
                &Duro_get_var_type, NULL, ecp,
                interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        if (rtyp == NULL)
            goto error;
        if (RDB_type_is_generic(rtyp)) {
            RDB_raise_syntax("generic type not permitted", ecp);
            if (!RDB_type_is_scalar(rtyp))
                RDB_del_nonscalar_type(rtyp, ecp);
            goto error;
        }

        if (interp->impl_typename != NULL) {
            /*
             * We're inside a IMPLEMENT TYPE; ... END IMPLEMENT block.
             * Only selector and getters allowed
             */
            if (strstr(opname, "get_") == opname) {
                /* Prepend operator name with <typename>_ */
                if (RDB_string_to_obj(&opnameobj, interp->impl_typename, ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, "_", ecp) != RDB_OK)
                    goto error;
                if (RDB_append_string(&opnameobj, opname, ecp) != RDB_OK)
                    goto error;
                opname = RDB_obj_string(&opnameobj);
            } else if (!RDB_type_is_scalar(rtyp)
                    || strcmp(RDB_type_name(rtyp), interp->impl_typename) != 0) {
                /* Not a selector */
                RDB_raise_syntax("invalid operator", ecp);
                goto error;
            }
        }

        /* Check for EXTERN ... */
        if (stmtp->nextp->nextp->nextp->nextp->nextp->nextp->val.token == TOK_EXTERN) {
            RDB_expression *langexp, *extnamexp;
            langexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (langexp == NULL)
                goto error;
            extnamexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (extnamexp == NULL)
                goto error;

            ret = ro_opdef_extern(opname, rtyp, paramc, paramv,
                    RDB_obj_string(RDB_expr_obj(langexp)),
                    RDB_obj_string(RDB_expr_obj(extnamexp)),
                    interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        } else {
            ret = create_ro_op(opname, paramc, paramv, rtyp,
#ifdef _WIN32
                    "duro",
#else
                    "libduro",
#endif
                    "Duro_dt_invoke_ro_op",
                    RDB_obj_string(&code), interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        }
        if (ret != RDB_OK)
            goto error;
    } else {
        if (interp->impl_typename != NULL) {
            /* Only setters allowed */
            if (strstr(opname, "set_") == opname) {
                /* Prepend operator name with <typename>_ */
                if (RDB_string_to_obj(&opnameobj, interp->impl_typename, ecp) != RDB_OK)
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

        /* Check for EXTERN ... */
        if (stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->val.token
                == TOK_EXTERN) {
            RDB_expression *langexp, *extnamexp;
            langexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (langexp == NULL)
                goto error;
            extnamexp = RDB_parse_node_expr(
                    stmtp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp->nextp,
                    ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
            if (extnamexp == NULL)
                goto error;

            ret = update_opdef_extern(opname, paramc, paramv,
                    RDB_obj_string(RDB_expr_obj(langexp)),
                    RDB_obj_string(RDB_expr_obj(extnamexp)),
                    interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        } else {
            ret = create_update_op(opname,
                    paramc, paramv,
#ifdef _WIN32
                    "duro",
#else
                    "libduro",
#endif
                    "Duro_dt_invoke_update_op",
                    RDB_obj_string(&code), interp, ecp,
                    interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
        }
        if (ret != RDB_OK)
            goto error;
    }

    for (i = 0; i < paramc; i++) {
        RDB_del_nonscalar_type(paramv[i].typ, ecp);
    }
    RDB_free(paramv);
    RDB_destroy_obj(&code, ecp);
    if (subtx) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf(ro ? "Read-only operator %s created.\n" : "Update operator %s created.\n", opname);
    RDB_destroy_obj(&opnameobj, ecp);
    return ret;

error:
    if (subtx)
        RDB_rollback(ecp, &tmp_tx);
    if (paramv != NULL) {
        for (i = 0; i < paramc; i++) {
            RDB_del_nonscalar_type(paramv[i].typ, ecp);
        }
        RDB_free(paramv);
    }
    RDB_destroy_obj(&code, ecp);
    RDB_destroy_obj(&opnameobj, ecp);
    return RDB_ERROR;
}

static int
exec_opdrop(const RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_object nameobj;
    const char *opname;
    RDB_transaction tmp_tx;

    /*
     * DROP OPERATOR is not allowed in user-defined operators,
     * to prevent an operator from being dropped while it is executed
     */
    if (interp->inner_op != NULL) {
        RDB_raise_syntax("DROP OPERATOR not permitted in user-defined operators",
                ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&nameobj);

    /* If we're within MODULE, prepend module name */
    if (*RDB_obj_string(&interp->module_name) != '\0') {
        if (Duro_module_q_id(&nameobj, RDB_expr_var_name(nodep->exp), interp, ecp) != RDB_OK)
            goto error;
        opname = RDB_obj_string(&nameobj);
    } else {
        opname = RDB_expr_var_name(nodep->exp);
    }

    /*
     * If a transaction is not active, start transaction if a database environment
     * is available
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp;

        if (interp->envp == NULL) {
            RDB_raise_resource_not_found("no connection", ecp);
            goto error;
        }

        dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            goto error;

        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            goto error;
        }
    }

    if (RDB_drop_op(opname, ecp,
            interp->txnp == NULL ? &tmp_tx : &interp->txnp->tx) != RDB_OK) {
        if (interp->txnp == NULL)
            RDB_rollback(ecp, &tmp_tx);
        goto error;
    }

    if (interp->txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Operator %s dropped.\n", opname);

    RDB_destroy_obj(&nameobj, ecp);
    return ret;

error:
    RDB_destroy_obj(&nameobj, ecp);
    return RDB_ERROR;
}

static int
exec_return(RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{    
    if (stmtp->kind != RDB_NODE_TOK) {
        RDB_expression *retexp;
        RDB_type *rtyp;

        if (retinfop == NULL) {
            RDB_raise_syntax("invalid RETURN", ecp);
            return RDB_ERROR;
        }

        retexp = RDB_parse_node_expr(stmtp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
        if (retexp == NULL)
            return RDB_ERROR;

        /*
         * Typecheck
         */
        rtyp = Duro_expr_type_retry(retexp, interp, ecp);
        if (rtyp == NULL)
            return RDB_ERROR;
        if (!RDB_type_equals(rtyp, retinfop->typ)) {
            RDB_raise_type_mismatch("invalid return type", ecp);
            return RDB_ERROR;
        }

        /*
         * Evaluate expression
         */
        if (Duro_evaluate_retry(retexp, interp, ecp, retinfop->objp) != RDB_OK)
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
exec_raise(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_object *errp = RDB_raise_err(ecp);
    assert(errp != NULL);

    exp = RDB_parse_node_expr(nodep, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (exp == NULL)
        return RDB_ERROR;

    Duro_evaluate_retry(exp, interp, ecp, errp);
    return RDB_ERROR;
}

static int
exec_catch(const RDB_parse_node *catchp, const RDB_type *errtyp,
        Duro_interp *interp, RDB_exec_context *ecp, Duro_return_info *retinfop)
{
    int ret;
    RDB_object *objp;

    if (Duro_add_varmap(interp, ecp) != RDB_OK)
        return RDB_ERROR;

    objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (objp == NULL) {
        goto error;
    }
    RDB_init_obj(objp);

    /*
     * Create and initialize local variable
     */
    if (Duro_put_var(RDB_expr_var_name(catchp->exp), objp, interp, ecp) != RDB_OK) {
        goto error;
    }
    if (RDB_copy_obj(objp, RDB_get_err(ecp), ecp) != RDB_OK) {
        goto error;
    }

    RDB_clear_err(ecp);
    interp->err_line = -1;

    ret = exec_stmts(catchp->nextp->nextp->kind == RDB_NODE_TOK ?
    		catchp->nextp->nextp->nextp->val.children.firstp :
			catchp->nextp->nextp->val.children.firstp,
			interp, ecp, retinfop);
    Duro_remove_varmap(interp);
    return ret;

error:
    Duro_remove_varmap(interp);
    return RDB_ERROR;    
}

static int
exec_try(const RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp, Duro_return_info *retinfop)
{
	int ret;
	RDB_parse_node *catchp;

    /*
     * Execute try body
     */
    if (Duro_add_varmap(interp, ecp) != RDB_OK)
        return RDB_ERROR;
    ret = exec_stmts(nodep->val.children.firstp, interp, ecp, retinfop);
    Duro_remove_varmap(interp);

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
                    		catchp->val.children.firstp->nextp->nextp,
                    		interp, ecp);
                    if (typ == NULL)
                    	return RDB_ERROR;
                    if (RDB_type_equals(errtyp, typ)) {
                        return exec_catch(catchp->val.children.firstp->nextp,
                        		typ, interp, ecp, retinfop);
                    }
                } else {
                    /* Catch clause without type */                    
                    return exec_catch(catchp->val.children.firstp->nextp,
                            errtyp, interp, ecp, retinfop);
                }
                catchp = catchp->nextp;
            }
        }
    }
    return ret;
}

/* Implements both MODULE .. END MODULE and IMPLEMENT MODULE .. END IMPLEMENT. */
static int
exec_moduledef(RDB_parse_node *stmtp, Duro_interp *interp, RDB_exec_context *ecp,
        Duro_return_info *retinfop)
{
    int ret;
    RDB_object oldmodname;
    size_t olen = strlen(RDB_obj_string(&interp->module_name));
    RDB_init_obj(&oldmodname);

    if (olen > 0) {
        RDB_append_string(&interp->module_name, ".", ecp);
    }
    RDB_append_string(&interp->module_name, RDB_expr_var_name(stmtp->exp), ecp);

    ret = exec_stmts(stmtp->nextp->nextp->val.children.firstp,
            interp, ecp, retinfop);

    if (RDB_string_n_to_obj(&oldmodname, RDB_obj_string(&interp->module_name),
            olen, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (RDB_copy_obj(&interp->module_name, &oldmodname, ecp) != RDB_OK) {
        return RDB_ERROR;
    }

    RDB_destroy_obj(&oldmodname, ecp);
    return ret;
}

static int
exec_constrdef(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    const char *constrname = RDB_expr_var_name(nodep->exp);
    RDB_expression *constrexp;

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    constrexp = RDB_parse_node_expr(nodep->nextp, ecp,
            interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (constrexp == NULL)
        goto error;
    constrexp = RDB_dup_expr(constrexp, ecp);
    if (constrexp == NULL)
        goto error;
    ret = RDB_create_constraint(constrname, constrexp, ecp,
    		interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (interp->txnp == NULL) {
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
    if (interp->txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_indexdef(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    int i;
    const char *indexname = RDB_expr_var_name(nodep->exp);
    RDB_object *tbp;
    int idxcompc = (RDB_parse_nodelist_length(nodep->nextp->nextp->nextp) + 1) / 2;
    RDB_seq_item *idxcompv;
    RDB_parse_node *attrnodep;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    tbp = RDB_get_table(RDB_expr_var_name(nodep->nextp->exp),
            ecp, &interp->txnp->tx);
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
            idxcompv, RDB_ORDERED, ecp, &interp->txnp->tx);
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Index %s created.\n", indexname);
    RDB_free(idxcompv);
    return ret;
}

static int
exec_constrdrop(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    RDB_transaction tmp_tx;
    const char *constrname = RDB_expr_var_name(nodep->exp);

    /*
     * Create temporary transaction, if no transaction is active
     */
    if (interp->txnp == NULL) {
        RDB_database *dbp = Duro_get_db(interp, ecp);
        if (dbp == NULL)
            return RDB_ERROR;
        if (RDB_begin_tx(ecp, &tmp_tx, dbp, NULL) != RDB_OK) {
            return RDB_ERROR;
        }
    }

    ret = RDB_drop_constraint(constrname, ecp, interp->txnp != NULL ? &interp->txnp->tx : &tmp_tx);
    if (ret != RDB_OK)
        goto error;

    if (interp->txnp == NULL) {
        ret = RDB_commit(ecp, &tmp_tx);
    } else {
        ret = RDB_OK;
    }
    if ((ret == RDB_OK) && RDB_parse_get_interactive())
        printf("Constraint %s dropped.\n", constrname);
    return ret;

error:
    if (interp->txnp == NULL)
        RDB_rollback(ecp, &tmp_tx);
    return RDB_ERROR;
}

static int
exec_indexdrop(RDB_parse_node *nodep, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    int ret;
    const char *indexname = RDB_expr_var_name(nodep->exp);

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    ret = RDB_drop_table_index(indexname, ecp, &interp->txnp->tx);
    if (ret != RDB_OK)
        goto error;

    if (RDB_parse_get_interactive())
        printf("Index %s dropped.\n", indexname);
    return ret;

error:
    return RDB_ERROR;
}

static int
exec_map(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_expression *exp;
    RDB_object idobj;
    const char *tbname;

    if (interp->txnp == NULL) {
        RDB_raise_no_running_tx(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&idobj);

    if (*RDB_obj_string(&interp->module_name) != '\0') {
        if (Duro_module_q_id(&idobj, RDB_expr_var_name(nodep->exp), interp, ecp)
                != RDB_OK) {
            goto error;
        }
        tbname = RDB_obj_string(&idobj);
    } else {
        tbname = RDB_expr_var_name(nodep->exp);
    }

    exp = RDB_parse_node_expr(nodep->nextp, ecp, interp->txnp != NULL ? &interp->txnp->tx : NULL);
    if (exp == NULL)
        goto error;

    if (RDB_map_public_table(tbname, exp, ecp, &interp->txnp->tx) != RDB_OK)
        goto error;

    RDB_destroy_obj(&idobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&idobj, ecp);
    return RDB_ERROR;
}

static int
exec_leave(RDB_parse_node *nodep, Duro_interp *interp, RDB_exec_context *ecp)
{
    if (nodep->kind == RDB_NODE_TOK) {
        interp->leave_targetname = NULL;
    } else {
        interp->leave_targetname = RDB_expr_var_name(nodep->exp);
    }
    return DURO_LEAVE;
}

static int
Duro_exec_stmt(RDB_parse_node *stmtp, Duro_interp *interp,
        RDB_exec_context *ecp, Duro_return_info *retinfop)
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
            ret = exec_if(firstchildp->nextp, interp, ecp, retinfop);
            break;
        case TOK_CASE:
            ret = exec_case(firstchildp->nextp, interp, ecp, retinfop);
            break;
        case TOK_FOR:
            if (firstchildp->nextp->nextp->val.token == TOK_IN) {
                ret = exec_foreach(firstchildp->nextp, NULL, interp, ecp, retinfop);
            } else {
                ret = exec_for(firstchildp->nextp, NULL, interp, ecp, retinfop);
            }
            break;
        case TOK_WHILE:
            ret = exec_while(firstchildp->nextp, NULL, interp, ecp, retinfop);
            break;
        case TOK_OPERATOR:
            ret = exec_opdef(stmtp, interp, ecp);
            break;
        case TOK_TRY:
            ret = exec_try(firstchildp->nextp, interp, ecp, retinfop);
            break;
        case TOK_LEAVE:
            ret = exec_leave(firstchildp->nextp, interp, ecp);
            break;
        case ';':
        /* Empty statement */
        ret = RDB_OK;
        break;
        case TOK_CALL:
            ret = exec_call(firstchildp->nextp, interp, ecp);
            break;
        case TOK_VAR:
            if (firstchildp->nextp->nextp->kind == RDB_NODE_TOK) {
                switch (firstchildp->nextp->nextp->val.token) {
                case TOK_REAL:
                    ret = Duro_exec_vardef_real(firstchildp->nextp, interp, ecp);
                    break;
                case TOK_VIRTUAL:
                    ret = Duro_exec_vardef_virtual(firstchildp->nextp, interp, ecp);
                    break;
                case TOK_PRIVATE:
                    ret = Duro_exec_vardef_private(firstchildp->nextp, interp, ecp);
                    break;
                case TOK_PUBLIC:
                    ret = Duro_exec_vardef_public(firstchildp->nextp, interp, ecp);
                    break;
                default:
                    ret = Duro_exec_vardef(firstchildp->nextp, interp, ecp);
                }
            } else {
                ret = Duro_exec_vardef(firstchildp->nextp, interp, ecp);
            }
            break;
        case TOK_DROP:
            switch (firstchildp->nextp->val.token) {
            case TOK_VAR:
                ret = Duro_exec_vardrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_CONSTRAINT:
                ret = exec_constrdrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_TYPE:
                ret = exec_typedrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_OPERATOR:
                ret = exec_opdrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_INDEX:
                ret = exec_indexdrop(firstchildp->nextp->nextp, interp, ecp);
                break;
            }
            break;
            case TOK_BEGIN:
                if (firstchildp->nextp->kind == RDB_NODE_INNER) {
                    /* BEGIN ... END */
                    ret = exec_stmts(firstchildp->nextp->val.children.firstp,
                            interp, ecp, retinfop);
                } else {
                    /* BEGIN TRANSACTION */
                    ret = exec_begin_tx(interp, ecp);
                }
                break;
            case TOK_COMMIT:
                ret = exec_commit(interp, ecp);
                break;
            case TOK_ROLLBACK:
                ret = exec_rollback(interp, ecp);
                break;
            case TOK_TYPE:
                ret = exec_typedef(firstchildp->nextp, interp, ecp);
                break;
            case TOK_RETURN:
                ret = exec_return(firstchildp->nextp, interp, ecp, retinfop);
                break;
            case TOK_LOAD:
                ret = exec_load(firstchildp->nextp, interp, ecp);
                break;
            case TOK_CONSTRAINT:
                ret = exec_constrdef(firstchildp->nextp, interp, ecp);
                break;
            case TOK_INDEX:
                ret = exec_indexdef(firstchildp->nextp, interp, ecp);
                break;
            case TOK_EXPLAIN:
                if (firstchildp->nextp->nextp->nextp == NULL) {
                    ret = Duro_exec_explain_assign(firstchildp->nextp, interp, ecp);
                } else {
                    ret = exec_explain(firstchildp->nextp, interp, ecp);
                }
                break;
            case TOK_RAISE:
                ret = exec_raise(firstchildp->nextp, interp, ecp);
                break;
            case TOK_IMPLEMENT:
                if (firstchildp->nextp->val.token == TOK_TYPE) {
                    ret = exec_typeimpl(firstchildp->nextp->nextp, interp, ecp);
                } else {
                    ret = exec_moduledef(firstchildp->nextp->nextp, interp, ecp, retinfop);
                }
                break;
            case TOK_MAP:
                ret = exec_map(firstchildp->nextp, interp, ecp);
                break;
            case TOK_RENAME:
                ret = Duro_exec_rename(firstchildp->nextp->nextp, interp, ecp);
                break;
            case TOK_MODULE:
                ret = exec_moduledef(firstchildp->nextp, interp, ecp, retinfop);
                break;
            default:
                RDB_raise_internal("invalid token", ecp);
                ret = RDB_ERROR;
        }
        if (ret == RDB_ERROR) {
            if (interp->err_line < 0) {
                interp->err_line = stmtp->lineno;
            }
        }
        return ret;
    }
    if (firstchildp->kind == RDB_NODE_EXPR) {
        if (firstchildp->nextp->val.token == '(') {
            /* Operator invocation */
            ret = exec_call(firstchildp, interp, ecp);
        } else {
            /* Loop with label */
            switch (firstchildp->nextp->nextp->val.token) {
            case TOK_WHILE:
                ret = exec_while(firstchildp->nextp->nextp->nextp,
                        firstchildp, interp, ecp, retinfop);
                break;
            case TOK_FOR:
                if (firstchildp->nextp->nextp->nextp->nextp->val.token == TOK_IN) {
                    ret = exec_foreach(firstchildp->nextp->nextp->nextp,
                            firstchildp, interp, ecp, retinfop);
                } else {
                    ret = exec_for(firstchildp->nextp->nextp->nextp,
                            firstchildp, interp, ecp, retinfop);
                }
                break;
            default:
                RDB_raise_internal("invalid token", ecp);
                ret = RDB_ERROR;
            }
        }
        if (ret == RDB_ERROR && interp->err_line < 0) {
            interp->err_line = stmtp->lineno;
        }
        return ret;
    }
    if (firstchildp->kind != RDB_NODE_INNER) {
        RDB_raise_internal("interpreter encountered invalid node", ecp);
        return RDB_ERROR;
    }
    if (firstchildp->nextp->val.token == '(') {
        /* Operator invocation with qualified operator name */
        ret = exec_call(firstchildp, interp, ecp);
        if (ret == RDB_ERROR && interp->err_line < 0) {
            interp->err_line = stmtp->lineno;
        }
        return ret;
    }

    /* Assignment */
    ret = Duro_exec_assign(firstchildp, interp, ecp);
    if (ret == RDB_ERROR && interp->err_line < 0) {
        interp->err_line = stmtp->lineno;
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
Duro_exec_stmt_impl_tx(RDB_parse_node *stmtp, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    /*
     * No implicit transaction if the statement is a BEGIN TX, COMMIT,
     * or ROLLBACK.
     */
    RDB_bool implicit_tx = RDB_obj_bool(&interp->implicit_tx_obj)
                    && !is_tx_stmt(stmtp) && (interp->txnp == NULL);

    /* No implicit tx if no database is available. */
    if (implicit_tx) {
        if (Duro_get_db(interp, ecp) == NULL) {
            if (RDB_obj_type(RDB_get_err(ecp)) != &RDB_NOT_FOUND_ERROR)
                return RDB_ERROR;
            RDB_clear_err(ecp);
            implicit_tx = RDB_FALSE;
        }
    }

    if (implicit_tx) {
        if (do_begin_tx(interp, ecp) != RDB_OK) {
            return RDB_ERROR;
        }
    }
    if (Duro_exec_stmt(stmtp, interp, ecp, NULL) != RDB_OK) {
        if (implicit_tx) {
            do_rollback(interp, ecp);
        }
        return RDB_ERROR;
    }

    if (implicit_tx) {
        return do_commit(interp, ecp);
    }
    return RDB_OK;
}

/*
 * Parse next statement and execute it.
 * Returning RDB_ERROR with no error in *ecp means that the end of input
 * has been reached.
 */
int
Duro_process_stmt(Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    RDB_parse_node *stmtp;
    RDB_object *dbnameobjp = RDB_hashmap_get(&interp->sys_varmap, "current_db");

    if (RDB_parse_get_interactive()) {
        /* Build interp->prompt */
        if (dbnameobjp != NULL && *RDB_obj_string(dbnameobjp) != '\0') {
            ret = RDB_string_to_obj(&interp->prompt, RDB_obj_string(dbnameobjp), ecp);
        } else {
            ret = RDB_string_to_obj(&interp->prompt, "no db", ecp);
        }
        if (ret != RDB_OK)
            return ret;
        RDB_append_string(&interp->prompt, "> ", ecp);
    }

    stmtp = RDB_parse_stmt(ecp);

    if (stmtp == NULL) {
        interp->err_line = yylineno;
        return RDB_ERROR;
    }
    RDB_clear_err(ecp);
    ret = Duro_exec_stmt_impl_tx(stmtp, interp, ecp);

    if (ret != RDB_OK) {
        if (ret == DURO_RETURN) {
            RDB_raise_syntax("invalid RETURN", ecp);
            interp->err_line = yylineno;
            return RDB_ERROR;
        }
        if (ret == DURO_LEAVE) {
            RDB_raise_syntax("unmatched LEAVE", ecp);
            interp->err_line = yylineno;
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

/**@defgroup interp Interpreter functions
 * \#include <dli/iinterp.h>
 * @{
 */

/**
 * Initialize the interpreter.
 */
int
Duro_init_interp(Duro_interp *interp, RDB_exec_context *ecp,
        RDB_environment *envp, const char *dbname)
{
    static RDB_parameter exit_int_params[1];
    static RDB_parameter connect_params[1];
    static RDB_parameter connect_create_params[2];
    static RDB_parameter create_db_params[1];
    static RDB_parameter create_env_params[1];
    static RDB_parameter system_params[2];
    static RDB_parameter trace_params[2];

    exit_int_params[0].typ = &RDB_INTEGER;
    exit_int_params[0].update = RDB_FALSE;
    connect_params[0].typ = &RDB_STRING;
    connect_params[0].update = RDB_FALSE;
    connect_create_params[0].typ = &RDB_STRING;
    connect_create_params[0].update = RDB_FALSE;
    connect_create_params[1].typ = &RDB_BOOLEAN;
    connect_create_params[1].update = RDB_FALSE;
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

    interp->txnp = NULL;
    interp->envp = envp;
    interp->inner_op = NULL;

    interp->leave_targetname = NULL;
    interp->impl_typename = NULL;
    interp->current_foreachp = NULL;

    Duro_init_vars(interp);

    RDB_init_op_map(&interp->sys_upd_op_map);

    RDB_init_hashmap(&interp->uop_info_map, 5);

    RDB_init_obj(&interp->current_db_obj);
    RDB_init_obj(&interp->implicit_tx_obj);

    RDB_init_obj(&interp->module_name);

    if (RDB_put_upd_op(&interp->sys_upd_op_map, "exit", 0, NULL, &exit_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "exit", 1, exit_int_params, &exit_int_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "connect", 1, connect_params, &connect_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "connect", 2, connect_create_params,
            &connect_recover_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "disconnect", 0, NULL, &disconnect_op,
            ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "create_db", 1, create_db_params,
            &create_db_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "create_env", 1, create_env_params,
            &create_env_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "trace", 1, trace_params,
            &trace_op, ecp) != RDB_OK)
        goto error;
    if (RDB_put_upd_op(&interp->sys_upd_op_map, "os.system", 2, system_params, &system_op,
            ecp) != RDB_OK)
        goto error;

    /* Create current_db and implicit_tx in system module */

    if (dbname == NULL)
        dbname = "";
    if (RDB_string_to_obj(&interp->current_db_obj, dbname, ecp) != RDB_OK) {
        goto error;
    }

    RDB_bool_to_obj(&interp->implicit_tx_obj, RDB_FALSE);

    if (RDB_hashmap_put(&interp->sys_varmap, "current_db", &interp->current_db_obj)
            != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    if (RDB_hashmap_put(&interp->sys_varmap, "implicit_tx", &interp->implicit_tx_obj)
            != RDB_OK) {
        RDB_raise_no_memory(ecp);
        goto error;
    }

    if (add_io(interp, ecp) != RDB_OK) {
        goto error;
    }

    if (RDB_string_to_obj(&interp->module_name, "", ecp) != RDB_OK)
        goto error;

    interp->err_opname = NULL;

    return RDB_OK;

error:
    RDB_destroy_obj(&interp->current_db_obj, ecp);
    RDB_destroy_obj(&interp->implicit_tx_obj, ecp);

    RDB_destroy_op_map(&interp->sys_upd_op_map);
    RDB_destroy_obj(&interp->module_name, ecp);
    RDB_destroy_hashmap(&interp->uop_info_map);
    return RDB_ERROR;
}

/**
 * Release resources allocated during interpreter initialization.
 * If a database environment is connected it will be closed.
 */
void
Duro_destroy_interp(Duro_interp *interp)
{
    RDB_exec_context ec;

    RDB_init_exec_context(&ec);

    RDB_free(interp->err_opname);

    Duro_destroy_vars(interp);

    if (RDB_parse_get_interactive()) {
        RDB_destroy_obj(&interp->prompt, &ec);
    }

    if (interp->txnp != NULL) {
        RDB_rollback(&ec, &interp->txnp->tx);

        if (RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }
    RDB_destroy_op_map(&interp->sys_upd_op_map);

    RDB_destroy_hashmap(&interp->uop_info_map);

    RDB_destroy_obj(&interp->module_name, &ec);

    if (interp->envp != NULL)
        RDB_close_env(interp->envp);

    RDB_destroy_exec_context(&ec);
}

RDB_environment *
Duro_dt_env(Duro_interp *interp)
{
    return interp->envp;
}

RDB_transaction *
Duro_dt_tx(Duro_interp *interp)
{
    return interp->txnp != NULL ? &interp->txnp->tx : NULL;
}

/**
 * Print an error to stderr.
 */
void
Duro_print_error(const RDB_object *errobjp) {
    Duro_print_error_f(errobjp, stderr);
}

/**
 * Print an error to an output stream.
 */
void
Duro_print_error_f(const RDB_object *errobjp, FILE *f)
{
    RDB_exec_context ec;
    RDB_object msgobj;
    RDB_type *errtyp = RDB_obj_type(errobjp);

    RDB_init_exec_context(&ec);
    RDB_init_obj(&msgobj);

    fputs(RDB_type_name(errtyp), f);

    if (RDB_obj_property(errobjp, "msg", &msgobj, NULL, &ec, NULL) == RDB_OK) {
        fprintf(f, ": %s", RDB_obj_string(&msgobj));
    }

    fputs ("\n", f);

    RDB_destroy_obj(&msgobj, &ec);
    RDB_destroy_exec_context(&ec);
}

/**
 * Make the interpreter raise a system_error.
 * This function may be called from a signal handler.
 */
void
Duro_dt_interrupt(Duro_interp *interp)
{
    interp->interrupted = 1;
}

/**
 * Read statements from file given by <var>path</var> and execute them.
 * If <var>path</var> is NULL, read from standard input.
 * If the input is a terminal, a function for reading lines must be
 * provided using RDB_parse_set_read_line_fn().
 */
int
Duro_dt_execute_path(const char *path,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    FILE *fp = NULL;

    if (path != NULL) {
        fp = fopen(path, "r");
        if (fp == NULL) {
            RDB_raise_resource_not_found(path, ecp);
            return RDB_ERROR;
        }
    } else {
        fp = stdin;
    }
    ret = Duro_dt_execute(fp, interp, ecp);
    if (path != NULL)
        fclose(fp);
    return ret;
}

/**
 * Read statements from input stream *infp and execute them.
 * If the input is a terminal, a function for reading lines must be
 * provided using RDB_parse_set_read_line_fn().
 */
int
Duro_dt_execute(FILE *infp, Duro_interp *interp, RDB_exec_context *ecp)
{
    interp->interrupted = 0;

    /* Initialize error line */
    interp->err_line = -1;

    if (isatty(fileno(infp))) {
        RDB_parse_set_interactive(RDB_TRUE);

        /* Prompt is only needed in interactive mode */
        RDB_init_obj(&interp->prompt);

        printf("Duro D/T library version %s\n", RDB_release_number);

        puts("Implicit transactions enabled.");
        RDB_bool_to_obj(&interp->implicit_tx_obj, RDB_TRUE);
    } else {
        RDB_parse_set_interactive(RDB_FALSE);
    }

    RDB_parse_init_buf(infp);

    /*
     * Store pointer to the Duro_interp structure in the execution context
     * to make it available to operators
     */
    RDB_ec_set_property(ecp, "INTERP", interp);

    for(;;) {
        if (Duro_process_stmt(interp, ecp) != RDB_OK) {
            RDB_object *errobjp = RDB_get_err(ecp);
            if (errobjp != NULL) {
                /*
                 * In non-interactive mode, return with error.
                 * In interactive mode, print error and continue with next line.
                 */
                if (!RDB_parse_get_interactive()) {
                    goto error;
                }

                /* Show line number only if an operator was invoked */
                if (interp->err_opname != NULL) {
                    fprintf(stderr, "error in operator %s at line %d: ", interp->err_opname, interp->err_line);
                }
                Duro_print_error(errobjp);
                RDB_parse_flush_buf();
                interp->err_line = -1;
                RDB_clear_err(ecp);
            } else {
                /* Exit on EOF  */
                puts("");
                RDB_parse_destroy_buf();
                return RDB_OK;
            }
        }
    }

error:
    RDB_parse_destroy_buf();
    return RDB_ERROR;
}

/**
 * Read statements from string instr and execute them.
 */
int
Duro_dt_execute_str(const char *instr, Duro_interp *interp,
        RDB_exec_context *ecp)
{
    YY_BUFFER_STATE buf;
    interp->interrupted = 0;

    RDB_parse_set_interactive(RDB_FALSE);

    RDB_ec_set_property(ecp, "INTERP", interp);

    buf = yy_scan_string(instr);

    while (Duro_process_stmt(interp, ecp) == RDB_OK);

    if (RDB_get_err(ecp) != NULL) {
        goto error;
    } else {
        /* Exit on EOF  */
        yy_delete_buffer(buf);
        return RDB_OK;
    }

error:
    yy_delete_buffer(buf);
    return RDB_ERROR;
}

RDB_expression *
Duro_dt_parse_expr_str(const char *instr,
        Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_parse_node *nodep = RDB_parse_expr(instr, ecp);
    if (nodep == NULL)
        return NULL;

    return RDB_parse_node_expr(nodep, ecp, Duro_dt_tx(interp));
}

/**
 * Return a pointer to the interp->prompt that is used in interactive mode.
 */
const char*
Duro_dt_prompt(Duro_interp *interp)
{
    return RDB_obj_string(&interp->prompt);
}

/**
 * Look up a variable and return a pointer to the RDB_object
 * containing its value.
 */
RDB_object *
Duro_lookup_var(const char *name, Duro_interp *interp, RDB_exec_context *ecp)
{
    RDB_object *objp = Duro_lookup_transient_var(interp, name);
    if (objp != NULL)
        return objp;

    if (interp->txnp != NULL) {
        /* Try to get table from DB */
        objp = RDB_get_table(name, ecp, &interp->txnp->tx);
    }
    if (objp == NULL)
        RDB_raise_name(name, ecp);
    return objp;
}

/**
 * Specifies how to create a user-defined operator implemented
 * in the language given by argument <var>lang</var>.
 *
 * If the interpreter encounters a statement of the form
 *
 * <code>
 * OPERATOR &lt;opname&gt; ( &lt;parameters&gt; ) RETURNS &lt;type&gt;
 * EXTERN '&lt;lang&gt;' '&lt;external_ref&gt;'; END OPERATOR;
 * </code>
 *
 * it will call RDB_create_ro_op() with arguments libname and symname
 * set to infop->libname and info->ro_op_symname, respectively.
 *
 * If the interpreter encounters a statement of the form
 *
 * <code>
 * OPERATOR &lt;opname&gt; ( &lt;parameters&gt; ) UPDATES { &lt;update parameters&gt; }
 * EXTERN '&lt;lang&gt;' '&lt;external_ref&gt;'; END OPERATOR;
 * </code>
 *
 * it will call RDB_create_update_op() with arguments libname and symname
 * set to infop->libname and info->update_op_symname, respectively.
 */
int
Duro_dt_put_creop_info(Duro_interp *interp, const char *lang,
        Duro_uop_info *infop, RDB_exec_context *ecp)
{
    int ret = RDB_hashmap_put(&interp->uop_info_map, lang, infop);
    if (ret != RDB_OK) {
        RDB_errno_to_error(ret, ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}

/**
 * Return the information about operator creation that was
 * previously pass to Duro_dt_put_creop_info().
 */
Duro_uop_info *
Duro_dt_get_creop_info(const Duro_interp *interp, const char *lang)
{
    return RDB_hashmap_get(&interp->uop_info_map, lang);
}

/**
 * @}
 */
