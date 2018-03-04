/*
 * Statement execution functions.
 *
 * Copyright (C) 2007, 2014-2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "iinterp.h"
#include "interp_stmt.h"
#include "interp_core.h"
#include "ioop.h"
#include <gen/releaseno.h>

#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>

#ifdef _WIN32
#include <io.h>
#define isatty _isatty
#else
#include <unistd.h>
#endif

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *txt);
void yy_delete_buffer(YY_BUFFER_STATE);

/** @page update-ops Built-in system and connection operators

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

int
Duro_rollback_all(Duro_interp *interp, RDB_exec_context *ecp)
{
    int ret;
    tx_node *htxnp;

    if (interp->txnp == NULL)
        return RDB_OK;
    ret = RDB_rollback_all(ecp, &interp->txnp->tx);

    do {
        htxnp = interp->txnp;
        interp->txnp = interp->txnp->parentp;
        RDB_free(htxnp);
    } while (interp->txnp != NULL);
    return ret;
}

static int
connect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->txnp != NULL) {
        ret = Duro_rollback_all(interp, ecp);
        if (ret == RDB_OK && RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }

    /* If a connection exists, close it */
    if (interp->envp != NULL) {
        RDB_close_env(interp->envp, ecp);
    }

    /*
     * Try opening the environment without RDB_RECOVER first
     * to attach to existing memory pool
     */

    interp->envp = RDB_open_env(RDB_obj_string(argv[0]), 0, ecp);
    if (interp->envp == NULL) {
        /*
         * Retry with RDB_RECOVER option, re-creating necessary files
         * and running recovery
         */
        interp->envp = RDB_open_env(RDB_obj_string(argv[0]), RDB_RECOVER, ecp);
        if (interp->envp == NULL) {
            RDB_handle_err(ecp, txp);
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

    if (interp->txnp != NULL) {
        ret = Duro_rollback_all(interp, ecp);
        if (ret == RDB_OK && RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }

    if (interp->envp != NULL) {
        RDB_close_env(interp->envp, ecp);
    }

    interp->envp = RDB_open_env(RDB_obj_string(argv[0]),
            RDB_obj_bool(argv[1]) ? RDB_RECOVER : 0, ecp);
    if (interp->envp == NULL) {
        RDB_handle_err(ecp, txp);
        interp->envp = NULL;
        return RDB_ERROR;
    }
    return RDB_OK;
}

static int
disconnect_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    Duro_var_entry *entryp;
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
        ret = Duro_rollback_all(interp, ecp);

        if (ret == RDB_OK && RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }

    /* Close DB environment */
    ret = RDB_close_env(interp->envp, ecp);
    interp->envp = NULL;
    if (ret != RDB_OK) {
        RDB_handle_err(ecp, txp);
        return RDB_ERROR;
    }

    /* If CURRENT_DB was set, set it to empty string */
    entryp = Duro_varmap_get(&interp->root_varmap, "current_db");
    if (entryp == NULL || entryp->varp == NULL || *RDB_obj_string(entryp->varp) == '\0') {
        return RDB_OK;
    }
    return RDB_string_to_obj(entryp->varp, "", ecp);
}

static int
create_env_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    int ret;
    Duro_interp *interp = RDB_ec_property(ecp, "INTERP");

    if (interp->envp != NULL) {
        RDB_close_env(interp->envp, ecp);
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

    interp->envp = RDB_create_env(RDB_obj_string(argv[0]), ecp);
    if (interp->envp == NULL) {
        RDB_handle_err(ecp, txp);
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

    if (Duro_varmap_put(&interp->root_varmap, "io.stdin", &Duro_stdin_obj,
            DURO_VAR_CONST, ecp) != RDB_OK) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }
    if (Duro_varmap_put(&interp->root_varmap, "io.stdout", &Duro_stdout_obj,
            DURO_VAR_CONST, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    if (Duro_varmap_put(&interp->root_varmap, "io.stderr", &Duro_stderr_obj,
            DURO_VAR_CONST, ecp) != RDB_OK) {
        return RDB_ERROR;
    }
    return RDB_OK;
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
    static RDB_parameter connect_params[1];
    static RDB_parameter connect_create_params[2];
    static RDB_parameter create_db_params[1];
    static RDB_parameter create_env_params[1];
    static RDB_parameter trace_params[2];

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

    RDB_init_obj(&interp->pkg_name);

    interp->current_db_objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (interp->current_db_objp == NULL)
        goto error;
    RDB_init_obj(interp->current_db_objp);

    if (dbname == NULL)
        dbname = "";
    if (RDB_string_to_obj(interp->current_db_objp, dbname, ecp) != RDB_OK) {
        goto error;
    }

    interp->implicit_tx_objp = RDB_alloc(sizeof (RDB_object), ecp);
    if (interp->implicit_tx_objp == NULL)
        goto error;
    RDB_init_obj(interp->implicit_tx_objp);

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

    /* Create current_db and implicit_tx in system package */

    RDB_bool_to_obj(interp->implicit_tx_objp, RDB_FALSE);

    if (Duro_varmap_put(&interp->root_varmap, "current_db",
            interp->current_db_objp, DURO_VAR_FREE, ecp) != RDB_OK) {
        goto error;
    }

    if (Duro_varmap_put(&interp->root_varmap, "implicit_tx",
            interp->implicit_tx_objp, DURO_VAR_FREE, ecp) != RDB_OK) {
        goto error;
    }

    if (add_io(interp, ecp) != RDB_OK) {
        goto error;
    }

    if (RDB_string_to_obj(&interp->pkg_name, "", ecp) != RDB_OK)
        goto error;

    interp->err_opname = NULL;

    return RDB_OK;

error:
    RDB_destroy_op_map(&interp->sys_upd_op_map);
    RDB_destroy_obj(&interp->pkg_name, ecp);
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
        int ret = Duro_rollback_all(interp, &ec);

        if (ret == RDB_OK && RDB_parse_get_interactive())
            printf("Transaction rolled back.\n");
    }
    RDB_destroy_op_map(&interp->sys_upd_op_map);

    RDB_destroy_hashmap(&interp->uop_info_map);

    RDB_destroy_obj(&interp->pkg_name, &ec);

    if (interp->envp != NULL)
        RDB_close_env(interp->envp, &ec);

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
 * Print an error to stderr, followed by a newline.
 */
void
Duro_println_error(const RDB_object *errobjp) {
    Duro_print_error_f(errobjp, stderr);
    fputs ("\n", stderr);
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

    if (errtyp != NULL) {
        fputs(RDB_type_name(errtyp), f);

        if (RDB_obj_property(errobjp, "msg", &msgobj, NULL, &ec, NULL) == RDB_OK) {
            fprintf(f, ": %s", RDB_obj_string(&msgobj));
        }
    }

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

    /* Initialize error line and operator */
    interp->err_line = -1;
    RDB_free(interp->err_opname);
    interp->err_opname = NULL;

    if (isatty(fileno(infp))) {
        RDB_parse_set_interactive(RDB_TRUE);

        /* Prompt is only needed in interactive mode */
        RDB_init_obj(&interp->prompt);

        printf("Duro D/T library version %s\n", RDB_release_number);

        puts("Implicit transactions enabled.");
        RDB_bool_to_obj(interp->implicit_tx_objp, RDB_TRUE);
    } else {
        RDB_parse_set_interactive(RDB_FALSE);
    }

    RDB_parse_init_buf(infp);

    /*
     * Store pointer to the Duro_interp structure in the execution context
     * to make it available to operators
     */
    if (RDB_ec_set_property(ecp, "INTERP", interp) != RDB_OK) {
        goto error;
    }

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
                    RDB_free(interp->err_opname);
                    interp->err_opname = NULL;
                }
                Duro_println_error(errobjp);
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

    /* Initialize error line and operator */
    interp->err_line = -1;
    RDB_free(interp->err_opname);
    interp->err_opname = NULL;

    RDB_parse_set_interactive(RDB_FALSE);

    if (RDB_ec_set_property(ecp, "INTERP", interp) != RDB_OK)
        return RDB_ERROR;

    buf = yy_scan_string(instr);
    if (buf == NULL) {
        RDB_raise_internal("yy_scan_string() failed", ecp);
        goto error;
    }

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
 * Looks up a variable and returns a pointer to the RDB_object
 * containing its value.
 */
RDB_object *
Duro_lookup_var(const char *name, Duro_interp *interp, RDB_exec_context *ecp)
{
    int flags;
    RDB_object *objp = Duro_lookup_sym(name, interp, &flags, ecp);
    if (objp != NULL && (DURO_VAR_CONST & flags)) {
        RDB_raise_name(name, ecp);
        return NULL;
    }
    return objp;
}

/**
 * Looks up a variable or constant and returns a pointer to the RDB_object
 * containing its value.
 */
RDB_object *
Duro_lookup_sym(const char *name, Duro_interp *interp, int *flagsp,
        RDB_exec_context *ecp)
{
    Duro_var_entry *entryp = Duro_lookup_transient_var_e(interp,  name);
    if (entryp != NULL) {
        if (flagsp != NULL)
            *flagsp = entryp->flags;
        return entryp->varp;
    }

    if (interp->txnp == NULL) {
        RDB_raise_name(name, ecp);
        return NULL;
    }
    /* Try to get table from DB */
    if (flagsp != NULL)
        *flagsp = 0;
    return RDB_get_table(name, ecp, &interp->txnp->tx);
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
