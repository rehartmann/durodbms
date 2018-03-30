/*
 * Copyright (C) 2018 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * DuroDBMS REST server
 */

#include <microhttpd.h>
#include <rel/rdb.h>
#include <dli/iinterp.h>
#include <sys/types.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <rel/json.h>
#include <signal.h>

#define DEFAULT_PORT 8888

RDB_exec_context ec;
Duro_interp interp;

static const char *
split_get(const char *path, char **exp)
{
    unsigned int dbnamelen;
    char *dbname;

    if (path[0] == '/')
        ++path;

    *exp = strchr(path, '/');
    if (*exp != NULL) {
        dbnamelen = (unsigned int) (*exp - path);
        ++*exp;
    } else {
        dbnamelen = strlen(path);
    }

    dbname = malloc(dbnamelen + 1);
    if (dbname == NULL)
        return NULL;
    strncpy(dbname, path, dbnamelen);
    dbname[dbnamelen] = '\0';
    return dbname;
}

static int
respond_out_of_memory(struct MHD_Connection *connection)
{
    const char *txt = "<html><head><title>Out of Memory</title><body><p>Out of memory";
    struct MHD_Response *response;
    int ret;

    response = MHD_create_response_from_buffer(strlen(txt),
            (void *) txt, MHD_RESPMEM_PERSISTENT);
    ret = MHD_queue_response(connection, MHD_HTTP_INSUFFICIENT_STORAGE, response);
        MHD_destroy_response(response);

    return ret;
}

static int
respond_not_found(struct MHD_Connection *connection)
{
    const char *txt = "<html><head><title>Not found</title><body><p>Not found";
    struct MHD_Response *response;
    int ret;

    response = MHD_create_response_from_buffer(strlen(txt),
            (void *) txt, MHD_RESPMEM_PERSISTENT);
    ret = MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, response);
        MHD_destroy_response(response);

    return ret;
}

static int
query_to_json(const char *dbname, const char *expstr, RDB_object *json)
{
    RDB_object *dbobjp;
    RDB_object result;
    RDB_expression *exp;
    const char *varname;

    RDB_init_obj(&result);
    dbobjp = Duro_lookup_var("current_db", &interp, &ec);
    if (dbobjp == NULL) {
        goto error;
    }

    if (RDB_string_to_obj(dbobjp, dbname, &ec) != RDB_OK) {
        goto error;
    }

    exp = Duro_dt_parse_expr_str(expstr, &interp, &ec);
    if (exp == NULL) {
        goto error;
    }

    if (Duro_begin_tx(&interp, &ec) != RDB_OK)
        goto error;

    varname = RDB_expr_var_name(exp);
    if (varname != NULL) {
        RDB_object *tbp = RDB_get_table(varname, &ec, Duro_dt_tx(&interp));
        if (tbp == NULL)
            goto error;
        if (RDB_table_to_array(&result, tbp, 0, NULL, 0, &ec,
                Duro_dt_tx(&interp)) != RDB_OK) {
            goto error;
        }
    } else {
        RDB_type *typ = RDB_expr_type(exp, NULL, NULL, NULL, &ec, Duro_dt_tx(&interp));
        if (typ != NULL && RDB_type_is_relation(typ)) {
            RDB_object *tbp = RDB_expr_to_vtable(exp, &ec, Duro_dt_tx(&interp));
            if (tbp == NULL) {
                RDB_drop_table(tbp, &ec, Duro_dt_tx(&interp));
                goto error;
            }
            exp = NULL;
            if (RDB_table_to_array(&result, tbp, 0, NULL, 0, &ec,
                    Duro_dt_tx(&interp)) != RDB_OK) {
                RDB_drop_table(tbp, &ec, Duro_dt_tx(&interp));
                goto error;
            }
            RDB_drop_table(tbp, &ec, Duro_dt_tx(&interp));
        } else {
            if (Duro_evaluate(exp, &interp, &ec, &result) != RDB_OK) {
                goto error;
            }
        }
    }

    if (RDB_obj_to_json(json, &result, &ec, Duro_dt_tx(&interp)) != RDB_OK) {
        goto error;
    }

    if (Duro_commit(&interp, &ec) != RDB_OK)
        goto error;

    if (exp != NULL)
        RDB_del_expr(exp, &ec);
    RDB_destroy_obj(&result, &ec);
    return RDB_OK;

error:
    if (interp.txnp != NULL) {
        Duro_rollback(&interp, &ec);
    }

    if (exp != NULL)
        RDB_del_expr(exp, &ec);
    RDB_destroy_obj(&result, &ec);
    return RDB_ERROR;
}

static int
respond_invalid_query(struct MHD_Connection *connection)
{
    const char *txt = "<html><head><title>Invalid query</title><body><p>Invalid query";
    struct MHD_Response *response;
    int ret;

    response = MHD_create_response_from_buffer(strlen(txt),
            (void *) txt, MHD_RESPMEM_PERSISTENT);
    ret = MHD_queue_response(connection, MHD_HTTP_NOT_FOUND, response);
    MHD_destroy_response(response);

    return ret;
}

static int
respond(void *cls, struct MHD_Connection *connection,
       const char *url,
       const char *method, const char *version,
       const char *upload_data,
       size_t *upload_data_size, void **ptr)
{
    static int aptr;
    const char *dbname;
    char *expstr;
    struct MHD_Response *response;
    RDB_object json;
    int ret;

    if (strcmp(method, MHD_HTTP_METHOD_GET) != 0) {
        return MHD_NO;
    }
    if (&aptr != *ptr) {
        *ptr = &aptr;
        return MHD_YES;
    }

    dbname = split_get(url, &expstr);
    if (dbname == NULL) {
        return respond_out_of_memory(connection);
    }
    if (expstr == NULL) {
        free((void *) dbname);
        return respond_not_found(connection);
    }

    RDB_init_obj(&json);
    if (query_to_json(dbname, expstr, &json) == RDB_ERROR) {
        free((void *) dbname);
        Duro_println_error(RDB_get_err(&ec));
        RDB_destroy_obj(&json, &ec);
        return respond_invalid_query(connection);
    }
    free((void *) dbname);

    response = MHD_create_response_from_buffer(strlen(json.val.bin.datap),
            json.val.bin.datap, MHD_RESPMEM_MUST_FREE);
    ret = MHD_add_response_header (response, "Content-Type", "application/json");
    if (ret == MHD_NO) {
        MHD_destroy_response(response);
        RDB_destroy_obj(&json, &ec);
        return MHD_NO;
    }
    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);

    /*
     * The data is freed by MHD so set the pointer to NULL
     * so that RDB_destroy_obj() doesn't free it
     */
    json.val.bin.datap = NULL;
    json.val.bin.len = 0;
    RDB_destroy_obj(&json, &ec);
    return ret;
}

static char *
read_args(int argc, char *argv[], int *port)
{
    char *envname = NULL;
    int i;

    *port = DEFAULT_PORT;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-e") == 0 && i + 1 < argc) {
            envname = argv[++i];
        }
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            *port = atoi(argv[++i]);
        }
    }
    return envname;
}

static void
print_usage(void)
{
    fputs("Usage: durod -e envdir [-p port]\n", stderr);
}

static void
sig(int signal)
{}

static void
handle_signals()
{
    struct sigaction sigact;

    sigact.sa_handler = SIG_IGN;
    sigact.sa_flags = SA_RESTART;
    if (sigaction(SIGPIPE, &sigact, NULL) != 0) {
        fprintf(stderr, "Failed to install SIGPIPE handler: %s\n", strerror(errno));
        exit(2);
    }
    sigact.sa_handler = &sig;
    sigact.sa_flags = SA_RESTART;
    if (sigaction(SIGINT, &sigact, NULL) != 0) {
        fprintf(stderr, "Failed to install SIGINT handler: %s\n", strerror(errno));
        exit(2);
    }
    sigact.sa_handler = &sig;
    sigact.sa_flags = SA_RESTART;
    if (sigaction(SIGTERM, &sigact, NULL) != 0) {
        fprintf(stderr, "Failed to install SIGTERM handler: %s\n", strerror(errno));
        exit(2);
    }
}

int
main(int argc, char *argv[])
{
    struct MHD_Daemon *daemon;
    char *envname;
    RDB_environment *envp = NULL;
    int port;
    sigset_t oldmask, newmask;

    envname = read_args(argc, argv, &port);
    if (envname == NULL) {
        fputs("No database environment specified.\n", stderr);
        print_usage();
        return 1;
    }

    handle_signals();

    RDB_init_exec_context(&ec);

    if (RDB_init_builtin(&ec) != RDB_OK) {
        Duro_println_error(RDB_get_err(&ec));
        goto error;
    }

    envp = RDB_open_env(envname, 0, &ec);
    if (envp == NULL) {
        envp = RDB_open_env(envname, RDB_RECOVER, &ec);
        if (envp == NULL) {
            Duro_println_error(RDB_get_err(&ec));
            goto error;
        }
    }

    if (Duro_init_interp(&interp, &ec, envp, NULL) != RDB_OK) {
        Duro_println_error(RDB_get_err(&ec));
        goto error;
    }

    /* Block SIGINT and SIGTERM */
    sigemptyset(&newmask);
    sigaddset(&newmask, SIGQUIT);
    sigaddset(&newmask, SIGTERM);
    if (sigprocmask(SIG_BLOCK, &newmask, &oldmask) != 0) {
        fputs("sigprocmask(SIG_BLOCK) failed\n", stderr);
        goto error;
    }

    daemon = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY, port, NULL, NULL,
            &respond, NULL, MHD_OPTION_END);
    if (daemon == NULL) {
        fputs("Starting HTTP server failed.\n", stderr);
        goto error;
    }

    /* Restore signal mask */
    if (sigprocmask(SIG_SETMASK, &oldmask, 0) != 0) {
        fputs("sigprocmask(SIG_SETMASK) failed\n", stderr);
        goto error;
    }
    pause();

    puts("Stopping server...");

    MHD_stop_daemon(daemon);

    if (envp != NULL) {
        if (RDB_close_env(envp, &ec) != RDB_OK) {
            Duro_println_error(RDB_get_err(&ec));
        }
    }

    RDB_destroy_exec_context(&ec);
    return 0;

error:
    if (envp != NULL) {
        RDB_close_env(envp, &ec);
    }

    RDB_destroy_exec_context(&ec);
    return 1;
}
