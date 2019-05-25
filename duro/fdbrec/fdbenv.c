#include "fdbenv.h"
#include <obj/excontext.h>
#include <rec/envimpl.h>
#include <fdbrec/fdbrecmap.h>
#include <fdbrec/fdbsequence.h>
#include <fdbrec/fdbtx.h>

#ifdef _WIN32
#include <windows.h>
#include <strsafe.h>
#else
#include <pthread.h>
#endif

#include <stdio.h>

static RDB_bool RDB_fdb_initialized = RDB_FALSE;

void *
RDB_event_loop(void *arg)
{
    int err = fdb_run_network();
    if (err != 0) {
        printf("error %d\n", err);
    }
    return NULL;
}

#ifdef _WIN32
DWORD WINAPI
RDB_win_event_loop(_In_ LPVOID lpParameter)
{
    RDB_event_loop(NULL);
    return (DWORD) 0;
}
#endif

static int
RDB_fdb_close_env(RDB_environment *envp, RDB_exec_context *ecp)
{
    if (RDB_fdb_resultf != NULL) {
        fdb_future_destroy(RDB_fdb_resultf);
        RDB_fdb_resultf = NULL;
    }
	fdb_database_destroy(envp->env.fdb);
	free(envp);
    return RDB_OK;
}

static void
fdb_set_errfile(RDB_environment *envp, FILE *file)
{ }

static FILE *
fdb_get_errfile(const RDB_environment *envp)
{
	return NULL;
}

RDB_environment *
RDB_fdb_open_env(const char *path, RDB_exec_context *ecp)
{
    fdb_error_t err;
    FDBCluster *cluster;
    FDBFuture *f;
    RDB_environment *envp;

    if (!RDB_fdb_initialized) {
#ifndef _WIN32
        pthread_t thread;
#endif

        err = fdb_select_api_version(600);
        if (err != 0) {
            RDB_handle_fdb_errcode(err, ecp, NULL);
            return NULL;
        }
        RDB_fdb_initialized = RDB_TRUE;

        err = fdb_setup_network();
        if (err != 0) {
            RDB_handle_fdb_errcode(err, ecp, NULL);
            return NULL;
        }
#ifdef _WIN32
        if (CreateThread(NULL, 0, &RDB_win_event_loop, NULL, 0, NULL) == NULL) {
#else
        if (pthread_create(&thread, NULL, &RDB_event_loop, NULL) != 0) {
#endif
            RDB_raise_system("thread creation failed", ecp);
            return NULL;
        }
    }

    envp = malloc(sizeof(RDB_environment));
    if (envp == NULL) {
        RDB_raise_no_memory(ecp);
        return NULL;
    }
    envp->close_fn = &RDB_fdb_close_env;
    envp->create_recmap_fn = &RDB_create_fdb_recmap;
    envp->open_recmap_fn = &RDB_open_fdb_recmap;
    envp->open_sequence_fn = &RDB_open_fdb_sequence;
    envp->rename_sequence_fn = &RDB_rename_fdb_sequence;
    envp->begin_tx_fn = &RDB_fdb_begin_tx;
    envp->commit_fn = &RDB_fdb_commit;
    envp->abort_fn = &RDB_fdb_abort;
    envp->tx_id_fn = &RDB_fdb_tx_id;
    envp->set_errfile_fn = &fdb_set_errfile;
    envp->get_errfile_fn = &fdb_get_errfile;

    envp->cleanup_fn = NULL;
    envp->xdata = NULL;
    envp->trace = 0;
    envp->queries = RDB_FALSE;

    f = fdb_create_cluster(path);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, NULL);
        free(envp);
        return NULL;
    }
    if ((err = fdb_future_get_cluster(f, &cluster)) != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, NULL);
        free(envp);
        return NULL;
    }
    fdb_future_destroy(f);
    f = fdb_cluster_create_database(cluster, (uint8_t const*) "DB", 2);
    err = fdb_future_block_until_ready(f);
    if (err != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, NULL);
        free(envp);
        return NULL;
    }
    if ((err = fdb_future_get_database(f, &envp->env.fdb)) != 0) {
        fdb_future_destroy(f);
        RDB_handle_fdb_errcode(err, ecp, NULL);
        free(envp);
        return NULL;
    }
    fdb_future_destroy(f);

    return envp;
}

void
RDB_handle_fdb_errcode(fdb_error_t err, RDB_exec_context *ecp, FDBTransaction *transaction)
{
    if (transaction != NULL) {
        FDBFuture *f = fdb_transaction_on_error(transaction, err);
        fdb_error_t err2 = fdb_future_block_until_ready(f);
        if (err2 == 0) {
            err2 = fdb_future_get_error(f);
        }
        ecp->error_retryable = (err2 == 0) ? RDB_TRUE : RDB_FALSE;
        fdb_future_destroy(f);
    }

	switch (err) {
	case 1020:
		RDB_raise_concurrency(ecp);
		break;
	case 1501:
		RDB_raise_no_memory(ecp);
		break;
	case 1511:
	case 1515:
		RDB_raise_resource_not_found(fdb_get_error(err), ecp);
	case 1102:
	case 2015:
	case 2016:
	case 2200:
	case 2201:
	case 2202:
	case 2203:
	case 4100:
		RDB_raise_internal(fdb_get_error(err), ecp);
		break;
	case 2004:
	case 2012:
	case 2013: 
	case 2018:
	case 2104:
		RDB_raise_invalid_argument(fdb_get_error(err), ecp);
		break;
	case 1038:
	case 2105:
		RDB_raise_in_use(fdb_get_error(err), ecp);
		break;
	case 2102:
	case 2103:
	case 2108:
		RDB_raise_not_supported(fdb_get_error(err), ecp);
		break;
	default:
		RDB_raise_system(fdb_get_error(err), ecp);
	}
}
