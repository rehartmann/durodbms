/*
 * $Id$
 *
 * Copyright (C) 2008 René Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Interpreter for Duro D/T.
 */

#include <rel/rdb.h>
#include <rel/internal.h>
#include "parse.h"
#include "iinterp.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

static void
usage_error(void)
{
    puts("usage: durodt [-e envpath] [-d database] [file]");
    exit(1);
}

static void
handle_sigint(int sig) {
    Duro_dt_interrupt();
}

int
main(int argc, char *argv[])
{
    int ret;
    RDB_exec_context ec;
    char *envname = NULL;
    char *dbname = "";
    RDB_environment *envp;
    char *infilename;
#ifndef _WIN32
    struct sigaction sigact;
#endif

#ifdef _WIN32
    signal(SIGINT, handle_sigint);
#else
    sigact.sa_handler = &handle_sigint;
    sigemptyset(&sigact.sa_mask);
    sigact.sa_flags = 0;
    if (sigaction(SIGINT, &sigact, NULL) == -1)
        fprintf(stderr, "sigaction(): %s\n", strerror(errno));
#endif

    while (argc > 1) {
        if (strcmp(argv[1], "-e") == 0) {
            if (argc < 3)
                usage_error();
            envname = argv[2];
            argc -= 2;
            argv += 2;
        } else if (strcmp(argv[1], "-d") == 0) {
            if (argc < 3)
                usage_error();
            dbname = argv[2];
            argc -= 2;
            argv += 2;
        } else
            break;
    }
    if (argc > 2)
        usage_error();
    if (argc == 2) {
        infilename = argv[1];
    } else {
        infilename = NULL;
    }

    if (envname != NULL) {
        ret = RDB_open_env(envname, &envp);
        if (ret != RDB_OK) {
            fprintf(stderr, "unable to open environment %s: %s\n", envname,
                db_strerror(errno));
            return 1;
        }
    } else {
        envp = NULL;
    }

    RDB_init_exec_context(&ec);

    if (_RDB_init_builtin_types(&ec) != RDB_OK) {
        Duro_print_error(RDB_get_err(&ec));
        goto error;
    }

    if (Duro_dt_execute(envp, dbname, infilename, &ec) != RDB_OK) {
        Duro_print_error(RDB_get_err(&ec));
        goto error;
    }

    RDB_destroy_exec_context(&ec);
    return 0;

error:
    RDB_destroy_exec_context(&ec);
    return 1;    
}
