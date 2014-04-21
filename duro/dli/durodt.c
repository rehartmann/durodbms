/*
 * $Id$
 *
 * Copyright (C) 2012 Rene Hartmann.
 * See the file COPYING for redistribution information.
 *
 * Main program of Duro D/T, an interpreter for Duro's variant of Tutorial D.
 */

#include <rel/rdb.h>
#include "parse.h"
#include "iinterp.h"

#include <string.h>
#include <errno.h>
#include <signal.h>
#include <locale.h>

/* Don't use GNU readline under Windows */
#ifndef _WIN32
#define USE_READLINE
#endif

#ifdef USE_READLINE
#include <readline/readline.h>
#include <readline/history.h>
#else
#include <stdio.h>
#endif

static Duro_interp interp;

static void
usage_error(void)
{
    puts("usage: durodt [-e envpath] [-d database] [file]");
    exit(1);
}

static void
handle_sigint(int sig)
{
    Duro_dt_interrupt(&interp);
}

/*
 * Read a line of input in interactive mode
 */
static char *
read_line_interactive(void)
{
    char *line;
#ifdef USE_READLINE
    /* Read a line using GNU readline */
    line = readline(Duro_dt_prompt(&interp));

    /* Store line in history if it is not empty */
    if (line != NULL && line[0] != '\0')
        add_history(line);

    return line;
#else
    #define DURO_INPUT_LINE_LENGTH 256

    fputs(Duro_dt_prompt(&interp), stdout);
    line = malloc(DURO_INPUT_LINE_LENGTH);
    if (line == NULL)
        return NULL;
    return fgets(line, DURO_INPUT_LINE_LENGTH, stdin);
#endif
}

void
free_line_interactive(char *line) {
    free(line);
}

#ifdef USE_READLINE
char *
generator(const char *text, int state)
{
    static int idx;
    static size_t len;
    int tok;
    const char *name;

    if (state == 0) {
        /* Initialize */
        idx = 0;
        len = strlen(text);
    }

    /* Return next matching keyword or NULL if there is none */
    while ((tok = RDB_parse_tokens[idx++]) != -1) {
        name = RDB_token_name(tok);
        if (strncasecmp(name, text, len) == 0)
            return strdup(name);
        /*
         * Tokens are in sorted order, so there will be no matching
         * tokens after a token that does not match.
         * So return NULL if it's not the first call.
         */
        if (state != 0)
            return NULL;
    }
    return NULL;
}

static char **
completion(const char *text, int start, int end)
{
    rl_attempted_completion_over = 1;
    return rl_completion_matches(text, generator);
}
#endif

int
main(int argc, char *argv[])
{
    int ret;
    RDB_exec_context ec;
    char *envname = NULL;
    char *dbname = "";
    RDB_environment *envp = NULL;
    char *infilename;
#ifndef _WIN32
    struct sigaction sigact;
#endif

    /*
     * Install signal handler to catch Control-C
     */
#ifdef _WIN32
    signal(SIGINT, handle_sigint);
#else
    /* POSIX */
    sigact.sa_handler = &handle_sigint;
    sigemptyset(&sigact.sa_mask);
    sigact.sa_flags = 0;
    if (sigaction(SIGINT, &sigact, NULL) == -1) {
        fprintf(stderr, "sigaction(): %s\n", strerror(errno));
        exit(2);
    }
#endif

    /* Set char/string locales according to the environment */
    setlocale(LC_COLLATE, "");
    setlocale(LC_CTYPE, "");

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
        ret = RDB_open_env(envname, &envp, 0);
        if (ret != RDB_OK) {
            ret = RDB_open_env(envname, &envp, RDB_CREATE);
            if (ret != RDB_OK) {
                fprintf(stderr, "unable to open environment %s: %s\n", envname,
                    db_strerror(errno));
                return 1;
            }
        }
    }

#ifdef USE_READLINE
    rl_attempted_completion_function = completion;
#endif

    RDB_parse_set_read_line_fn(&read_line_interactive);
    RDB_parse_set_free_line_fn(&free_line_interactive);

    RDB_init_exec_context(&ec);

    if (RDB_init_builtin(&ec) != RDB_OK) {
        Duro_print_error(RDB_get_err(&ec));
        goto error;
    }

    if (Duro_init_interp(&interp, &ec, envp, dbname) != RDB_OK) {
        Duro_print_error(RDB_get_err(&ec));
        goto error;
    }

    if (Duro_dt_execute_path(infilename, &interp, &ec) != RDB_OK) {
        Duro_print_error(RDB_get_err(&ec));
        Duro_destroy_interp(&interp);
        goto error;
    }

    Duro_destroy_interp(&interp);

    RDB_destroy_exec_context(&ec);
    return 0;

error:
    RDB_destroy_exec_context(&ec);
    return 1;
}
