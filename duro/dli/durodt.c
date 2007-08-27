/*
 * $Id$
 *
 * Copyright (C) 2007 René Hartmann.
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

#ifdef _WIN32
#define _RDB_EXTERN_VAR __declspec(dllimport)
#else
#define _RDB_EXTERN_VAR extern
#endif

_RDB_EXTERN_VAR FILE *yyin;

void yyrestart(FILE *);

extern int yydebug;

/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

static void
print_error(const RDB_object *errobjp)
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

static void
usage_error(void)
{
    puts("usage: durodt [-e envpath] [-d database] [file]");
    exit(1);
}

int
main(int argc, char *argv[])
{
    int ret;
    RDB_exec_context ec;
    char *envname = NULL;
    char *dbname = "";

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
        yyin = fopen(argv[1], "r");
        if (yyin == NULL) {
            fprintf(stderr, "unable to open %s: %s\n", argv[1],
                    strerror(errno));
            return 1;
        }
    } else {
        yyin = stdin;
    }

    if (envname != NULL) {
        ret = RDB_open_env(envname, &envp);
        if (ret != RDB_OK) {
            fprintf(stderr, "unable to open environment %s: %s\n", envname,
                db_strerror(errno));
            return 1;
        }
    }

    _RDB_parse_interactive = (RDB_bool) isatty(fileno(yyin));

    /* yydebug = 1; */
    err_line = -1;

    RDB_init_exec_context(&ec);

    if (_RDB_init_builtin_types(&ec) != RDB_OK) {
        print_error(RDB_get_err(&ec));
        return 1;
    }

    if (Duro_init_exec(&ec, dbname) != RDB_OK) {
        print_error(RDB_get_err(&ec));
        return 1;
    }

    if (_RDB_parse_interactive) {
        puts("Duro D/T early development version");
        _RDB_parse_init_buf();
    }

    for(;;) {
        if (Duro_process_stmt(&ec) != RDB_OK) {
            RDB_object *errobjp = RDB_get_err(&ec);
            if (errobjp != NULL) {
                if (!_RDB_parse_interactive) {
                    printf("error in statement at or near line %d: ", err_line);
                }
                print_error(errobjp);
                if (_RDB_parse_interactive) {
                    _RDB_parse_init_buf();
                } else {
                    Duro_exit_interp();
                    exit(1);
                }
                RDB_clear_err(&ec);
            } else {
                /* Exit on EOF  */
                puts("");
                Duro_exit_interp();
                exit(0);
            }
        }
    }
}
