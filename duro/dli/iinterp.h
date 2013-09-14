#ifndef IINTERP_H
#define IINTERP_H

#include <rel/rdb.h>
#include "parse.h"

#include <stdio.h>

int
Duro_init_interp(RDB_exec_context *, const char *);

void
Duro_destroy_interp(void);

void
Duro_print_error(const RDB_object *);

void
Duro_print_error_f(const RDB_object *, FILE *);

void
Duro_dt_interrupt(void);

int
Duro_dt_execute(RDB_environment *, FILE *, RDB_exec_context *);

int
Duro_dt_execute_path(RDB_environment *, const char *,
        RDB_exec_context *);
int
Duro_dt_execute_str(RDB_environment *, const char *, RDB_exec_context *);

const char*
Duro_dt_prompt(void);

#endif /*IINTERP_H*/
