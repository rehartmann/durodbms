#ifndef IINTERP_H
#define IINTERP_H

#include <rel/rdb.h>
#include "parse.h"

int
Duro_init_interp(RDB_exec_context *, const char *);

void
Duro_exit_interp(void);

void
Duro_print_error(const RDB_object *);

void
Duro_dt_interrupt(void);

int
Duro_dt_execute(RDB_environment *, const char *, RDB_exec_context *);

const char*
Duro_dt_prompt(void);

#endif /*IINTERP_H*/
