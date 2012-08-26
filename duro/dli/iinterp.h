#ifndef IINTERP_H
#define IINTERP_H

#include <rel/rdb.h>
#include "parse.h"

void
Duro_print_error(const RDB_object *);

void
Duro_dt_interrupt(void);

int
Duro_dt_execute(RDB_environment *, char *, char *, RDB_exec_context *);

const char*
Duro_dt_prompt(void);

#endif /*IINTERP_H*/
