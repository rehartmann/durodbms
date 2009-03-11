#ifndef IINTERP_H
#define IINTERP_H

#include <rel/rdb.h>
#include "parse.h"
#include <signal.h>

#define DURO_RETURN (-1000)
#define DURO_LEAVE (-1001)

typedef struct varmap_node {
    RDB_hashmap map;
    struct varmap_node *parentp;
} varmap_node;

void
Duro_print_error(const RDB_object *);

void
Duro_dt_interrupt(void);

int
Duro_dt_execute(RDB_environment *, char *, char *, RDB_exec_context *);

#endif /*IINTERP_H*/
