#ifndef EXECUTE_H
#define EXECUTE_H

#include <rel/rdb.h>
#include "parse.h"

#define DURO_RETURN (-1000)

typedef struct varmap_node {
    RDB_hashmap map;
    struct varmap_node *parentp;
} varmap_node;

extern RDB_environment *envp;

extern varmap_node toplevel_vars;

int
Duro_init_exec(RDB_exec_context *, const char *);

void
Duro_exit_interp(void);

int
Duro_exec_stmt(const RDB_parse_statement *, RDB_exec_context *, RDB_object *);

int
Duro_process_stmt(RDB_exec_context *);

#endif /*EXECUTE_H*/
