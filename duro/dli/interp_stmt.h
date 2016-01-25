/*
 * The interpreter main loop
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef INTERP_STMT_H_
#define INTERP_STMT_H_

typedef struct Duro_interp Duro_interp;

typedef struct RDB_exec_context RDB_exec_context;

int
Duro_process_stmt(Duro_interp *, RDB_exec_context *);

#endif /* INTERP_STMT_H_ */
