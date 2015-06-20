/*
 * Copyright (C) 2014-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef INTERP_VARDEF_H_
#define INTERP_VARDEF_H_

int
Duro_exec_vardef(RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_vardef_private(RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_vardef_public(RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_vardef_real(RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_vardef_virtual(RDB_parse_node *, Duro_interp *, RDB_exec_context *);

int
Duro_exec_constdef(RDB_parse_node *, Duro_interp *,
        RDB_exec_context *);

#endif /* INTERP_VARDEF_H_ */
