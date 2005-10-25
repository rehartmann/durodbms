#ifndef RDB_TYPEIMPL_H
#define RDB_TYPEIMPL_H

/* $Id$ */

/*
This file is part of Duro, a relational database library.
Copyright (C) 2003 René Hartmann.

Duro is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

Duro is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Duro; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include <rel/rdb.h>

int
RDB_implement_type(const char *name, RDB_type *, RDB_int, RDB_exec_context *,
        RDB_transaction *);

/*
 * Return a pointer to the internal representaion of the value
 * pointed to by valp and store the length of the internal
 * representation in the location pointed to by lenp.
 */
void *
RDB_obj_irep(RDB_object *valp, size_t *lenp);

/*
 * Initialize the value pointed to by valp with the internal
 * representation given by datap and len.
 * Return RDB_OK on success, RDB_NO_MEMORY if allocating memory failed.
 */
int
RDB_irep_to_obj(RDB_object *valp, RDB_type *, const void *datap, size_t len,
        RDB_exec_context *);

#endif
