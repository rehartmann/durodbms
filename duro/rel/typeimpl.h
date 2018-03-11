#ifndef RDB_TYPEIMPL_H
#define RDB_TYPEIMPL_H

/*
This file is part of Duro, a relational database management system.
Copyright (C) 2003-2005, 2007, 2008, 2012-2015 Rene Hartmann.

Duro is free software; you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
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

#define RDB_GETTER_INFIX "_get_"
#define RDB_SETTER_INFIX "_set_"
#define RDB_COMPARER_SUFFIX "_cmp"

#define RDB_SYS_REP ((RDB_int) -1)

int
RDB_implement_type(const char *, RDB_type *, RDB_int, RDB_exec_context *,
        RDB_transaction *);

void *
RDB_obj_irep(RDB_object *, size_t *);

int
RDB_irep_to_obj(RDB_object *, RDB_type *, const void *, size_t,
        RDB_exec_context *);

RDB_bool
RDB_is_getter(const RDB_operator *);

RDB_bool
RDB_is_setter(const RDB_operator *);

RDB_bool
RDB_is_comparer(const RDB_operator *);

int
RDB_drop_typeimpl_ops(const RDB_type *, RDB_exec_context *,
        RDB_transaction *);

#endif
