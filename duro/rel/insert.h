#ifndef INSERT_H
#define INSERT_H

/*
 * $Id$
 *
 * Copyright (C) 2006 René Hartmann.
 * See the file COPYING for redistribution information.
 * 
 * Declares an internal function for inserting tuples into tables.
 */

#include "rdb.h"

int
_RDB_insert_real(RDB_object *tbp, const RDB_object *tplp, RDB_exec_context *,
        RDB_transaction *);

#endif /*INSERT_H*/
