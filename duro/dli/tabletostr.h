#ifndef RDB_TABLESTOSTR_H
#define RDB_TABLESTOSTR_H

/*
 * Copyright (C) 2004 René Hartmann.
 * See the file COPYING for redistribution information.
 */

/* $Id$ */

#include <rel/rdb.h>

enum {
    RDB_SHOW_INDEX = 1
};

int
_RDB_table_to_str(RDB_object *objp, RDB_table *tbp, int options);

#endif
