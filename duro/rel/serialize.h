#ifndef RDB_SERIALIZE_H
#define RDB_SERIALIZE_H

/* $Id$ */

#include "rdb.h"

int
_RDB_serialize_table(RDB_value *valp, int *posp, RDB_table *tbp);

int
_RDB_deserialize_table(RDB_value *valp, int *posp, RDB_database *dbp,
                       RDB_table **tbpp);

#endif
