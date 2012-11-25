/*
 * cat_type.h
 *
 *  Created on: 02.09.2012
 *      Author: Rene Hartmann
 */

#ifndef CAT_TYPE_H_
#define CAT_TYPE_H_

#include <rel/rdb.h>

int
RDB_cat_get_type(const char *, RDB_exec_context *, RDB_transaction *,
        RDB_type **);

int
RDB_cat_check_type_used(RDB_type *, RDB_exec_context *, RDB_transaction *);

#endif /* CAT_TYPE_H_ */
