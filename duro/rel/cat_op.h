/*
 * cat_op.h
 *
 *  Created on: 02.09.2012
 *      Author: rene
 */

#ifndef CAT_OP_H_
#define CAT_OP_H_

#include <rel/rdb.h>

int
RDB_cat_load_ro_op(const char *, RDB_exec_context *, RDB_transaction *);

int
RDB_cat_load_upd_op(const char *, RDB_exec_context *, RDB_transaction *);


#endif /* CAT_OP_H_ */
