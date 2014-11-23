/*
 * qr_stored.h
 *
 *  Created on: 23.11.2014
 *      Author: rene
 */

#ifndef QR_STORED_H_
#define QR_STORED_H_

#include "rdb.h"

int
RDB_init_stored_qresult(RDB_qresult *, RDB_object *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_next_stored_tuple(RDB_qresult *, RDB_object *, RDB_object *,
        RDB_bool, RDB_bool, RDB_type *,
        RDB_exec_context *, RDB_transaction *);

#endif /* QR_STORED_H_ */
