/*
 * qr_tclose.h
 *
 *  Created on: 30.09.2012
 *      Author: Rene Hartmann
 */

#ifndef QR_TCLOSE_H_
#define QR_TCLOSE_H_

#include "rdb.h"

int
RDB_tclose_qresult(RDB_qresult *, RDB_expression *, RDB_exec_context *,
        RDB_transaction *);

int
RDB_next_tclose_tuple(RDB_qresult *, RDB_object *,
        RDB_exec_context *, RDB_transaction *);

#endif /* QR_TCLOSE_H_ */
