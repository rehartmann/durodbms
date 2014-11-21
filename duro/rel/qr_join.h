/*
 * qr_join.h
 *
 *  Created on: 21.11.2014
 *      Author: Rene Hartmann
 */

#ifndef QR_JOIN_H_
#define QR_JOIN_H_

#include "rdb.h"

int
RDB_next_join(RDB_qresult *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

#endif /* QR_JOIN_H_ */
