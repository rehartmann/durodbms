/*
 * qr_join.h
 *
 *  Created on: 21.11.2014
 *      Author: Rene Hartmann
 */

#ifndef QR_JOIN_H_
#define QR_JOIN_H_

typedef struct RDB_object RDB_object;
typedef struct RDB_exec_context RDB_exec_context;
typedef struct RDB_transaction RDB_transaction;
typedef struct RDB_qresult RDB_qresult;
typedef struct RDB_expression RDB_expression;

int
RDB_join_qresult(RDB_qresult *, RDB_expression *,
        RDB_exec_context *, RDB_transaction *);

int
RDB_next_join(RDB_qresult *, RDB_object *, RDB_exec_context *,
        RDB_transaction *);

#endif /* QR_JOIN_H_ */
