/*
 * key.h
 *
 *  Created on: 03.08.2013
 *      Author: Rene Hartmann
 */

#ifndef KEY_H_
#define KEY_H_

int
RDB_check_keys(const RDB_type *, int, const RDB_string_vec[],
        RDB_exec_context *);

int
RDB_all_key(int, const RDB_attr *, RDB_exec_context *,
        RDB_string_vec *);

RDB_bool
RDB_keys_equal(int, const RDB_string_vec[],
        int, const RDB_string_vec[]);

void
RDB_free_keys(int, RDB_string_vec *);

#endif /* KEY_H_ */
