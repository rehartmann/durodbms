/*
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef VIEWOP_H_
#define VIEWOP_H_

#include <fcgiapp.h>
#include <dli/iinterp.h>

enum {
    MOD_PREFIX_LEN = 2       /* Length of "t." */
};

RDB_operator *
Dr_provide_view_op(const char *, Duro_interp *,
        RDB_exec_context *, FCGX_Stream *);

#endif /* VIEWOP_H_ */
