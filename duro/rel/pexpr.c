/*
 * Copyright (C) 2013-2014 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "pexpr.h"
#include "tostr.h"

#include <errno.h>
#include <string.h>

int
RDB_print_expr(const RDB_expression *exp, FILE *fp, RDB_exec_context *ecp)
{
    RDB_object dst;
    int ret;

    RDB_init_obj(&dst);

    /* Convert expression to string */
    if (RDB_expr_to_str(&dst, exp, ecp, NULL, 0) != RDB_OK) {
        RDB_destroy_obj(&dst, ecp);
        return RDB_ERROR;
    }

    /* Write string to output */
    ret = fputs(RDB_obj_string(&dst), fp);

    RDB_destroy_obj(&dst, ecp);
    if (ret == EOF) {
        RDB_raise_system(strerror(errno), ecp);
        return RDB_ERROR;
    }
    return RDB_OK;
}
