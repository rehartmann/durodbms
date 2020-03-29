/*
 * Copyright (C) 2013 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef OBJINTERNAL_H_
#define OBJINTERNAL_H_

#include "object.h"
#include "type.h"
#include "operator.h"
#include "expression.h"

#ifndef _WIN32
#include <ltdl.h>
#else
#include <winsock2.h>
#include <windows.h>
#endif

enum {
    /** marks types which have been defined but not implemented */
    RDB_NOT_IMPLEMENTED = -2
};

enum RDB_obj_kind
RDB_val_kind(const RDB_type *);

struct RDB_expression {
    enum RDB_expr_kind kind;
    union {
        char *varname;
        RDB_object obj;
        struct {
            RDB_object *tbp;
            struct RDB_tbindex *indexp;
        } tbref;
        struct {
            RDB_expr_list args;
            char *name;

            /* When the operator is stored in a variable or returned by an operator */
            RDB_expression *op;
            struct {
                int objc;
                RDB_expression *stopexp;

                /*
                 * The following fields are only valid if objc > 0
                 * or stopexp != NULL
                 */

                /* Optionally stores the values in objpv */
                RDB_object *objv;

                RDB_object **objpv;
                RDB_bool asc;
                RDB_bool all_eq;
            } optinfo;
        } op;
    } def;

    /*
     * The expression type. NULL if the type has not been determined.
     * If typ is non-scalar, it is destroyed by RDB_drop_expr().
     */
    RDB_type *typ;
    struct RDB_expression *nextp;

    /* RDB_TRUE if the expression has been transformed by RDB_transform(). */
    RDB_bool transformed;
    RDB_bool optimized;
};

struct RDB_op_data {
    char *name;
    RDB_type *rtyp;
    char *version;
    RDB_object source;
#ifdef _WIN32
    HMODULE modhdl;
#else
    lt_dlhandle modhdl;
#endif

    int paramc;
    RDB_parameter *paramv;
    union {
        RDB_upd_op_func *upd_fp;
        RDB_ro_op_func *ro_fp;
    } opfn;
    void *u_data;
    RDB_op_cleanup_func *cleanup_fp;
    RDB_object cretime;
    RDB_bool locked;
};

int
RDB_find_rename_from(int, const RDB_renaming[], const char *);

typedef struct RDB_transaction RDB_transaction;

int
RDB_sys_select(int argc, RDB_object *[], struct RDB_type *,
        struct RDB_exec_context *, RDB_object *);

int
RDB_init_builtin_scalar_ops(struct RDB_exec_context *);

int
RDB_add_type(RDB_type *, RDB_exec_context *);

RDB_expression *
RDB_create_unexpr(RDB_expression *, enum RDB_expr_kind,
        RDB_exec_context *);

RDB_bool
RDB_expr_refers_var(const RDB_expression *, const char *attrname);

int
RDB_invrename_expr(RDB_expression *, RDB_expression *,
        RDB_exec_context *);

int
RDB_resolve_exprnames(RDB_expression **, RDB_expression *,
        RDB_exec_context *);

int
RDB_destroy_expr(RDB_expression *, RDB_exec_context *);

int
RDB_add_selector(RDB_type *, RDB_exec_context *);

int
RDB_copy_expr_typeinfo_if_needed(RDB_expression *, const RDB_expression *,
        RDB_exec_context *);

int
RDB_drop_expr_children(RDB_expression *, RDB_exec_context *);

int
RDB_copy_tuple(RDB_object *dstp, const RDB_object *srcp, RDB_exec_context *);

int
RDB_enlarge_array_buf(RDB_object *, RDB_int, RDB_exec_context *);

int
RDB_del_type(RDB_type *, RDB_exec_context *);

RDB_type *
RDB_obj_impl_type(const RDB_object *);

RDB_bool
RDB_irep_is_string(const RDB_type *);

RDB_bool
RDB_expr_op_is_noarg(const RDB_expression *);

int
RDB_set_str_obj_len(RDB_object *, size_t, RDB_exec_context *);

#endif /* OBJINTERNAL_H_ */
