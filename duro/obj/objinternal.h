/*
 * trinternal.h
 *
 *  Created on: 29.09.2013
 *      Author: rene
 */

#ifndef TRINTERNAL_H_
#define TRINTERNAL_H_

#include "object.h"
#include "type.h"
#include "operator.h"
#include "expression.h"

#include <ltdl.h>

enum {
    /** marks types which have been defined but not implemented */
    RDB_NOT_IMPLEMENTED = -2,
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

typedef struct RDB_transaction RDB_transaction;

int
RDB_sys_select(int argc, RDB_object *[], const char *,
        struct RDB_type *, struct RDB_exec_context *, RDB_transaction *, RDB_object *);

int
RDB_init_builtin_scalar_ops(struct RDB_exec_context *);

int
RDB_add_type(RDB_type *, RDB_exec_context *);

RDB_expression *
RDB_create_unexpr(RDB_expression *, enum RDB_expr_kind,
        RDB_exec_context *);

RDB_expression *
RDB_create_binexpr(RDB_expression *, RDB_expression *,
                    enum RDB_expr_kind, RDB_exec_context *);

RDB_bool
RDB_expr_refers_var(const RDB_expression *, const char *attrname);

RDB_bool
RDB_expr_table_depend(const RDB_expression *, const RDB_object *);

RDB_bool
RDB_expr_expr_depend(const RDB_expression *, const RDB_expression *);

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

#endif /* TRINTERNAL_H_ */
