/*
 * Copyright (C) 2013, 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#ifndef EXPRESSION_H_
#define EXPRESSION_H_

typedef struct RDB_type RDB_type;

enum RDB_expr_kind {
    RDB_EX_OBJ,
    RDB_EX_TBP,

    RDB_EX_VAR,

    /* 3 is no longer used */
    RDB_EX_RO_OP = 4
};

typedef struct RDB_expression RDB_expression;

typedef struct RDB_expr_list {
    RDB_expression *firstp;
    RDB_expression *lastp;
} RDB_expr_list;

RDB_bool
RDB_expr_is_const(const RDB_expression *);

enum RDB_expr_kind
RDB_expr_kind(const RDB_expression *);

RDB_expression *
RDB_bool_to_expr(RDB_bool, RDB_exec_context *);

RDB_expression *
RDB_int_to_expr(RDB_int, RDB_exec_context *);

RDB_expression *
RDB_float_to_expr(RDB_float, RDB_exec_context *);

RDB_expression *
RDB_string_to_expr(const char *, RDB_exec_context *);

int
RDB_del_expr(RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_dup_expr(const RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_ro_op(const char *, RDB_exec_context *);

RDB_expression *
RDB_ro_op_from_expr(RDB_expression *, RDB_exec_context *);

void
RDB_add_arg(RDB_expression *, RDB_expression *);

RDB_expression *
RDB_obj_to_expr(const RDB_object *, RDB_exec_context *);

RDB_expression *
RDB_table_ref(RDB_object *tbp, RDB_exec_context *);

RDB_expression *
RDB_var_ref(const char *varname, RDB_exec_context *);

const char *
RDB_expr_op_name(const RDB_expression *);

RDB_expr_list *
RDB_expr_op_args(RDB_expression *);

const char *
RDB_expr_var_name(const RDB_expression *);

RDB_bool
RDB_expr_is_op(const RDB_expression *, const char *);

RDB_expression *
RDB_eq(RDB_expression *, RDB_expression *, RDB_exec_context *);

RDB_expression *
RDB_tuple_attr(RDB_expression *, const char *attrname, RDB_exec_context *);

int
RDB_expr_attr_qid(const RDB_expression *, RDB_object *, RDB_exec_context *);

RDB_expression *
RDB_expr_property(RDB_expression *, const char *, RDB_exec_context *);

RDB_object *
RDB_expr_obj(RDB_expression *);

void
RDB_set_expr_type(RDB_expression *, RDB_type *);

RDB_bool
RDB_expr_is_table_ref(const RDB_expression *);

RDB_bool
RDB_expr_is_string(const RDB_expression *);

void
RDB_init_expr_list(RDB_expr_list *explistp);

int
RDB_destroy_expr_list(RDB_expr_list *, RDB_exec_context *);

RDB_int
RDB_expr_list_length(const RDB_expr_list *);

void
RDB_expr_list_append(RDB_expr_list *, RDB_expression *);

void
RDB_join_expr_lists(RDB_expr_list *, RDB_expr_list *);

RDB_expression *
RDB_expr_list_get(RDB_expr_list *, int);

#endif /* EXPRESSION_H_ */
