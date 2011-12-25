/*
 * $Id$
 *
 * Copyright (C) 2004-2011 René Hartmann.
 * See the file COPYING for redistribution information.
 */

%{
#define YYDEBUG 1

#include <ctype.h>
#include <dli/parse.h>
#include <rel/rdb.h>
#include <rel/internal.h>
#include <gen/strfns.h>
#include <gen/hashtabit.h>

#define YYSTYPE RDB_parse_node*

#include <string.h>

extern RDB_parse_node *_RDB_parse_resultp;
extern RDB_exec_context *_RDB_parse_ecp;
extern int yylineno;

#define YYPRINT(file, type, value) RDB_print_parse_node(file, value, _RDB_parse_ecp);

static RDB_expression *
table_dum_expr(void)
{
    RDB_object obj;
    RDB_expression *exp;

    RDB_init_obj(&obj);
    exp = RDB_obj_to_expr(&obj, _RDB_parse_ecp);
    RDB_destroy_obj(&obj, _RDB_parse_ecp);
    if (exp == NULL)
        return NULL;

    if (RDB_init_table(RDB_expr_obj(exp), NULL, 0, NULL, 0, NULL,
            _RDB_parse_ecp) != RDB_OK) {
        RDB_drop_expr(exp, _RDB_parse_ecp);
        return NULL;
    }

    return exp;
}

int
yylex(void);

void
yyerror(const char *);

%}

%error-verbose
%locations

%token TOK_START_EXP TOK_START_STMT
%token TOK_ID "identifier"
%token TOK_LIT_INTEGER "integer literal"
%token TOK_LIT_STRING "character literal"
%token TOK_LIT_FLOAT "floating point literal"
%token TOK_LIT_BOOLEAN "boolean literal"
%token TOK_WHERE "WHERE"
%token TOK_UNION "UNION"
%token TOK_INTERSECT "INTERSECT"
%token TOK_MINUS "MINUS"
%token TOK_SEMIMINUS "SEMIMINUS"
%token TOK_SEMIJOIN "SEMIJOIN"
%token TOK_JOIN "JOIN"
%token TOK_RENAME "RENAME"
%token TOK_EXTEND "EXTEND"
%token TOK_SUMMARIZE "SUMMARIZE"
%token TOK_DIVIDEBY "DIVIDEBY"
%token TOK_WRAP "WRAP"
%token TOK_UNWRAP "UNWRAP"
%token TOK_GROUP "GROUP"
%token TOK_UNGROUP "UNGROUP"
%token TOK_CALL "CALL"
%token TOK_FROM "FROM"
%token TOK_TUPLE "TUPLE"
%token TOK_RELATION "RELATION"
%token TOK_ARRAY "ARRAY"
%token TOK_BUT "BUT"
%token TOK_AS "AS"
%token TOK_PER "PER"
%token TOK_VAR "VAR"
%token TOK_DROP "DROP"
%token TOK_INIT "INIT"
%token TOK_ADD "ADD"
%token TOK_BEGIN "BEGIN"
%token TOK_TX "TRANSACTION"
%token TOK_REAL "REAL"
%token TOK_VIRTUAL "VIRTUAL"
%token TOK_PRIVATE "PRIVATE"
%token TOK_KEY "KEY"
%token TOK_COMMIT "COMMIT"
%token TOK_ROLLBACK "ROLLBACK"
%token TOK_MATCHES "MATCHES"
%token TOK_IN "IN"
%token TOK_SUBSET_OF "SUBSET_OF"
%token TOK_OR "OR"
%token TOK_AND "AND"
%token TOK_NOT "NOT"
%token TOK_CONCAT "||"
%token TOK_NE "<>"
%token TOK_LE "<="
%token TOK_GE ">="
%token TOK_COUNT "COUNT"
%token TOK_SUM "SUM"
%token TOK_AVG "AVG"
%token TOK_MAX "MAX"
%token TOK_MIN "MIN"
%token TOK_ALL "ALL"
%token TOK_ANY "ANY"
%token TOK_SAME_TYPE_AS "SAME_TYPE_AS"
%token TOK_SAME_HEADING_AS "SAME_HEADING_AS"
%token TOK_IF "IF"
%token TOK_THEN "THEN"
%token TOK_ELSE "ELSE"
%token TOK_CASE "CASE"
%token TOK_WHEN "WHEN"
%token TOK_END "END"
%token TOK_FOR "FOR"
%token TOK_TO "TO"
%token TOK_WHILE "WHILE"
%token TOK_LEAVE "LEAVE"
%token TOK_TABLE_DEE "TABLE_DEE"
%token TOK_TABLE_DUM "TABLE_DUM"
%token TOK_ASSIGN ":="
%token TOK_INSERT "INSERT"
%token TOK_DELETE "DELETE"
%token TOK_UPDATE "UPDATE"
%token TOK_TYPE "TYPE"
%token TOK_POSSREP "POSSREP"
%token TOK_CONSTRAINT "CONSTRAINT"
%token TOK_OPERATOR "OPERATOR"
%token TOK_RETURNS "RETURNS"
%token TOK_UPDATES "UPDATES"
%token TOK_RETURN "RETURN"
%token TOK_LOAD "LOAD"
%token TOK_ORDER "ORDER"
%token TOK_ASC "ASC"
%token TOK_DESC "DESC"
%token TOK_WITH "WITH"
%token TOK_RAISE "RAISE"
%token TOK_TRY "TRY"
%token TOK_CATCH "CATCH"
%token TOK_IMPLEMENT "IMPLEMENT"
%token TOK_INVALID "invalid"

%left ':'
%left TOK_FROM TOK_ELSE ','
%left TOK_UNION TOK_MINUS TOK_INTERSECT TOK_SEMIMINUS TOK_JOIN TOK_SEMIJOIN
        TOK_WHERE TOK_RENAME TOK_WRAP TOK_UNWRAP TOK_GROUP TOK_UNGROUP
        TOK_DIVIDEBY TOK_PER '{'
%left TOK_OR
%left TOK_AND
%left TOK_NOT
%left '=' '<' '>' TOK_NE TOK_LE TOK_GE TOK_IN TOK_SUBSET_OF TOK_MATCHES
%left '+' '-' TOK_CONCAT
%left '*' '/'
%left '.' UPLUS UMINUS '['

%%

start: TOK_START_EXP expression {
        _RDB_parse_resultp = $2;
    }
    | TOK_START_STMT statement {
        _RDB_parse_resultp = $2;
    	YYACCEPT;
    }
    | TOK_START_STMT {
        _RDB_parse_resultp = NULL;
    }
    ;

statement: statement_body ';' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }
    | TOK_BEGIN ne_statement_list TOK_END {
        RDB_expression *condp = RDB_bool_to_expr (RDB_TRUE, _RDB_parse_ecp);
        if (condp == NULL)
            YYERROR;
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_IF expression TOK_THEN ne_statement_list TOK_END TOK_IF {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        $$->lineno = yylineno;
    }
    | TOK_IF expression TOK_THEN ne_statement_list TOK_ELSE
            ne_statement_list TOK_END TOK_IF {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        $$->lineno = yylineno;
    }
    | case_opt_semi when_def_list TOK_END TOK_CASE {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }
    | case_opt_semi when_def_list TOK_ELSE
            ne_statement_list TOK_END TOK_CASE {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        $$->lineno = yylineno;
    }
    | TOK_FOR TOK_ID TOK_ASSIGN expression TOK_TO expression ';'
            ne_statement_list TOK_END TOK_FOR {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        RDB_parse_add_child($$, $9);
        RDB_parse_add_child($$, $10);
        $$->lineno = yylineno;
    }
    | TOK_ID ':' TOK_FOR TOK_ID TOK_ASSIGN expression TOK_TO expression ';'
            ne_statement_list TOK_END TOK_FOR {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        RDB_parse_add_child($$, $9);
        RDB_parse_add_child($$, $10);
        RDB_parse_add_child($$, $11);
        RDB_parse_add_child($$, $12);
        $$->lineno = yylineno;
    }
    | TOK_WHILE expression ';' ne_statement_list TOK_END TOK_WHILE {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        $$->lineno = yylineno;
    }
    | TOK_LEAVE TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }
    | TOK_ID ':' TOK_WHILE expression ';' ne_statement_list TOK_END TOK_WHILE {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        $$->lineno = yylineno;
    }
    | TOK_OPERATOR TOK_ID '(' attribute_list ')' TOK_RETURNS type ';'
            ne_statement_list TOK_END TOK_OPERATOR {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        RDB_parse_add_child($$, $9);
        RDB_parse_add_child($$, $10);
        RDB_parse_add_child($$, $11);
        $$->lineno = yylineno;
    }
    | TOK_OPERATOR TOK_ID '(' attribute_list ')' TOK_UPDATES '{' id_list '}' ';'
            ne_statement_list TOK_END TOK_OPERATOR {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        RDB_parse_add_child($$, $9);
        RDB_parse_add_child($$, $10);
        RDB_parse_add_child($$, $11);
        RDB_parse_add_child($$, $12);
        RDB_parse_add_child($$, $13);
        $$->lineno = yylineno;
    }
    | TOK_TRY ne_statement_list ne_catch_def_list TOK_END TOK_TRY {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }

case_opt_semi: TOK_CASE ';'
    | TOK_CASE

when_def: TOK_WHEN expression TOK_THEN ne_statement_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }

when_def_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->lineno = yylineno;
    }
    | when_def_list when_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
    }

possrep_def: TOK_POSSREP '{' attribute_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
	}
	| TOK_POSSREP TOK_ID '{' attribute_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
	}

possrep_def_list: possrep_def {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        $$->lineno = yylineno;
    }
	| possrep_def_list possrep_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
	}

order_item: TOK_ID TOK_ASC {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }
    | TOK_ID TOK_DESC {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }

ne_order_item_list: order_item  {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_order_item_list ',' order_item {
        $$ = $1;
        RDB_parse_add_child($$, $3);
    }

order_item_list: ne_order_item_list {
    }
    | /* Empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
    }

ne_catch_def_list: catch_def {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_catch_def_list catch_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
    }

catch_def: TOK_CATCH TOK_ID type ';' ne_statement_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }
    | TOK_CATCH TOK_ID ';' ne_statement_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }

statement_body: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->lineno = yylineno;
    }
    | TOK_CALL TOK_ID '(' expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }
    | TOK_ID '(' expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID type {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID type TOK_INIT expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_INIT expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_REAL type ne_key_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_REAL type TOK_INIT expression key_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        if ($7 != NULL)
	        RDB_parse_add_child($$, $7);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_REAL TOK_INIT expression key_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        if ($6 != NULL)
	        RDB_parse_add_child($$, $6);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_PRIVATE type ne_key_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_PRIVATE type TOK_INIT expression key_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        if ($7 != NULL)
	        RDB_parse_add_child($$, $7);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_PRIVATE TOK_INIT expression key_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        if ($6 != NULL)
	        RDB_parse_add_child($$, $6);
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID TOK_VIRTUAL expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }
    | TOK_DROP TOK_VAR TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | assignment
    | TOK_BEGIN TOK_TX {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }
    | TOK_COMMIT {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        $$->lineno = yylineno;
    }
    | TOK_ROLLBACK {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        $$->lineno = yylineno;
    }
    | TOK_TYPE TOK_ID possrep_def_list {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_TYPE TOK_ID possrep_def_list TOK_CONSTRAINT expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }
    | TOK_DROP TOK_TYPE TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_DROP TOK_OPERATOR TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_RETURN expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }
    | TOK_RETURN {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        $$->lineno = yylineno;
    }
    | TOK_LOAD TOK_ID TOK_FROM expression TOK_ORDER '(' order_item_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        $$->lineno = yylineno;
    }
    | TOK_CONSTRAINT TOK_ID expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_DROP TOK_CONSTRAINT TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }    
    | TOK_RAISE expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }
    | TOK_IMPLEMENT TOK_TYPE TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }

ne_key_list: TOK_KEY '{' id_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }
    | ne_key_list TOK_KEY '{' id_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }

key_list: /* empty */ {
        $$ = NULL;
    }
    | ne_key_list {
        $$ = $1;
    }

assignment: assign {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | assignment ',' assign {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }

assign: vexpr TOK_ASSIGN expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_INSERT TOK_ID expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }
    | TOK_DELETE TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }
    | TOK_DELETE TOK_ID TOK_WHERE expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }
    | TOK_UPDATE TOK_ID '{' ne_attr_assign_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        $$->lineno = yylineno;
    }
    | TOK_UPDATE TOK_ID TOK_WHERE expression '{' ne_attr_assign_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        $$->lineno = yylineno;
    }

ne_attr_assign_list: simple_assign {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        $$->lineno = yylineno;
    }
	| ne_attr_assign_list ',' simple_assign {
	    $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
	}

simple_assign: TOK_ID TOK_ASSIGN expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        $$->lineno = yylineno;
    }

vexpr: TOK_ID
    | ro_op_invocation
    | vexpr '[' expression ']' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }

ne_statement_list: statement {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        $$->lineno = yylineno;
    }
    | ne_statement_list statement {
    	$$ = $1;
        RDB_parse_add_child($$, $2);
        $$->lineno = yylineno;
    }

expression: expression '{' id_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        $$->lineno = yylineno;
    }
    | expression '{' TOK_ALL TOK_BUT id_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | expression TOK_WHERE expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_RENAME '(' renaming_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_UNION expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_INTERSECT expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_MINUS expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_SEMIMINUS expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_SEMIJOIN expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_JOIN expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_EXTEND expression TOK_ADD '(' name_intro_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_UPDATE expression '{' ne_attr_assign_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_SUMMARIZE expression TOK_PER expression
           TOK_ADD '(' summarize_add_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
    }
    | expression TOK_DIVIDEBY expression TOK_PER expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_WRAP '(' wrapping_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_UNWRAP '(' id_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_GROUP '{' id_list '}' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | expression TOK_UNGROUP TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_OR expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_AND expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_NOT expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | expression '=' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_NE expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_GE expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_LE expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '>' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '<' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_IN expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_MATCHES expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_SUBSET_OF expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | '+' expression %prec UPLUS {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | '-' expression %prec UMINUS {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | expression '+' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '-' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_CONCAT expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }            
    | expression '*' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '/' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | vexpr
    | literal
    | count_invocation
    | sum_invocation
    | avg_invocation
    | max_invocation
    | min_invocation
    | all_invocation
    | any_invocation
    | '(' expression ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '.' TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_ID TOK_FROM expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_TUPLE TOK_FROM expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_IF expression TOK_THEN expression TOK_ELSE expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_WITH name_intro_list ':' expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

id_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
    }
    | ne_id_list
    ;

ne_id_list: TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_id_list ',' TOK_ID {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

renaming_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
    }
    | ne_renaming_list
    ;

ne_renaming_list: renaming {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_renaming_list ',' renaming {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

renaming: TOK_ID TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
/*
    | "PREFIX" STRING AS STRING
    | "SUFFIX" STRING AS STRING
*/
    ;

name_intro_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
    }
    | ne_name_intro_list
    ;

ne_name_intro_list: name_intro {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_name_intro_list ',' name_intro {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

name_intro: expression TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

summarize_add_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
    }
    | ne_summarize_add_list
    ;

ne_summarize_add_list: summarize_add {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_summarize_add_list ',' summarize_add {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

summarize_add: TOK_COUNT '(' ')' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_SUM '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_AVG '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_MAX '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_MIN '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_ALL '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_ANY '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    ;

wrapping_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
    }
    | ne_wrapping_list
    ;

ne_wrapping_list: wrapping {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_wrapping_list ',' wrapping {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

wrapping: '{' id_list '}' TOK_AS TOK_ID {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

sum_invocation: TOK_SUM '(' ne_expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

avg_invocation: TOK_AVG '(' ne_expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

max_invocation: TOK_MAX '(' ne_expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

min_invocation: TOK_MIN '(' ne_expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

all_invocation: TOK_ALL '(' ne_expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

any_invocation: TOK_ANY '(' ne_expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

ro_op_invocation: TOK_ID '(' expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

ne_expression_list: expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_expression_list ',' expression {
        $$ = $1;

        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

literal: TOK_RELATION '{' expression_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
/*     | TOK_RELATION '{' attribute_name_type_list '}'
       '{' expression_list '}' {
    } */
    | TOK_TABLE_DEE {
        int ret;

        RDB_object tpl;
        RDB_expression *exp = table_dum_expr();
        if (exp == NULL) {
            YYERROR;
        }

        RDB_init_obj(&tpl);
        ret = RDB_insert(&exp->var.obj, &tpl, _RDB_parse_ecp, NULL);
        RDB_destroy_obj(&tpl, _RDB_parse_ecp);
        if (ret != RDB_OK) {
            YYERROR;
        }

        $$ = RDB_new_parse_expr(exp, NULL, _RDB_parse_ecp);
		RDB_parse_del_node($1, _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
    }
    | TOK_TABLE_DUM {
        RDB_expression *exp = table_dum_expr();
        if (exp == NULL) {
            YYERROR;
        }
        $$ = RDB_new_parse_expr(exp, NULL, _RDB_parse_ecp);
		RDB_parse_del_node($1, _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
    }
    | TOK_TUPLE '{' '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    } 
    | TOK_TUPLE '{' ne_tuple_item_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    } 
    | TOK_ARRAY '(' expression_list ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_LIT_STRING
    | TOK_LIT_INTEGER
    | TOK_LIT_FLOAT
    | TOK_LIT_BOOLEAN
    ;

ne_tuple_item_list: TOK_ID expression {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | ne_tuple_item_list ',' TOK_ID expression {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

attribute_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
	}
	| ne_attribute_list {
	    $$ = $1;
	}

ne_attribute_list: TOK_ID type {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | ne_attribute_list ',' TOK_ID type {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

type: TOK_ID
    | TOK_TUPLE '{' '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_TUPLE '{' ne_attribute_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_RELATION '{' attribute_list '}' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_ARRAY type {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_SAME_TYPE_AS '(' expression ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
	}
    | TOK_TUPLE TOK_SAME_HEADING_AS '(' expression ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
	}
    | TOK_RELATION TOK_SAME_HEADING_AS '(' expression ')' {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
	}

expression_list: /* empty */ {
        $$ = RDB_new_parse_inner(_RDB_parse_ecp);
        if ($$ == NULL) {
            /* !! Drop $1 ... */
            YYERROR;
        }
    }
    | ne_expression_list
    ;

%%

const char *
_RDB_token_name(int tok)
{
    static char chtok[2];

	switch (tok) {
	    case TOK_WHERE:
	    	return "WHERE";
	    case TOK_UNION:
	        return "UNION";
	    case TOK_INTERSECT:
	        return "INTERSECT";
	    case TOK_MINUS:
	        return "MINUS";
	    case TOK_SEMIMINUS:
	        return "SEMIMINUS";
	    case TOK_SEMIJOIN:
	        return "SEMIJOIN";
	    case TOK_JOIN:
	        return "JOIN";
	    case TOK_RENAME:
	        return "RENAME";
	    case TOK_EXTEND:
	        return "EXTEND";
	    case TOK_SUMMARIZE:
	        return "SUMMARIZE";
	    case TOK_DIVIDEBY:
	        return "DIVIDEBY";
	    case TOK_WRAP:
	        return "WRAP";
	    case TOK_UNWRAP:
	        return "UNWRAP";
	    case TOK_GROUP:
	        return "GROUP";
	    case TOK_UNGROUP:
	        return "UNGROUP";
	    case TOK_CALL:
	        return "CALL";
	    case TOK_FROM:
	        return "FROM";
	    case TOK_TUPLE:
	        return "TUPLE";
	    case TOK_RELATION:
	        return "RELATION";
	    case TOK_ARRAY:
	        return "ARRAY";
	    case TOK_BUT:
	        return "BUT";
	    case TOK_AS:
	        return "AS";
	    case TOK_PER:
	        return "PER";
	    case TOK_VAR:
	        return "VAR";
	    case TOK_DROP:
	        return "DROP";
	    case TOK_INIT:
	        return "INIT";
	    case TOK_ADD:
	        return "ADD";
	    case TOK_BEGIN:
	        return "BEGIN";
	    case TOK_TX:
	        return "TRANSACTION";
	    case TOK_REAL:
	        return "REAL";
	    case TOK_VIRTUAL:
	        return "VIRTUAL";
	    case TOK_PRIVATE:
	        return "PRIVATE";
	    case TOK_KEY:
	        return "KEY";
	    case TOK_COMMIT:
	        return "COMMIT";
	    case TOK_ROLLBACK:
	        return "ROLLBACK";
	    case TOK_MATCHES:
	        return "MATCHES";
	    case TOK_IN:
	        return "IN";
	    case TOK_SUBSET_OF:
	        return "SUBSET_OF";
	    case TOK_OR:
	        return "OR";
	    case TOK_AND:
	        return "AND";
	    case TOK_NOT:
	        return "NOT";
	    case TOK_CONCAT:
	        return "||";
	    case TOK_NE:
	        return "<>";
	    case TOK_LE:
	        return "<=";
	    case TOK_GE:
	        return ">=";
	    case TOK_COUNT:
	        return "COUNT";
	    case TOK_SUM:
	        return "SUM";
	    case TOK_AVG:
	        return "AVG";
	    case TOK_MAX:
	        return "MAX";
	    case TOK_MIN:
	        return "MIN";
	    case TOK_ALL:
	        return "ALL";
	    case TOK_ANY:
	        return "ANY";
	    case TOK_SAME_TYPE_AS:
	        return "SAME_TYPE_AS";
	    case TOK_SAME_HEADING_AS:
	        return "SAME_HEADING_AS";
	    case TOK_IF:
	        return "IF";
	    case TOK_THEN:
	        return "THEN";
	    case TOK_ELSE:
	        return "ELSE";
	    case TOK_CASE:
	        return "CASE";
	    case TOK_WHEN:
	        return "WHEN";
	    case TOK_END:
	        return "END";
	    case TOK_FOR:
	        return "FOR";
	    case TOK_TO:
	        return "TO";
	    case TOK_WHILE:
	        return "WHILE";
	    case TOK_LEAVE:
	        return "LEAVE";
	    case TOK_TABLE_DEE:
	        return "TABLE_DEE";
	    case TOK_TABLE_DUM:
	        return "TABLE_DUM";
	    case TOK_ASSIGN:
	        return ":=";
	    case TOK_INSERT:
	        return "INSERT";
	    case TOK_DELETE:
	        return "DELETE";
	    case TOK_UPDATE:
	        return "UPDATE";
	    case TOK_TYPE:
	        return "TYPE";
	    case TOK_POSSREP:
	        return "POSSREP";
	    case TOK_CONSTRAINT:
	        return "CONSTRAINT";
        case TOK_OPERATOR:
            return "OPERATOR";
        case TOK_RETURNS:
            return "RETURNS";
        case TOK_UPDATES:
            return "UPDATES";
        case TOK_RETURN:
            return "RETURN";
        case TOK_LOAD:
            return "LOAD";
        case TOK_ORDER:
            return "ORDER";
        case TOK_ASC:
            return "ASC";
        case TOK_DESC:
            return "DESC";
        case TOK_WITH:
            return "WITH";
        case TOK_RAISE:
            return "RAISE";
        case TOK_TRY:
            return "TRY";
        case TOK_CATCH:
            return "CATCH";
        case TOK_IMPLEMENT:
         	return "IMPLEMENT";
    }
    chtok[0] = (char) tok;
    chtok[1] = '\0';
  	return chtok;
}
