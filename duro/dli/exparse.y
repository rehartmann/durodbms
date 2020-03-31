/*
 * Grammar definition for Duro D/T.
 *
 * Copyright (C) 2004-2009, 2011-2016 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

/* The two EXPLAIN statements cause shift-reduce conflicts */ 
%expect 2

%{
#define YYDEBUG 1

#include <obj/object.h>
#include <obj/expression.h>
#include <obj/type.h>
#include <dli/parsenode.h>

#define YYSTYPE RDB_parse_node*

extern RDB_parse_node *RDB_parse_resultp;
extern RDB_exec_context *RDB_parse_ecp;
extern int yylineno;

#define YYPRINT(file, type, value) RDB_print_parse_node(file, value, RDB_parse_ecp);

static RDB_expression *
table_dum_expr(void)
{
    RDB_type *reltyp;
    RDB_expression *exp = RDB_ro_op("relation", RDB_parse_ecp);
    if (exp == NULL)
        return NULL; 

    reltyp = RDB_new_relation_type(0, NULL, RDB_parse_ecp);
    if (reltyp == NULL) {
        RDB_del_expr(exp, RDB_parse_ecp);
        return NULL;
    }

    RDB_set_expr_type(exp, reltyp);

    return exp;
}

static RDB_parse_node *
new_parse_inner(void)
{
    RDB_parse_node *nodep = RDB_new_parse_inner(RDB_parse_ecp);
    if (nodep == NULL)
    	return NULL;
    nodep->lineno = yylineno;
    return nodep;
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
%token TOK_LIT_BINARY "hex binary literal"
%token TOK_LIT_FLOAT "floating point literal"
%token TOK_LIT_BOOLEAN "boolean literal"
%token TOK_WHERE "WHERE"
%token TOK_UNION "UNION"
%token TOK_D_UNION "D_UNION"
%token TOK_INTERSECT "INTERSECT"
%token TOK_MINUS "MINUS"
%token TOK_SEMIMINUS "SEMIMINUS"
%token TOK_SEMIJOIN "SEMIJOIN"
%token TOK_MATCHING "MATCHING"
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
%token TOK_CONST "CONST"
%token TOK_INIT "INIT"
%token TOK_BEGIN "BEGIN"
%token TOK_TX "TRANSACTION"
%token TOK_REAL "REAL"
%token TOK_VIRTUAL "VIRTUAL"
%token TOK_PRIVATE "PRIVATE"
%token TOK_PUBLIC "PUBLIC"
%token TOK_KEY "KEY"
%token TOK_DEFAULT "DEFAULT"
%token TOK_COMMIT "COMMIT"
%token TOK_ROLLBACK "ROLLBACK"
%token TOK_IN "IN"
%token TOK_SUBSET_OF "SUBSET_OF"
%token TOK_OR "OR"
%token TOK_AND "AND"
%token TOK_XOR "XOR"
%token TOK_NOT "NOT"
%token TOK_CONCAT "||"
%token TOK_NE "<>"
%token TOK_LE "<="
%token TOK_GE ">="
%token TOK_LIKE "LIKE"
%token TOK_REGEX_LIKE "REGEX_LIKE"
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
%token TOK_D_INSERT "D_INSERT"
%token TOK_DELETE "DELETE"
%token TOK_I_DELETE "I_DELETE"
%token TOK_UPDATE "UPDATE"
%token TOK_TYPE "TYPE"
%token TOK_IS "IS"
%token TOK_POSSREP "POSSREP"
%token TOK_CONSTRAINT "CONSTRAINT"
%token TOK_OPERATOR "OPERATOR"
%token TOK_RETURNS "RETURNS"
%token TOK_UPDATES "UPDATES"
%token TOK_EXTERN "EXTERN"
%token TOK_VERSION "VERSION"
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
%token TOK_ORDERED "ORDERED"
%token TOK_INDEX "INDEX"
%token TOK_EXPLAIN "EXPLAIN"
%token TOK_MAP "MAP"
%token TOK_PACKAGE "PACKAGE"
%token TOK_LIMIT "LIMIT"
%token TOK_INVALID "invalid"

%left TOK_FROM TOK_ELSE ','
%left TOK_UNION TOK_D_UNION TOK_MINUS TOK_INTERSECT TOK_SEMIMINUS TOK_JOIN TOK_SEMIJOIN
        TOK_MATCHING TOK_DIVIDEBY TOK_PER
%left TOK_SUBSET_OF TOK_IN
%left TOK_WHERE '{' TOK_RENAME TOK_WRAP TOK_UNWRAP TOK_GROUP TOK_UNGROUP ':'
%left TOK_OR TOK_XOR
%left TOK_AND
%left TOK_NOT
%left '=' '<' '>' TOK_NE TOK_LE TOK_GE TOK_LIKE TOK_REGEX_LIKE
%left '+' '-' TOK_CONCAT
%left '*' '/' '%'
%left UPLUS UMINUS '['
%left '.'

%destructor {
    if ($$ != NULL) {
	    RDB_parse_del_node($$, RDB_parse_ecp);
	}
} <>

%%

start: TOK_START_EXP expression {
        $$ = $1;
        RDB_parse_resultp = $2;
    }
    | TOK_START_STMT statement {
        $$ = $1;
        RDB_parse_resultp = $2;
    	YYACCEPT;
    }
    | TOK_START_STMT {
        $$ = $1;
        RDB_parse_resultp = NULL;
    }
    ;

statement: assignment ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_CALL TOK_ID '(' expression_commalist ')' ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_raise_internal("Test", RDB_parse_ecp);
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_ID '(' expression_commalist ')' ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_CALL dot_invocation '(' expression_commalist ')' ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_raise_internal("Test", RDB_parse_ecp);
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | dot_invocation '(' expression_commalist ')' ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    |  TOK_VAR TOK_ID type opt_init ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            if ($4 != NULL)
                RDB_parse_del_node($4, RDB_parse_ecp);             
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        if ($4 != NULL)
            RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_VAR TOK_ID TOK_INIT expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_VAR TOK_ID TOK_REAL type opt_init key_list opt_default ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            if ($5 != NULL)
                RDB_parse_del_node($5, RDB_parse_ecp);
   	        RDB_parse_del_node($6, RDB_parse_ecp);
            if ($7 != NULL)
                RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        if ($5 != NULL)
            RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        if ($7 != NULL)
            RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
    }
    | TOK_VAR TOK_ID TOK_REAL TOK_INIT expression key_list opt_default ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            if ($7 != NULL)
               RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        if ($7 != NULL)
            RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
    }
    | TOK_VAR TOK_ID TOK_PRIVATE type opt_init key_list opt_default ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            if ($5 != NULL)
               RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            if ($7 != NULL)
               RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        if ($5 != NULL)
            RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        if ($7 != NULL)
            RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
    }
    | public_var_def
    | TOK_VAR TOK_ID TOK_PRIVATE TOK_INIT expression key_list opt_default ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
	        if ($6 != NULL)
		        RDB_parse_del_node($6, RDB_parse_ecp);
	        if ($7 != NULL)
		        RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        if ($6 != NULL)
	        RDB_parse_add_child($$, $6);
        if ($7 != NULL)
	        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
    }
    | TOK_VAR TOK_ID TOK_VIRTUAL expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | var_drop
    | TOK_CONST TOK_ID expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | type_drop
    | op_drop
    | TOK_DROP TOK_CONSTRAINT TOK_ID ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_RENAME TOK_VAR renaming ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | TOK_BEGIN ne_statement_list TOK_END ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_IF expression TOK_THEN ne_statement_list TOK_END TOK_IF ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | TOK_IF expression TOK_THEN ne_statement_list TOK_ELSE
            ne_statement_list TOK_END TOK_IF ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            YYABORT;
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
    }
    | case_opt_semi when_def_list TOK_END TOK_CASE ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | case_opt_semi when_def_list TOK_ELSE
            ne_statement_list TOK_END TOK_CASE ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | TOK_FOR TOK_ID TOK_ASSIGN expression TOK_TO expression ';'
            ne_statement_list TOK_END TOK_FOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            YYABORT;
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
    }
    | TOK_FOR TOK_ID TOK_IN expression
            TOK_ORDER '(' order_item_commalist ')' ';'
            ne_statement_list TOK_END TOK_FOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            RDB_parse_del_node($12, RDB_parse_ecp);
            RDB_parse_del_node($13, RDB_parse_ecp);
            YYABORT;
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
    }
    | TOK_ID ':' TOK_FOR TOK_ID TOK_IN expression
            TOK_ORDER '(' order_item_commalist ')' ';'
            ne_statement_list TOK_END TOK_FOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            RDB_parse_del_node($12, RDB_parse_ecp);
            RDB_parse_del_node($13, RDB_parse_ecp);
            RDB_parse_del_node($14, RDB_parse_ecp);
            RDB_parse_del_node($15, RDB_parse_ecp);
            YYABORT;
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
        RDB_parse_add_child($$, $14);
        RDB_parse_add_child($$, $15);
    }
    | TOK_ID ':' TOK_FOR TOK_ID TOK_ASSIGN expression TOK_TO expression ';'
            ne_statement_list TOK_END TOK_FOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            RDB_parse_del_node($12, RDB_parse_ecp);
            RDB_parse_del_node($13, RDB_parse_ecp);
            YYABORT;
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
    }
    | TOK_WHILE expression ';' ne_statement_list TOK_END TOK_WHILE ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | TOK_LEAVE TOK_ID ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_LEAVE ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_ID ':' TOK_WHILE expression ';' ne_statement_list TOK_END TOK_WHILE ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            YYABORT;
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
    }
    | ro_op_def     
    | update_op_def
    | TOK_TRY ne_statement_list ne_catch_def_list TOK_END TOK_TRY ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_BEGIN TOK_TX ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_COMMIT ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_ROLLBACK ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | type_def
    | TOK_RETURN expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_RETURN ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_LOAD qualified_id TOK_FROM expression TOK_ORDER '(' order_item_commalist ')' opt_limit ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            if ($9 != NULL) {
                RDB_parse_del_node($9, RDB_parse_ecp);
            }
            RDB_parse_del_node($10, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        if ($9 != NULL) {
            RDB_parse_add_child($$, $9);
        }
        RDB_parse_add_child($$, $10);
    }
    | TOK_CONSTRAINT TOK_ID expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_INDEX TOK_ID TOK_ID '(' ne_id_commalist ')' ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | TOK_DROP TOK_INDEX TOK_ID ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | map_def
    | TOK_EXPLAIN expression TOK_ORDER '(' order_item_commalist ')' ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | TOK_EXPLAIN assignment ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    } 
    | TOK_RAISE expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | type_impl
    | package_def
    | package_impl
    | package_drop
    ;

    pkg_stmt_list: /* empty */ {
        $$ = new_parse_inner();
    }
    | pkg_stmt_list pkg_stmt {
       	$$ = $1;
        RDB_parse_add_child($$, $2);
    }
 
    pkg_stmt: ro_op_def
    | update_op_def
    | public_var_def
    | type_def
    | package_def
    | op_drop
    | type_drop
    | var_drop
    ;

    op_drop: TOK_DROP TOK_OPERATOR TOK_ID ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_DROP TOK_OPERATOR TOK_ID TOK_VERSION TOK_ID ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }

    type_drop: TOK_DROP TOK_TYPE TOK_ID ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

    var_drop: TOK_DROP TOK_VAR TOK_ID ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

    package_def: TOK_PACKAGE TOK_ID ';' pkg_stmt_list TOK_END TOK_PACKAGE ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    ;

    package_impl: TOK_IMPLEMENT TOK_PACKAGE TOK_ID ';' package_impl_stmt_list TOK_END TOK_IMPLEMENT ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            YYABORT;
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

    package_impl_stmt_list: /* empty */ {
        $$ = new_parse_inner();
    }
    | package_impl_stmt_list package_impl_stmt {
       	$$ = $1;
        RDB_parse_add_child($$, $2);
    }
 
    package_impl_stmt: type_impl
    | map_def
    ;

    package_drop: TOK_DROP TOK_PACKAGE qualified_id ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }

    type_impl: TOK_IMPLEMENT TOK_TYPE TOK_ID ';' TOK_END TOK_IMPLEMENT ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | TOK_IMPLEMENT TOK_TYPE TOK_ID TOK_AS type ';' ne_op_def_list
            TOK_END TOK_IMPLEMENT ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            YYABORT;
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
    }
    ;

    map_def: TOK_MAP TOK_ID expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

ordering: /* empty */ {
        $$ = NULL;
    }
    | TOK_ORDERED {
        $$ = $1;
    }
    ;

opt_init: /* empty */ {
        $$ = NULL;
    }
    | TOK_INIT expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    ;

    public_var_def: TOK_VAR TOK_ID TOK_PUBLIC type key_list ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    ;

ro_op_def: TOK_OPERATOR TOK_ID '(' id_type_commalist ')' TOK_RETURNS type opt_version ';'
            statement_list TOK_END TOK_OPERATOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            RDB_parse_del_node($12, RDB_parse_ecp);
            RDB_parse_del_node($13, RDB_parse_ecp);
            YYABORT;
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
    }
    | TOK_OPERATOR TOK_ID '(' id_type_commalist ')' TOK_RETURNS type opt_version
            TOK_EXTERN TOK_LIT_STRING TOK_LIT_STRING ';'
            TOK_END TOK_OPERATOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            RDB_parse_del_node($12, RDB_parse_ecp);
            RDB_parse_del_node($13, RDB_parse_ecp);
            RDB_parse_del_node($14, RDB_parse_ecp);
            RDB_parse_del_node($15, RDB_parse_ecp);
            YYABORT;
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
        RDB_parse_add_child($$, $14);
        RDB_parse_add_child($$, $15);
    }
    ;

update_op_def: TOK_OPERATOR TOK_ID '(' id_type_commalist ')' TOK_UPDATES '{' id_commalist '}'
            opt_version ';' statement_list TOK_END TOK_OPERATOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            RDB_parse_del_node($12, RDB_parse_ecp);
            RDB_parse_del_node($13, RDB_parse_ecp);
            RDB_parse_del_node($14, RDB_parse_ecp);
            RDB_parse_del_node($15, RDB_parse_ecp);
            YYABORT;
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
        RDB_parse_add_child($$, $14);
        RDB_parse_add_child($$, $15);
    }
    | TOK_OPERATOR TOK_ID '(' id_type_commalist ')' TOK_UPDATES '{' id_commalist '}'
            opt_version TOK_EXTERN TOK_LIT_STRING TOK_LIT_STRING ';'
            TOK_END TOK_OPERATOR ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            RDB_parse_del_node($11, RDB_parse_ecp);
            RDB_parse_del_node($12, RDB_parse_ecp);
            RDB_parse_del_node($13, RDB_parse_ecp);
            RDB_parse_del_node($14, RDB_parse_ecp);
            RDB_parse_del_node($15, RDB_parse_ecp);
            RDB_parse_del_node($16, RDB_parse_ecp);
            RDB_parse_del_node($17, RDB_parse_ecp);
            YYABORT;
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
        RDB_parse_add_child($$, $14);
        RDB_parse_add_child($$, $15);
        RDB_parse_add_child($$, $16);
        RDB_parse_add_child($$, $17);
    }    
    ;

opt_version: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL)
            YYABORT;
    }
    | TOK_VERSION TOK_ID {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    ;

type_def: TOK_TYPE TOK_ID supertypes ordering ne_possrep_def_list
        TOK_INIT expression ';'
    {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            if ($4 != NULL)
                RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        if ($4 != NULL)
            RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
    }
    | TOK_TYPE TOK_ID supertypes ordering ne_possrep_def_list
            TOK_CONSTRAINT expression TOK_INIT expression ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            if ($4 != NULL)
                RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            RDB_parse_del_node($9, RDB_parse_ecp);
            RDB_parse_del_node($10, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        if ($4 != NULL)
            RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
        RDB_parse_add_child($$, $8);
        RDB_parse_add_child($$, $9);
        RDB_parse_add_child($$, $10);
    }
    | TOK_TYPE TOK_ID supertypes TOK_UNION ordering ';' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            if ($5 != NULL)
                RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        if ($5 != NULL)
            RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    ;

supertypes: /* Empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | TOK_IS ne_id_commalist {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    ;

/*
 * Type implementation with given actual representation
 * requires at least one operator (the selector)
 */
ne_op_def_list: ro_op_def {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | update_op_def {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_op_def_list ro_op_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
    }
    | ne_op_def_list update_op_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
    }
    ;

case_opt_semi: TOK_CASE ';' {
        $$ = $1;
        RDB_parse_del_node($2, RDB_parse_ecp); /* Ignore ';' */
    }
    | TOK_CASE

when_def: TOK_WHEN expression TOK_THEN ne_statement_list {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }

when_def_list: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | when_def_list when_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
    }

possrep_def: TOK_POSSREP '(' id_type_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
	}
	| TOK_POSSREP TOK_ID '(' id_type_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
	}
    | TOK_POSSREP '{' id_type_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
	}
	| TOK_POSSREP TOK_ID '{' id_type_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
	}

ne_possrep_def_list: possrep_def {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
	| ne_possrep_def_list possrep_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
	}

order_item: TOK_ID TOK_ASC {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_ID TOK_DESC {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }

ne_order_item_commalist: order_item  {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_order_item_commalist ',' order_item {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }

order_item_commalist: ne_order_item_commalist
    | /* Empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }

opt_limit: /* empty */ {
        $$ = NULL;
    }
    | TOK_LIMIT expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    } 

ne_catch_def_list: catch_def {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_catch_def_list catch_def {
        $$ = $1;
        RDB_parse_add_child($$, $2);
    }

catch_def: TOK_CATCH TOK_ID type ';' ne_statement_list {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_CATCH TOK_ID ';' ne_statement_list {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }

ne_key_list: TOK_KEY '{' id_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | ne_key_list TOK_KEY '{' id_commalist '}' {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }

key_list: /* empty */ {
        $$ = new_parse_inner();
    }
    | ne_key_list {
        $$ = $1;
    }

opt_default: /* empty */ {
        $$ = NULL;
    }
    | TOK_DEFAULT '{' tuple_item_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    } 

assignment: assign {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | assignment ',' assign {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }

assign: var_expression TOK_ASSIGN expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_INSERT qualified_id expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_D_INSERT qualified_id expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_DELETE qualified_id {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_DELETE qualified_id TOK_WHERE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_DELETE qualified_id expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_I_DELETE qualified_id expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_UPDATE var_expression '{' ne_id_assign_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_UPDATE var_expression TOK_WHERE expression '{' ne_id_assign_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }

ne_id_assign_commalist: id_assign {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
	| ne_id_assign_commalist ',' id_assign {
	    $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
	}

id_assign: TOK_ID TOK_ASSIGN expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }

var_expression: TOK_ID
    | dot_invocation
    | var_ro_op_invocation /* For THE_ and LENGTH operators */
    | expression '[' expression ']' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

ne_statement_list: statement {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_statement_list statement {
    	$$ = $1;
        RDB_parse_add_child($$, $2);
    }
    ;

statement_list: ne_statement_list
    | /* Empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    ;

expression: expression '{' id_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | expression '{' TOK_ALL TOK_BUT id_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | expression TOK_WHERE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_RENAME '{' renaming_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_UNION expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_D_UNION expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_INTERSECT expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_MINUS expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_SEMIMINUS expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_SEMIJOIN expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_MATCHING expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_NOT TOK_MATCHING expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | expression TOK_JOIN expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_EXTEND expression ':' '{' id_assign_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_UPDATE expression ':' '{' id_assign_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_SUMMARIZE expression TOK_PER expression
           ':' '{' id_assign_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            RDB_parse_del_node($8, RDB_parse_ecp);
            YYABORT;
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
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_WRAP '(' wrapping_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_UNWRAP '(' id_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | expression TOK_GROUP '{' id_commalist '}' TOK_AS TOK_ID {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
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
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_OR expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_AND expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_XOR expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_NOT expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | expression '=' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '<' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '>' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_NE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_LE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_GE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_SUBSET_OF expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_IN expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_LIKE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_REGEX_LIKE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | '+' expression %prec UPLUS {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | '-' expression %prec UMINUS {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | expression '+' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '-' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression TOK_CONCAT expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }            
    | expression '*' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '/' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | expression '%' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | var_expression
    | '(' expression ')' '(' expression_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_RELATION '{' ne_expression_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_RELATION '{' id_type_commalist '}'
           '{' expression_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            RDB_parse_del_node($7, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
        RDB_parse_add_child($$, $7);
    }
    | TOK_TABLE_DEE {
		RDB_expression *argexp;
        RDB_expression *exp = table_dum_expr();
		RDB_parse_del_node($1, RDB_parse_ecp);
        if (exp == NULL) {
            YYABORT;
        }

        /*
         * Create expression which represents a newly initialized RDB_object.
         * In this context, it represents an empty tuple.
         */
        argexp = RDB_ro_op("tuple", RDB_parse_ecp);
        if (argexp == NULL) {
            RDB_del_expr(exp, RDB_parse_ecp);
            YYABORT;
        }
        RDB_add_arg(exp, argexp);

        $$ = RDB_new_parse_expr(exp, NULL, RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_del_expr(exp, RDB_parse_ecp);
            YYABORT;
        }
    }
    | TOK_TABLE_DUM {
        RDB_expression *exp = table_dum_expr();
		RDB_parse_del_node($1, RDB_parse_ecp);
        if (exp == NULL) {
            YYABORT;
        }
        $$ = RDB_new_parse_expr(exp, NULL, RDB_parse_ecp);
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | TOK_TUPLE '{' tuple_item_commalist '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_ARRAY type '(' expression_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    | TOK_ARRAY '(' ne_expression_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | count_invocation
    | agg_invocation
    | '(' expression ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_ID TOK_FROM expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_TUPLE TOK_FROM expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | TOK_IF expression TOK_THEN expression TOK_ELSE expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_WITH '(' id_assign_commalist ')' ':' expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }
    | TOK_LIT_STRING
    | TOK_LIT_BINARY
    | TOK_LIT_INTEGER
    | TOK_LIT_FLOAT
    | TOK_LIT_BOOLEAN
    ;

    dot_invocation: expression '.' TOK_ID {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

id_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | ne_id_commalist
    ;

ne_id_commalist: TOK_ID {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_id_commalist ',' TOK_ID {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

renaming_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | ne_renaming_commalist
    ;

ne_renaming_commalist: renaming {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_renaming_commalist ',' renaming {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

renaming: TOK_ID TOK_AS TOK_ID {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
/* not yet implemented
    | "PREFIX" STRING AS STRING
    | "SUFFIX" STRING AS STRING
*/
    ;

id_assign_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | ne_id_assign_commalist
    ;

agg_op_name: TOK_SUM
	| TOK_AVG
	| TOK_MAX
	| TOK_MIN
	| TOK_ALL
	| TOK_AND
	| TOK_ANY
	| TOK_OR
	;

wrapping_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | ne_wrapping_commalist
    ;

ne_wrapping_commalist: wrapping {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_wrapping_commalist ',' wrapping {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

wrapping: '{' id_commalist '}' TOK_AS TOK_ID {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    /* For SUMMARIZE */
    | TOK_COUNT '(' ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

agg_invocation: agg_op_name '(' expression ')' {
        RDB_parse_node *nodep = new_parse_inner();
        if (nodep == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child(nodep, $3);

        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node(nodep, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, nodep);
        RDB_parse_add_child($$, $4);
    }
    | agg_op_name '(' expression ',' expression ')' {
        RDB_parse_node *nodep = new_parse_inner();
        if (nodep == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child(nodep, $3);
        RDB_parse_add_child(nodep, $4);
        RDB_parse_add_child(nodep, $5);

        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node(nodep, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, nodep);
        RDB_parse_add_child($$, $6);
    }
    ;

var_ro_op_invocation: TOK_ID '(' expression_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | dot_invocation '(' expression_commalist ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

ne_expression_commalist: expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_expression_commalist ',' expression {
        $$ = $1;

        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;


tuple_item_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | ne_tuple_item_commalist
    ;

ne_tuple_item_commalist: TOK_ID expression {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | ne_tuple_item_commalist ',' TOK_ID expression {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

id_type_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
	}
	| ne_id_type_commalist {
	    $$ = $1;
	}

ne_id_type_commalist: TOK_ID type {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | ne_id_type_commalist ',' TOK_ID type {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    ;

id_type_commalist_gen: id_type_commalist {
        $$ = $1;
    }
    | ne_id_type_commalist ',' '*' {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    | '*' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }

type_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | ne_type_commalist {
        $$ = $1;
    }

ne_type_commalist: type {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | ne_type_commalist ',' type {
        $$ = $1;
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

type: qualified_id
    | TOK_TUPLE '{' id_type_commalist_gen '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_RELATION '{' id_type_commalist_gen '}' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
    }
    | TOK_ARRAY type {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
    }
    | TOK_SAME_TYPE_AS '(' expression ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
	}
    | TOK_TUPLE TOK_SAME_HEADING_AS '(' expression ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
	}
    | TOK_RELATION TOK_SAME_HEADING_AS '(' expression ')' {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
	}
	| TOK_OPERATOR '(' type_commalist ')' TOK_RETURNS type {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            RDB_parse_del_node($2, RDB_parse_ecp);
            RDB_parse_del_node($3, RDB_parse_ecp);
            RDB_parse_del_node($4, RDB_parse_ecp);
            RDB_parse_del_node($5, RDB_parse_ecp);
            RDB_parse_del_node($6, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
        RDB_parse_add_child($$, $4);
        RDB_parse_add_child($$, $5);
        RDB_parse_add_child($$, $6);
    }

qualified_id: TOK_ID {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            RDB_parse_del_node($1, RDB_parse_ecp);
            YYABORT;
        }
        RDB_parse_add_child($$, $1);
    }
    | qualified_id '.' TOK_ID {
        $$ = $1;

        RDB_parse_add_child($$, $2);
        RDB_parse_add_child($$, $3);
    }
    ;

expression_commalist: /* empty */ {
        $$ = new_parse_inner();
        if ($$ == NULL) {
            YYABORT;
        }
    }
    | ne_expression_commalist
    ;

%%
