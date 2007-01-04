/* $Id$
 *
 * Copyright (C) 2004-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

%{
#define YYDEBUG 1

#include <dli/parse.h>
#include <rel/rdb.h>
#include <rel/internal.h>
#include <gen/strfns.h>
#include <gen/hashtabit.h>
#include <string.h>

extern RDB_transaction *_RDB_parse_txp;
extern RDB_expression *_RDB_parse_resultp;
extern RDB_parse_statement *_RDB_parse_stmtp;
extern RDB_ltablefn *_RDB_parse_ltfp;
extern void *_RDB_parse_arg;
extern RDB_exec_context *_RDB_parse_ecp;

RDB_expression *
_RDB_parse_lookup_table(RDB_expression *exp);

typedef struct {
    RDB_expression *namexp;
    RDB_type *typ;
} parse_attribute;

static RDB_expression *
table_dum_expr(void);

int
yylex(void);

void
yyerror(const char *);

%}

%error-verbose
%locations

%union {
    RDB_expression *exp;
    struct {
        int expc;
        RDB_expression *expv[DURO_MAX_LLEN];
    } explist;
    RDB_type *type;
    RDB_parse_statement *stmt;
    struct {
        RDB_parse_statement *firstp;
        RDB_parse_statement *lastp;
    } stmtlist;
    struct {
    	RDB_expression *dstp;
    	RDB_expression *srcp;
    } assign;
    parse_attribute attr;
    struct {
        int attrc;
        parse_attribute attrv[DURO_MAX_LLEN];
    } attrlist;
}

%token TOK_START_EXP TOK_START_STMT
%token <exp> TOK_ID TOK_LIT_INTEGER TOK_LIT_STRING TOK_LIT_FLOAT TOK_LIT_BOOLEAN
%token TOK_WHERE TOK_UNION TOK_INTERSECT TOK_MINUS TOK_SEMIMINUS TOK_SEMIJOIN
        TOK_JOIN TOK_RENAME TOK_EXTEND TOK_SUMMARIZE TOK_DIVIDEBY TOK_WRAP
        TOK_UNWRAP TOK_GROUP TOK_UNGROUP
%token TOK_CALL
%token TOK_FROM TOK_TUPLE TOK_RELATION TOK_BUT TOK_AS TOK_PER TOK_VAR TOK_INIT
%token TOK_ADD
%token TOK_MATCHES TOK_IN TOK_SUBSET_OF
%token TOK_OR TOK_AND TOK_NOT
%token TOK_CONCAT
%token TOK_NE TOK_LE TOK_GE
%token TOK_COUNT TOK_SUM TOK_AVG TOK_MAX TOK_MIN TOK_ALL TOK_ANY
%token TOK_IF TOK_THEN TOK_ELSE TOK_END TOK_FOR TOK_TO TOK_WHILE
%token TOK_TABLE_DEE TOK_TABLE_DUM
%token TOK_ASSIGN
%token TOK_INVALID

%type <exp> expression literal operator_invocation count_invocation
        sum_invocation avg_invocation min_invocation max_invocation
        all_invocation any_invocation ne_tuple_item_list

%type <explist> expression_list ne_expression_list
        ne_attribute_name_list attribute_name_list
        extend_add_list ne_extend_add_list extend_add summarize_add
        renaming ne_renaming_list renaming_list
        summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list

%type <stmt> statement statement_body assignment

%type <type> type

%type <stmtlist> ne_statement_list

%type <assign> assign;

%type <attr> attribute

%type <attrlist> ne_attribute_list

%destructor {
    int i;

    for (i = 0; i < $$.expc; i++) {
        RDB_drop_expr($$.expv[i], _RDB_parse_ecp);
    }
} expression_list ne_expression_list
        ne_attribute_name_list attribute_name_list
        extend_add_list ne_extend_add_list extend_add
        ne_renaming_list renaming_list renaming
        summarize_add summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list

%destructor {
    RDB_drop_expr($$, _RDB_parse_ecp);
} expression

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
%left '.' UPLUS UMINUS

%%

start: TOK_START_EXP expression {
        _RDB_parse_resultp = $2;
    }
    | TOK_START_STMT statement {
    	_RDB_parse_stmtp = $2;
    	YYACCEPT;
    }
    | TOK_START_STMT {
        _RDB_parse_stmtp = NULL;
    }
    ;

statement: statement_body ';'
    | TOK_IF expression TOK_THEN ne_statement_list TOK_END TOK_IF {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($4.firstp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_IF;
        $$->var.ifthen.condp = $2;
        $$->var.ifthen.ifp = $4.firstp;
    	$$->var.ifthen.elsep = NULL;
    }
    | TOK_IF expression TOK_THEN ne_statement_list TOK_ELSE
            ne_statement_list TOK_END TOK_IF {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($4.firstp, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($6.firstp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_IF;
        $$->var.ifthen.condp = $2;
        $$->var.ifthen.ifp = $4.firstp;
    	$$->var.ifthen.elsep = $6.firstp;
    }
    | TOK_FOR TOK_ID TOK_ASSIGN expression TOK_TO expression ';'
            ne_statement_list TOK_END TOK_FOR {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_drop_expr($6, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($8.firstp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_FOR;
        $$->var.forloop.varexp = $2;
        $$->var.forloop.fromp = $4;
        $$->var.forloop.top = $6;
        $$->var.forloop.bodyp = $8.firstp;
    }
    | TOK_WHILE expression ';' ne_statement_list TOK_END TOK_WHILE {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($4.firstp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_WHILE;
        $$->var.whileloop.condp = $2;
        $$->var.whileloop.bodyp = $4.firstp;
    }
    ;

statement_body: /* empty */ {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
    	$$->kind = RDB_STMT_NOOP;
    }
    | TOK_CALL TOK_ID '(' expression_list ')' {
    	/* !! */
    }
    | TOK_VAR TOK_ID type {
        int ret;

        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            if (!RDB_type_is_scalar($3)) {
                RDB_drop_type($3, _RDB_parse_ecp, NULL);
            }
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF;
        RDB_init_obj(&$$->var.vardef.varname);
    	ret = RDB_string_to_obj(&$$->var.vardef.varname, $2->var.varname,
    			_RDB_parse_ecp);
    	RDB_drop_expr($2, _RDB_parse_ecp);
    	if (ret != RDB_OK) {
    	    RDB_destroy_obj(&$$->var.vardef.varname, NULL);
    	    YYERROR;
    	}
    	$$->var.vardef.typ = $3;
	   	$$->var.vardef.initexp = NULL;
    }
    | TOK_VAR TOK_ID type TOK_INIT expression {
        int ret;

        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            if (!RDB_type_is_scalar($3)) {
                RDB_drop_type($3, _RDB_parse_ecp, NULL);
            }
            RDB_drop_expr($5, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF;
        RDB_init_obj(&$$->var.vardef.varname);
    	ret = RDB_string_to_obj(&$$->var.vardef.varname, $2->var.varname,
    			_RDB_parse_ecp);
    	RDB_drop_expr($2, NULL);
    	if (ret != RDB_OK) {
    	    RDB_destroy_obj(&$$->var.vardef.varname, NULL);
	    	RDB_drop_expr($5, NULL);
    	    YYERROR;
    	}
    	$$->var.vardef.typ = $3;
	   	$$->var.vardef.initexp = $5;
    }
    | TOK_VAR TOK_ID TOK_INIT expression {
        int ret;

        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF;
        RDB_init_obj(&$$->var.vardef.varname);
    	ret = RDB_string_to_obj(&$$->var.vardef.varname, $2->var.varname,
    			_RDB_parse_ecp);
    	RDB_drop_expr($2, NULL);
    	if (ret != RDB_OK) {
    	    RDB_destroy_obj(&$$->var.vardef.varname, NULL);
	    	RDB_drop_expr($4, NULL);
    	    YYERROR;
    	}
    	$$->var.vardef.typ = NULL;
	   	$$->var.vardef.initexp = $4;
    }
    | TOK_ID expression_list {
        int ret;
        int i;

        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            for (i = 0; i < $2.expc; i++)
                RDB_drop_expr($2.expv[i], _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
    	$$->kind = RDB_STMT_CALL;
        RDB_init_obj(&$$->var.call.opname);
    	ret = RDB_string_to_obj(&$$->var.call.opname, $1->var.varname,
    			_RDB_parse_ecp);
    	RDB_drop_expr($1, NULL);
    	if (ret != RDB_OK) {
    	    RDB_destroy_obj(&$$->var.call.opname, NULL);
    	    YYERROR;
    	}
  	    $$->var.call.argc = $2.expc;
    	for (i = 0; i < $2.expc; i++)
    	    $$->var.call.argv[i] = $2.expv[i];
    }
    | assignment

assignment: assign {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
        	RDB_drop_expr($1.dstp, _RDB_parse_ecp);
        	RDB_drop_expr($1.srcp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_ASSIGN;
        $$->var.assignment.ac = 1;
        $$->var.assignment.av[0].dstp = $1.dstp;
        $$->var.assignment.av[0].srcp = $1.srcp;
    }
    | assignment ',' assign {
        $$ = $1;
        $$->var.assignment.av[$1->var.assignment.ac].dstp = $3.dstp;
        $$->var.assignment.av[$1->var.assignment.ac].srcp = $3.srcp;
        $$->var.assignment.ac++;
    }

assign: TOK_ID TOK_ASSIGN expression {
        $$.dstp = $1;
        $$.srcp = $3;
    }

ne_statement_list: statement {
		$1->nextp = NULL;
		$$.firstp = $1;
		$$.lastp = $1;
    }
    | ne_statement_list statement {
		$2->nextp = NULL;
    	$1.lastp->nextp = $2;
    	$1.lastp = $2;
    	$$ = $1;
    }

expression: expression '{' attribute_name_list '}' {
        int i;
        int argc = $3.expc + 1;
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("PROJECT", argc, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            for (i = 0; i < argc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        for (i = 0; i < $3.expc; i++) {
            RDB_add_arg($$, $3.expv[i]);
        }
    }
    | expression '{' TOK_ALL TOK_BUT attribute_name_list '}' {
        int i;
        int argc = $5.expc + 1;
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            for (i = 0; i < $5.expc; i++) {
                RDB_drop_expr($5.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }

        $$ = RDB_ro_op("REMOVE", argc, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            for (i = 0; i < $5.expc; i++)
                RDB_drop_expr($5.expv[i], _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        for (i = 0; i < $5.expc; i++) {
            RDB_add_arg($$, $5.expv[i]);
        }
    }
    | expression TOK_WHERE expression {
        RDB_expression *texp;

        texp = _RDB_parse_lookup_table($1);
        if (texp == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("WHERE", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_add_arg($$, $3);
    }
    | expression TOK_RENAME '(' renaming_list ')' {
        int i;
        int argc = 1 + $4.expc;
        RDB_expression *exp = _RDB_parse_lookup_table($1);
        if (exp == NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("RENAME", argc, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, exp);
        for (i = 0; i < $4.expc; i++) {
            RDB_add_arg($$, $4.expv[i]);
        }
    }
    | expression TOK_UNION expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL)
        {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("UNION", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
    }
    | expression TOK_INTERSECT expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("INTERSECT", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
    }
    | expression TOK_MINUS expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("MINUS", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
    }
    | expression TOK_SEMIMINUS expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("SEMIMINUS", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
    }
    | expression TOK_SEMIJOIN expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("SEMIJOIN", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
    }
    | expression TOK_JOIN expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("JOIN", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
    }
    | TOK_EXTEND expression TOK_ADD '(' extend_add_list ')' {
        int i;
        int argc = $5.expc + 1;
        RDB_expression *texp = _RDB_parse_lookup_table($2);
        if (texp == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            for (i = 0; i < $5.expc; i++)
                RDB_drop_expr($5.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("EXTEND", argc, _RDB_parse_ecp);
        if ($$ == NULL) {
 	        RDB_drop_expr(texp, _RDB_parse_ecp);
	        for (i = 0; i < $5.expc; i++) {
    	        RDB_drop_expr($5.expv[i], _RDB_parse_ecp);
        	}
            YYERROR;
        }
        RDB_add_arg($$, texp);
        for (i = 0; i < $5.expc; i++) {
            RDB_add_arg($$, $5.expv[i]);
        }
    }
    | TOK_SUMMARIZE expression TOK_PER expression
           TOK_ADD '(' summarize_add_list ')' {
        int i;
        int argc = $7.expc + 2;
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($2);
        if (tex1p == NULL)
        {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            for (i = 0; i < $7.expc; i++) {
                RDB_drop_expr($7.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($4);
        if (tex2p == NULL)
        {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            for (i = 0; i < $7.expc; i++) {
                RDB_drop_expr($7.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }

        $$ = RDB_ro_op("SUMMARIZE", argc, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
	        for (i = 0; i < $7.expc; i++) {
	            RDB_drop_expr($7.expv[i], _RDB_parse_ecp);
        	}
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
        for (i = 0; i < $7.expc; i++) {
	        RDB_add_arg($$, $7.expv[i]);
        }
    }
    | expression TOK_DIVIDEBY expression TOK_PER expression {
        RDB_expression *tex1p, *tex2p, *tex3p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL)
        {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }

        tex3p = _RDB_parse_lookup_table($5);
        if (tex3p == NULL)
        {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("DIVIDE", 3, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            RDB_drop_expr(tex3p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
        RDB_add_arg($$, tex3p);
    }
    | expression TOK_WRAP '(' wrapping_list ')' {
        int i;
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("WRAP", $4.expc + 1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++) {
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }
        RDB_add_arg($$, texp);
        for (i = 0; i < $4.expc; i++) {
            RDB_add_arg($$, $4.expv[i]);
        }
    }
    | expression TOK_UNWRAP '(' attribute_name_list ')' {
        int i;
        int argc = $4.expc + 1;
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("UNWRAP", argc, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        RDB_add_arg($$, texp);
        for (i = 0; i < $4.expc; i++) {
            RDB_add_arg($$, $4.expv[i]);
        }
    }
    | expression TOK_GROUP '{' attribute_name_list '}' TOK_AS TOK_ID {
        int i;
        int argc = $4.expc + 2;
        RDB_expression *lexp;
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            RDB_drop_expr($7, _RDB_parse_ecp);
            YYERROR;
        }
        $$ = RDB_ro_op("GROUP", argc, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            RDB_drop_expr($7, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        for (i = 0; i < $4.expc; i++) {
            RDB_add_arg($$, $4.expv[i]);
        }
        lexp = RDB_string_to_expr($7->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($7, _RDB_parse_ecp);
        if (lexp == NULL) {
            YYERROR;
        }
        RDB_add_arg($$, lexp);
    }
    | expression TOK_UNGROUP TOK_ID {
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        $$ = RDB_ro_op("UNGROUP", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_add_arg($$, RDB_string_to_expr($3->var.varname, _RDB_parse_ecp));
        RDB_drop_expr($3, _RDB_parse_ecp);
    }
    | expression TOK_OR expression {
        $$ = RDB_ro_op("OR", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_AND expression {
        $$ = RDB_ro_op("AND", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | TOK_NOT expression {
        $$ = RDB_ro_op("NOT", 1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $2);
    }
    | expression '=' expression {
        $$ = RDB_ro_op("=", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_NE expression {
        $$ = RDB_ro_op("<>", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_GE expression {
        $$ = RDB_ro_op(">=", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_LE expression {
        $$ = RDB_ro_op("<=", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '>' expression {
        $$ = RDB_ro_op(">", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '<' expression {
        $$ = RDB_ro_op("<", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_IN expression {
        /* If $1 is a name, try to find a table with that name */
        RDB_expression *exp = _RDB_parse_lookup_table($1);
        if (exp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("IN", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, exp);
        RDB_add_arg($$, $3);
    }
    | expression TOK_MATCHES expression {
        $$ = RDB_ro_op("MATCHES", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_SUBSET_OF expression {
        RDB_expression *ex1p, *ex2p;

        ex1p = _RDB_parse_lookup_table($1);
        if (ex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
           YYERROR;
        }

        ex2p = _RDB_parse_lookup_table($3);
        if (ex2p == NULL) {
            RDB_drop_expr(ex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
           YYERROR;
        }

        $$ = RDB_ro_op("SUBSET_OF", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(ex1p, _RDB_parse_ecp);
            RDB_drop_expr(ex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, ex1p);
        RDB_add_arg($$, ex2p);
    }
    | '+' expression %prec UPLUS {
        $$ = $2;
    }
    | '-' expression %prec UMINUS {
        $$ = RDB_ro_op("-", 1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $2);
    }
    | expression '+' expression {
        $$ = RDB_ro_op("+", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '-' expression {
        $$ = RDB_ro_op("-", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_CONCAT expression {
        $$ = RDB_ro_op("||", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }            
    | expression '*' expression {
        $$ = RDB_ro_op("*", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '/' expression {
        $$ = RDB_ro_op("/", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | TOK_ID
    | expression '.' TOK_ID {
        $$ = RDB_tuple_attr($1, $3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$ == NULL)
            RDB_drop_expr($1, _RDB_parse_ecp);
    }
    | literal
    | count_invocation
    | sum_invocation
    | avg_invocation
    | max_invocation
    | min_invocation
    | all_invocation
    | any_invocation
    | operator_invocation
    | '(' expression ')' {
        $$ = $2;
    }
    | TOK_TUPLE TOK_FROM expression {
        RDB_expression *texp;

        texp = _RDB_parse_lookup_table($3);
        if (texp == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("TO_TUPLE", 1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
    }
    | TOK_IF expression TOK_THEN expression TOK_ELSE expression {
        RDB_expression *ex1p, *ex2p;

        ex1p = _RDB_parse_lookup_table($4);
        if (ex1p == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_drop_expr($6, _RDB_parse_ecp);
            YYERROR;
        }
        ex2p = _RDB_parse_lookup_table($6);
        if (ex2p == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr(ex1p, _RDB_parse_ecp);
            RDB_drop_expr($6, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("IF", 3, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr(ex1p, _RDB_parse_ecp);
            RDB_drop_expr(ex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $2);
        RDB_add_arg($$, ex1p);
        RDB_add_arg($$, ex2p);
    }
    ;

attribute_name_list: /* empty */ {
        $$.expc = 0;
    }
    | ne_attribute_name_list
    ;

ne_attribute_name_list: TOK_ID {
        $$.expc = 1;
        $$.expv[0] = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$.expv[0] == NULL) {
            YYERROR;
        }            
    }
    | ne_attribute_name_list ',' TOK_ID {
        int i;

        /* Copy old attributes */
        if ($1.expc >= DURO_MAX_LLEN)
            YYERROR; /* !! ret = RDB_LIMIT_EXCEEDED */
        for (i = 0; i < $1.expc; i++)
            $$.expv[i] = $1.expv[i];
        $$.expv[$1.expc] = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$.expv[0] == NULL) {
            for (i = 0; i < $1.expc; i++)
                RDB_drop_expr($1.expv[i], _RDB_parse_ecp);
            YYERROR;
        }            
        $$.expc = $1.expc + 1;
    }
    ;

renaming_list: /* empty */ {
        $$.expc = 0;
    }
    | ne_renaming_list
    ;

ne_renaming_list: renaming {
        $$.expv[0] = $1.expv[0];
        $$.expv[1] = $1.expv[1];
        $$.expc = 2;
    }
    | ne_renaming_list ',' renaming {
        int i;

        if ($1.expc >= DURO_MAX_LLEN) {
            for (i = 0; i < $1.expc; i++)
                RDB_drop_expr($1.expv[i], _RDB_parse_ecp);
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }
        for (i = 0; i < $1.expc; i++) {
            $$.expv[i] = $1.expv[i];
        }
        $$.expv[$1.expc] = $3.expv[0];
        $$.expv[$1.expc + 1] = $3.expv[1];
        $$.expc = $1.expc + 2;
    }
    ;

renaming: TOK_ID TOK_AS TOK_ID {
            $$.expv[0] = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
            $$.expv[1] = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            if ($$.expv[0] == NULL || $$.expv[1] == NULL) {
                if ($$.expv[0] != NULL)
                    RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
                if ($$.expv[1] != NULL)
                    RDB_drop_expr($$.expv[1], _RDB_parse_ecp);
                YYERROR;
             }
             $$.expc = 2;
        }
/*
        | "PREFIX" STRING AS STRING
        | "SUFFIX" STRING AS STRING
*/
        ;


extend_add_list: /* empty */ {
        $$.expc = 0;
    }
    | ne_extend_add_list
    ;

ne_extend_add_list: extend_add {
        $$.expv[0] = $1.expv[0];
        $$.expv[1] = $1.expv[1];
        $$.expc = 2;
    }
    | ne_extend_add_list ',' extend_add {
        int i;

        if ($$.expc >= DURO_MAX_LLEN) {
            for (i = 0; i < $1.expc; i++) {
                RDB_drop_expr($1.expv[i], _RDB_parse_ecp);
            }
            for (i = 0; i < $3.expc; i++) {
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }

        /* Copy old attributes */
        for (i = 0; i < $1.expc; i++) {
            $$.expv[i] = $1.expv[i];
        }

        /* Add new attribute */
        $$.expv[$1.expc] = $3.expv[0];
        $$.expv[$1.expc + 1] = $3.expv[1];
    
        $$.expc = $1.expc + 2;
    }
    ;

extend_add: expression TOK_AS TOK_ID {
        $$.expv[0] = $1;
        $$.expv[1] = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

summarize_add_list: /* empty */ {
        $$.expc = 0;
    }
    | ne_summarize_add_list
    ;

ne_summarize_add_list: summarize_add {
        $$.expv[0] = $1.expv[0];
        $$.expv[1] = $1.expv[1];
        $$.expc = 2;
    }
    | ne_summarize_add_list ',' summarize_add {
        int i;

        if ($1.expc >= DURO_MAX_LLEN) {
            for (i = 0; i < $1.expc; i++)
                RDB_drop_expr($1.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        /* Copy old elements */
        for (i = 0; i < $1.expc; i++) {
            $$.expv[i] = $1.expv[i];
        }

        /* Add new element */
        $$.expv[$1.expc] = $3.expv[0];
        $$.expv[$1.expc + 1] = $3.expv[1];

        $$.expc = $1.expc + 2;
    }
    ;

summarize_add: TOK_COUNT '(' ')' TOK_AS TOK_ID {
        $$.expv[0] = RDB_ro_op("COUNT", 0, _RDB_parse_ecp);
        if ($$.expv[0] == NULL)
            YYERROR;
        $$.expv[1] = RDB_string_to_expr($5->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($5, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    | TOK_SUM '(' expression ')' TOK_AS TOK_ID {
        $$.expv[0] = RDB_ro_op("SUM", 1, _RDB_parse_ecp);
        if ($$.expv[0] == NULL)
            YYERROR;
        RDB_add_arg($$.expv[0], $3);
        $$.expv[1] = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    | TOK_AVG '(' expression ')' TOK_AS TOK_ID {
        $$.expv[0] = RDB_ro_op("AVG", 1, _RDB_parse_ecp);
        if ($$.expv[0] == NULL)
            YYERROR;
        RDB_add_arg($$.expv[0], $3);
        $$.expv[1] = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    | TOK_MAX '(' expression ')' TOK_AS TOK_ID {
        $$.expv[0] = RDB_ro_op("MAX", 1, _RDB_parse_ecp);
        if ($$.expv[0] == NULL)
            YYERROR;
        RDB_add_arg($$.expv[0], $3);
        $$.expv[1] = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    | TOK_MIN '(' expression ')' TOK_AS TOK_ID {
        $$.expv[0] = RDB_ro_op("MIN", 1, _RDB_parse_ecp);
        if ($$.expv[0] == NULL)
            YYERROR;
        RDB_add_arg($$.expv[0], $3);
        $$.expv[1] = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    | TOK_ALL '(' expression ')' TOK_AS TOK_ID {
        $$.expv[0] = RDB_ro_op("ALL", 1, _RDB_parse_ecp);
        if ($$.expv[0] == NULL)
            YYERROR;
        RDB_add_arg($$.expv[0], $3);
        $$.expv[1] = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    | TOK_ANY '(' expression ')' TOK_AS TOK_ID {
        $$.expv[0] = RDB_ro_op("ANY", 1, _RDB_parse_ecp);
        if ($$.expv[0] == NULL)
            YYERROR;
        RDB_add_arg($$.expv[0], $3);
        $$.expv[1] = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    ;

wrapping_list: /* empty */ {
        $$.expc = 0;
    }
    | ne_wrapping_list
    ;

ne_wrapping_list: wrapping {
        $$.expv[0] = $1.expv[0];
        $$.expv[1] = $1.expv[1];
        $$.expc = 2;
    }
    | ne_wrapping_list ',' wrapping {
        int i;

        if ($1.expc >= DURO_MAX_LLEN) {
            for (i = 0; i < $1.expc + 1; i++) {
                RDB_drop_expr($1.expv[i], _RDB_parse_ecp);
            }
            for (i = 0; i < $3.expc + 1; i++) {
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }

        /* Copy old elements */
        for (i = 0; i < $1.expc; i++) {
            $$.expv[i] = $1.expv[i];
        }

        /* Add new elements */
        $$.expv[$1.expc] = $3.expv[0];
        $$.expv[$1.expc + 1] = $3.expv[1];
    
        $$.expc = $1.expc + 2;
    }
    ;

wrapping: '{' attribute_name_list '}' TOK_AS TOK_ID {
        int i;

        /* Empty object, used to create an expression */
        RDB_object eobj; 

        RDB_init_obj(&eobj);

        $$.expv[0] = RDB_obj_to_expr(&eobj, _RDB_parse_ecp);
        RDB_destroy_obj(&eobj, _RDB_parse_ecp);
        if ($$.expv[0] == NULL) {
            for (i = 0; i < $2.expc; i++) {
                RDB_drop_expr($2.expv[i], _RDB_parse_ecp);
            }
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }

        if (RDB_set_array_length(RDB_expr_obj($$.expv[0]),
                (RDB_int) $2.expc, _RDB_parse_ecp) != RDB_OK) {
            for (i = 0; i < $2.expc; i++) {
                RDB_drop_expr($2.expv[i], _RDB_parse_ecp);
            }
            RDB_drop_expr($5, _RDB_parse_ecp);
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        RDB_expr_obj($$.expv[0])->typ = RDB_create_array_type(&RDB_STRING,
                _RDB_parse_ecp); /* !! */

        for (i = 0; i < $2.expc; i++) {
            if (RDB_array_set(RDB_expr_obj($$.expv[0]),
                    (RDB_int) i, RDB_expr_obj($2.expv[i]),
                    _RDB_parse_ecp) != RDB_OK) {
                for (i = 0; i < $2.expc; i++) {
                    RDB_drop_expr($2.expv[i], _RDB_parse_ecp);
                }
                RDB_drop_expr($5, _RDB_parse_ecp);
                RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
                YYERROR;
            }
        }

        $$.expv[1] = RDB_string_to_expr($5->var.varname, _RDB_parse_ecp);
        for (i = 0; i < $2.expc; i++) {
            RDB_drop_expr($2.expv[i], _RDB_parse_ecp);
        }
        RDB_drop_expr($5, _RDB_parse_ecp);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($$.expv[0], _RDB_parse_ecp);
            YYERROR;
        }
        $$.expc = 2;
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        RDB_expression *exp = _RDB_parse_lookup_table($3);
        if (exp == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("COUNT", 1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, exp);
    }
    ;

sum_invocation: TOK_SUM '(' ne_expression_list ')' {
        int i;

        if ($3.expc > 2) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid SUM arguments",
                    _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("SUM", $3.expc, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if ($3.expc == 2) {
                RDB_add_arg($$, $3.expv[1]);
            }
        }
    }
    ;

avg_invocation: TOK_AVG '(' ne_expression_list ')' {
        int i;

        if ($3.expc > 2) {
            for (i = 0; i < $3.expc; i++) {
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            }
            RDB_raise_invalid_argument("invalid AVG arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("AVG", $3.expc, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if ($3.expc == 2) {
                RDB_add_arg($$, $3.expv[1]);
            }
        }
    }
    ;

max_invocation: TOK_MAX '(' ne_expression_list ')' {
        int i;

        if ($3.expc > 2) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid MAX arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("MAX", $3.expc, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if ($3.expc == 2) {
                RDB_add_arg($$, $3.expv[1]);
            }
        }
    }
    ;

min_invocation: TOK_MIN '(' ne_expression_list ')' {
        int i;

        if ($3.expc > 2) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid MIN arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("MIN", $3.expc, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if ($3.expc == 2) {
                RDB_add_arg($$, $3.expv[1]);
            }
        }
    }
    ;

all_invocation: TOK_ALL '(' ne_expression_list ')' {
        int i;

        if ($3.expc > 2) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid ALL arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("ALL", $3.expc, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if ($3.expc == 2) {
                RDB_add_arg($$, $3.expv[1]);
            }
        }
    }
    ;

any_invocation: TOK_ANY '(' ne_expression_list ')' {
        int i;

        if ($3.expc > 2) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid ANY arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("ANY", $3.expc, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if ($3.expc == 2) {
                RDB_add_arg($$, $3.expv[1]);
            }
        }
    }
    ;

operator_invocation: TOK_ID '(' expression_list ')' {
        if ($3.expc == 1
                && strlen($1->var.varname) > 4
                && strncmp($1->var.varname, "THE_", 4) == 0) {
            /* THE_ operator - requires special treatment */
            $$ = RDB_expr_comp($3.expv[0], $1->var.varname + 4,
                    _RDB_parse_ecp);
            RDB_drop_expr($1, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr($3.expv[0], _RDB_parse_ecp);
                YYERROR;
            }
        } else {
            int i;

            for (i = 0; i < $3.expc; i++) {
                RDB_expression *exp = _RDB_parse_lookup_table($3.expv[i]);
                if (exp == NULL) {
                    for (i = 0; i < $3.expc; i++)
                        RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                    YYERROR;
                }
                $3.expv[i] = exp;
            }
            $$ = RDB_ro_op($1->var.varname, $3.expc, _RDB_parse_ecp);
            RDB_drop_expr($1, _RDB_parse_ecp);
            if ($$ == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }
            for (i = 0; i < $3.expc; i++) {
            	RDB_add_arg($$, $3.expv[i]);
            }
        }
    }
    ;

ne_expression_list: expression {
        $$.expc = 1;
        $$.expv[0] = $1;
    }
    | ne_expression_list ',' expression {
        int i;

        if ($1.expc >= DURO_MAX_LLEN) {
            for (i = 0; i < $1.expc; i++)
                RDB_drop_expr($1.expv[i], _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        for (i = 0; i < $1.expc; i++) {
            $$.expv[i] = $1.expv[i];
        }
        $$.expv[$1.expc] = $3;
        $$.expc = $1.expc + 1;
    }
    ;

literal: TOK_RELATION '{' ne_expression_list '}' {
        int attrc;
        int i;
        int ret;
        RDB_attr *attrv;
        RDB_hashtable_iter hiter;
        tuple_entry *entryp;
        RDB_object obj;
        RDB_object *tplp = RDB_expr_obj($3.expv[0]);

        /*
         * Get type from first tuple
         */
        if (tplp == NULL)
            YYERROR;
        if (tplp->kind != RDB_OB_TUPLE) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            RDB_raise_type_mismatch("tuple required", _RDB_parse_ecp);
            YYERROR;
        }
        attrc = RDB_tuple_size(tplp);
        attrv = malloc(sizeof (RDB_attr) * attrc);
        if (attrv == NULL) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }

        RDB_init_hashtable_iter(&hiter, &tplp->var.tpl_tab);
        for (i = 0; i < attrc; i++) {
            /* Get next attribute */
            entryp = RDB_hashtable_next(&hiter);

            attrv[i].name = entryp->key;
            if (entryp->obj.typ == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                free(attrv);
                RDB_destroy_hashtable_iter(&hiter);
                RDB_raise_not_supported("", _RDB_parse_ecp);
                YYERROR;
            }
            attrv[i].typ = entryp->obj.typ;
            attrv[i].defaultp = NULL;
        }
        RDB_destroy_hashtable_iter(&hiter);

        RDB_init_obj(&obj);
        $$ = RDB_obj_to_expr(&obj, _RDB_parse_ecp);        
        RDB_destroy_obj(&obj, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            free(attrv);
            YYERROR;
        }

        ret = RDB_init_table(RDB_expr_obj($$), NULL, attrc, attrv, 0, NULL,
                _RDB_parse_ecp);
        free(attrv);
        if (ret != RDB_OK) {
            RDB_drop_expr($$, _RDB_parse_ecp);
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        if (RDB_insert(RDB_expr_obj($$), tplp, _RDB_parse_ecp, _RDB_parse_txp)
                != RDB_OK) {
            RDB_drop_expr($$, _RDB_parse_ecp);
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        for (i = 1; i < $3.expc; i++) {
            tplp = RDB_expr_obj($3.expv[i]);
            if (tplp == NULL) {
                RDB_drop_expr($$, _RDB_parse_ecp);
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }
            if (tplp->kind != RDB_OB_TUPLE) {
                RDB_drop_expr($$, _RDB_parse_ecp);
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                RDB_raise_type_mismatch("tuple required", _RDB_parse_ecp);
                YYERROR;
            }
            if (RDB_insert(RDB_expr_obj($$), tplp, _RDB_parse_ecp,
                    _RDB_parse_txp) != RDB_OK) {
                RDB_drop_expr($$, _RDB_parse_ecp);
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }
        }

        for (i = 0; i < $3.expc; i++)
            RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
    }
/*     | TOK_RELATION '{' attribute_name_type_list '}'
       '{' expression_list '}' {
    }
    | TOK_RELATION '{' '}'
       '{' expression_list '}' {
    }
    */ | TOK_TABLE_DEE {
        int ret;

        RDB_object tpl;
        RDB_expression *exp = table_dum_expr();
        if (exp == NULL) {
            YYERROR;
        }

        RDB_init_obj(&tpl);
        ret = RDB_insert(&exp->var.obj, &tpl, _RDB_parse_ecp,
                _RDB_parse_txp);
        RDB_destroy_obj(&tpl, _RDB_parse_ecp);
        if (ret != RDB_OK) {
            YYERROR;
        }

        $$ = exp;
    }
    | TOK_TABLE_DUM {
        RDB_expression *exp = table_dum_expr();
        if (exp == NULL) {
            YYERROR;
        }
        $$ = exp;
    }
    | TOK_TUPLE '{' '}' {
        RDB_object obj;

        RDB_init_obj(&obj);

        $$ = RDB_obj_to_expr(&obj, _RDB_parse_ecp);
        RDB_destroy_obj(&obj, _RDB_parse_ecp);
    } 
    | TOK_TUPLE '{' ne_tuple_item_list '}' {
        $$ = $3;
    } 
    | TOK_LIT_STRING
    | TOK_LIT_INTEGER
    | TOK_LIT_FLOAT
    | TOK_LIT_BOOLEAN
    ;

ne_tuple_item_list: TOK_ID expression {
        RDB_object obj;
        RDB_object *valp;
        int ret;

        RDB_init_obj(&obj);
        valp = RDB_expr_obj($2);
        if (valp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        ret = RDB_tuple_set(&obj, $1->var.varname, valp,
                _RDB_parse_ecp);
        RDB_drop_expr($1, _RDB_parse_ecp);
        RDB_drop_expr($2, _RDB_parse_ecp);
        if (ret != RDB_OK) {
            YYERROR;
        }

        $$ = RDB_obj_to_expr(&obj, _RDB_parse_ecp);
        RDB_destroy_obj(&obj, _RDB_parse_ecp);
        if ($$ == NULL) {
             YYERROR;
        }
    }
    | ne_tuple_item_list ',' TOK_ID expression {
        int ret;
        RDB_object *valp = RDB_expr_obj($4);
        if (valp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_raise_type_mismatch("", _RDB_parse_ecp);
            YYERROR;
        }

		$$ = $1;

        ret = RDB_tuple_set(RDB_expr_obj($$), $3->var.varname,
                valp, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        RDB_drop_expr($4, _RDB_parse_ecp);
        if (ret != RDB_OK)
            YYERROR;
    }
    ;

ne_attribute_list: attribute {
        $$.attrc = 1;
        $$.attrv[0].namexp = $1.namexp;
        $$.attrv[0].typ = $1.typ;
    }
    | ne_attribute_list ',' attribute {
        $$ = $1;
        $$.attrv[$$.attrc].namexp = $3.namexp;
        $$.attrv[$$.attrc].typ = $3.typ;
        $$.attrc++;
    }
    ;

attribute: TOK_ID type {
        $$.namexp = $1;
        $$.typ = $2;
    }
    ;

type: TOK_ID {
		$$ = RDB_get_type($1->var.varname, _RDB_parse_ecp, _RDB_parse_txp);
		if ($$ == NULL)
		    YYERROR;
    }
    | TOK_TUPLE '{' '}' {
        $$ = RDB_create_tuple_type(0, NULL, _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
    }
    | TOK_TUPLE '{' ne_attribute_list '}' {
        int i;
        RDB_attr attrv[DURO_MAX_LLEN];

        for (i = 0; i < $3.attrc; i++) {
            attrv[i].name = $3.attrv[i].namexp->var.varname;
            attrv[i].typ = RDB_dup_nonscalar_type($3.attrv[i].typ,
            		_RDB_parse_ecp);
        }
        $$ = RDB_create_tuple_type($3.attrc, attrv, _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
    }
/*
    | "SAME_TYPE_AS" '(' expression ')'
    | "SAME_HEADING_AS" '(' expression ')'
    | "RELATION" '{' attribute_name_type_list '}'
    | "RELATION" '{' '}'
    ;
*/

expression_list: /* empty */ {
        $$.expc = 0;
    }
    | ne_expression_list
    ;

%%

RDB_object *
RDB_get_ltable(void *arg);

/*
 * Check if exp refers to a table. If yes, destroy exp and return
 * an expression that wraps the table. Otherwise, return exp.
 */
RDB_expression *
_RDB_parse_lookup_table(RDB_expression *exp)
{
    if (exp->kind == RDB_EX_VAR) {
        RDB_object *tbp;

        if (_RDB_parse_ltfp != NULL) {
	        /* Try to find local table first */
	        tbp = (*_RDB_parse_ltfp)(exp->var.varname, _RDB_parse_arg);
	        if (tbp != NULL) {
	            RDB_drop_expr(exp, _RDB_parse_ecp);
	            return RDB_table_ref(tbp, _RDB_parse_ecp);
	        }
        }

        /* Local table not found, try to find global table */
        if (_RDB_parse_txp != NULL) {
	        tbp = RDB_get_table(exp->var.varname, _RDB_parse_ecp,
	                _RDB_parse_txp);
	        if (tbp != NULL) {
	            RDB_drop_expr(exp, _RDB_parse_ecp);
	            return RDB_table_ref(tbp, _RDB_parse_ecp);
	        }
        }
    }
    return exp;
}

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

     if (RDB_init_table(RDB_expr_obj(exp), NULL, 0, NULL, 0, NULL, _RDB_parse_ecp)
             != RDB_OK) {
        RDB_drop_expr(exp, _RDB_parse_ecp);
        return NULL;
     }

     return exp;
}
