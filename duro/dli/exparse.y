/* $Id$
 *
 * Copyright (C) 2004-2006 René Hartmann.
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
extern RDB_ltablefn *_RDB_parse_ltfp;
extern void *_RDB_parse_arg;
extern RDB_exec_context *_RDB_parse_ecp;

RDB_expression *
_RDB_parse_lookup_table(RDB_expression *exp);

static RDB_expression *
table_dum_expr(void);

int
yylex(void);

void
yyerror(const char *);

enum {
    DURO_MAX_LLEN = 200
};

%}

%expect 14

%error-verbose

%union {
    RDB_expression *exp;
    struct {
        int expc;
        RDB_expression *expv[DURO_MAX_LLEN];
    } explist;
}

%token <exp> TOK_ID
%token <exp> TOK_LIT_INTEGER
%token <exp> TOK_LIT_STRING
%token <exp> TOK_LIT_FLOAT
%token <exp> TOK_LIT_BOOLEAN
%token TOK_WHERE
%token TOK_UNION
%token TOK_INTERSECT
%token TOK_MINUS
%token TOK_SEMIMINUS
%token TOK_SEMIJOIN
%token TOK_JOIN
%token TOK_FROM
%token TOK_TUPLE
%token TOK_RELATION
%token TOK_BUT
%token TOK_AS
%token TOK_RENAME
%token TOK_EXTEND
%token TOK_SUMMARIZE
%token TOK_DIVIDEBY
%token TOK_WRAP
%token TOK_UNWRAP
%token TOK_GROUP
%token TOK_UNGROUP
%token TOK_PER
%token TOK_ADD
%token TOK_MATCHES
%token TOK_IN
%token TOK_SUBSET_OF
%token TOK_OR
%token TOK_AND
%token TOK_NOT
%token TOK_CONCAT
%token TOK_NE
%token TOK_LE
%token TOK_GE
%token TOK_COUNT
%token TOK_SUM
%token TOK_AVG
%token TOK_MAX
%token TOK_MIN
%token TOK_ALL
%token TOK_ANY
%token TOK_IF
%token TOK_THEN
%token TOK_ELSE
%token TOK_TABLE_DEE
%token TOK_TABLE_DUM
%token INVALID

%type <exp> relation project where rename extend summarize wrap unwrap
        group ungroup sdivideby expression or_expression and_expression
        not_expression primary_expression rel_expression add_expression
        mul_expression literal operator_invocation count_invocation
        sum_invocation avg_invocation min_invocation max_invocation
        all_invocation any_invocation extractor ne_tuple_item_list
        ifthenelse

%type <explist> expression_list ne_expression_list
        ne_attribute_name_list attribute_name_list
        extend_add_list ne_extend_add_list extend_add summarize_add
        renaming ne_renaming_list renaming_list
        summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list

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

%%

expression: or_expression { _RDB_parse_resultp = $1; }
    | extractor { _RDB_parse_resultp = $1; }
    | ifthenelse { _RDB_parse_resultp = $1; }
    | relation { _RDB_parse_resultp = $1; }
    | project { _RDB_parse_resultp = $1; }
    | where { _RDB_parse_resultp = $1; }
    | rename { _RDB_parse_resultp = $1; }
    | extend { _RDB_parse_resultp = $1; }
    | summarize { _RDB_parse_resultp = $1; }
    | wrap { _RDB_parse_resultp = $1; }
    | unwrap { _RDB_parse_resultp = $1; }
    | group { _RDB_parse_resultp = $1; }
    | ungroup { _RDB_parse_resultp = $1; }
    | sdivideby { _RDB_parse_resultp = $1; }
    ;

project: expression '{' attribute_name_list '}' {
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

where: expression TOK_WHERE or_expression {
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
    ;

rename: expression TOK_RENAME '(' renaming_list ')' {
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

relation: expression TOK_UNION primary_expression {
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
    | expression TOK_INTERSECT primary_expression {
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
    | expression TOK_MINUS primary_expression {
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
    | expression TOK_SEMIMINUS primary_expression {
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
    | expression TOK_SEMIJOIN primary_expression {
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
    | expression TOK_JOIN primary_expression {
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
    ;

extend: TOK_EXTEND expression TOK_ADD '(' extend_add_list ')' {
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

summarize: TOK_SUMMARIZE expression TOK_PER expression
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

sdivideby: expression TOK_DIVIDEBY expression
           TOK_PER primary_expression {
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
    ;    

wrap: expression TOK_WRAP '(' wrapping_list ')' {
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

unwrap: expression TOK_UNWRAP '(' attribute_name_list ')' {
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
    ;

group: expression TOK_GROUP '{' attribute_name_list '}' TOK_AS TOK_ID {
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
    ;

ungroup: expression TOK_UNGROUP TOK_ID {
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
    ;    

or_expression: and_expression
    | or_expression TOK_OR and_expression {
        $$ = RDB_ro_op("OR", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    ;

and_expression: not_expression
    | and_expression TOK_AND not_expression {
        $$ = RDB_ro_op("AND", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    ;

not_expression: rel_expression
    | TOK_NOT rel_expression {
        $$ = RDB_ro_op("NOT", 1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $2);
    }
    ;

rel_expression: add_expression
    | add_expression '=' add_expression {
        $$ = RDB_ro_op("=", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression TOK_NE add_expression {
        $$ = RDB_ro_op("<>", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression TOK_GE add_expression {
        $$ = RDB_ro_op(">=", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression TOK_LE add_expression {
        $$ = RDB_ro_op("<=", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression '>' add_expression {
        $$ = RDB_ro_op(">", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression '<' add_expression {
        $$ = RDB_ro_op("<", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression TOK_IN add_expression {
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
    | add_expression TOK_MATCHES add_expression {
        $$ = RDB_ro_op("MATCHES", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression TOK_SUBSET_OF add_expression {
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
    ;

add_expression: mul_expression
    | '+' mul_expression {
        $$ = $2;
    }
    | '-' mul_expression {
        $$ = RDB_ro_op("-", 1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $2);
    }
    | add_expression '+' mul_expression {
        $$ = RDB_ro_op("+", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression '-' mul_expression {
        $$ = RDB_ro_op("-", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | add_expression TOK_CONCAT mul_expression {
        $$ = RDB_ro_op("||", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }            
    ;

mul_expression: primary_expression
    | mul_expression '*' primary_expression {
        $$ = RDB_ro_op("*", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | mul_expression '/' primary_expression {
        $$ = RDB_ro_op("/", 2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    ;

primary_expression: TOK_ID
    | primary_expression '.' TOK_ID {
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
    ;

extractor: TOK_TUPLE TOK_FROM expression {
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
    ;

ifthenelse: TOK_IF or_expression TOK_THEN add_expression TOK_ELSE add_expression {
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

/*
attribute_name_type_list: attribute_name_type
    | attribute_name_type_list ',' attribute_name_type
    ;

attribute_name_type: TOK_ID type
    ;

type: TOK_ID
    | "SAME_TYPE_AS" '(' expression ')'
    | "RELATION" '{' attribute_name_type_list '}'
    | "RELATION" '{' '}'
    | TOK_TUPLE '{' attribute_name_type_list '}'
    | TOK_TUPLE '{' '}'
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

        /* Try to find local table first */
        tbp = (*_RDB_parse_ltfp)(exp->var.varname, _RDB_parse_arg);
        if (tbp != NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            return RDB_table_ref(tbp, _RDB_parse_ecp);
        }

        /* Local table not found, try to find global table */
        tbp = RDB_get_table(exp->var.varname, _RDB_parse_ecp,
                _RDB_parse_txp);
        if (tbp != NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            return RDB_table_ref(tbp, _RDB_parse_ecp);
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
