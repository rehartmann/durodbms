/* $Id$
 *
 * Copyright (C) 2004-2005 René Hartmann.
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

%type <exp> relation project select rename extend summarize wrap unwrap
        group ungroup sdivideby expression or_expression and_expression
        not_expression primary_expression rel_expression add_expression
        mul_expression literal operator_invocation count_invocation
        sum_invocation avg_invocation min_invocation max_invocation
        all_invocation any_invocation extractor tuple_item_list
        ifthenelse summarize_add

%type <explist> expression_list ne_expression_list
        ne_attribute_name_list attribute_name_list
        extend_add_list extend_add
        renaming renaming_list summarize_add_list wrapping wrapping_list

%destructor {
    int i;

    for (i = 0; i < $$.expc; i++) {
        RDB_drop_expr($$.expv[i], _RDB_parse_ecp);
    }
} expression_list ne_expression_list
        ne_attribute_name_list attribute_name_list
        extend_add_list extend_add
        renaming_list renaming
        summarize_add_list wrapping wrapping_list

%destructor {
    RDB_drop_expr($$, _RDB_parse_ecp);
} expression summarize_add

%%

expression: or_expression { _RDB_parse_resultp = $1; }
    | extractor { _RDB_parse_resultp = $1; }
    | ifthenelse { _RDB_parse_resultp = $1; }
    | relation { _RDB_parse_resultp = $1; }
    | project { _RDB_parse_resultp = $1; }
    | select { _RDB_parse_resultp = $1; }
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
        RDB_expression *argv[DURO_MAX_LLEN + 1];

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        for (i = 0; i < $3.expc; i++) {
            argv[i + 1] = $3.expv[i];
        }
        $$ = RDB_ro_op("PROJECT", argc, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < argc; i++)
                RDB_drop_expr(argv[i], _RDB_parse_ecp);
            free(argv);
            YYERROR;
        }
    }
    | expression '{' TOK_ALL TOK_BUT attribute_name_list '}' {
        int i;
        int argc = $5.expc + 1;
        RDB_expression *argv[DURO_MAX_LLEN + 1];

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            for (i = 0; i < $5.expc; i++)
                RDB_drop_expr($5.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        for (i = 0; i < $5.expc; i++) {
            argv[i + 1] = $5.expv[i];
        }
        $$ = RDB_ro_op("REMOVE", argc, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < argc; i++)
                RDB_drop_expr(argv[i], _RDB_parse_ecp);
            free(argv);
            YYERROR;
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

select: expression TOK_WHERE or_expression {
        RDB_expression *texp;

        texp = _RDB_parse_lookup_table($1);
        if (texp == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op_va("WHERE", _RDB_parse_ecp, texp, $3,
                (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

rename: expression TOK_RENAME '(' renaming_list ')' {
        int i;
        RDB_expression *argv[DURO_MAX_LLEN + 1];
        int argc = 1 + $4.expc;
        RDB_expression *exp = _RDB_parse_lookup_table($1);
        if (exp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        argv[0] = exp;
        for (i = 0; i < $4.expc; i++) {
            argv[1 + i] = $4.expv[i];
        }

        $$ = RDB_ro_op("RENAME", argc, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
             for (i = 0; i < argc; i++)
                 RDB_drop_expr(argv[i], _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

renaming_list: renaming {
        $$.expv[0] = $1.expv[0];
        $$.expv[1] = $1.expv[1];
        $$.expc = 2;
    }
    | renaming_list ',' renaming {
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

        $$ = RDB_ro_op_va("UNION", _RDB_parse_ecp, tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("INTERSECT", _RDB_parse_ecp, tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("MINUS", _RDB_parse_ecp, tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("SEMIMINUS", _RDB_parse_ecp, tex1p, tex2p,
                (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("SEMIJOIN", _RDB_parse_ecp, tex1p, tex2p,
                (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("JOIN", _RDB_parse_ecp, tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

extend: TOK_EXTEND expression TOK_ADD '(' extend_add_list ')' {
        int i;
        RDB_expression *argv[DURO_MAX_LLEN + 1];
        int argc = $5.expc + 1;

        argv[0] = _RDB_parse_lookup_table($2);
        if (argv[0] == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            for (i = 0; i < $5.expc; i++)
                RDB_drop_expr($5.expv[i], _RDB_parse_ecp);
            YYERROR;
        }
        for (i = 0; i < $5.expc; i++) {
            argv[1 + i] = $5.expv[i];
        }

        $$ = RDB_ro_op("EXTEND", argc, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < argc; i++)
                RDB_drop_expr(argv[i], _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

extend_add_list: extend_add {
        $$.expv[0] = $1.expv[0];
        $$.expv[1] = $1.expv[1];
        $$.expc = 2;
    }
    | extend_add_list ',' extend_add {
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
        RDB_expression *argv[DURO_MAX_LLEN + 2];

        argv[0] = _RDB_parse_lookup_table($2);
        if (argv[0] == NULL)
        {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            for (i = 0; i < $7.expc; i++) {
                RDB_drop_expr($7.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }

        argv[1] = _RDB_parse_lookup_table($4);
        if (argv[1] == NULL)
        {
            RDB_drop_expr(argv[0], _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            for (i = 0; i < $7.expc; i++) {
                RDB_drop_expr($7.expv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }

        for (i = 0; i < $7.expc; i++) {
            argv[i + 2] = $7.expv[i];
        }
        $$ = RDB_ro_op("SUMMARIZE", argc, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < argc; i++) {
                RDB_drop_expr(argv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("DIVIDE_BY_PER", _RDB_parse_ecp, tex1p, tex2p, tex3p,
                (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            RDB_drop_expr(tex3p, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;    

summarize_add_list: summarize_add {
        $$.expv[0] = $1;
        $$.expc = 1;
    }
    | summarize_add_list ',' summarize_add {
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
        $$.expv[$1.expc] = $3;
    
        $$.expc = $1.expc + 1;
    }
    ;

summarize_add: TOK_COUNT TOK_AS TOK_ID {
        RDB_expression *dummyexp = RDB_ro_op_va("", _RDB_parse_ecp, NULL);
        if (dummyexp == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_expr_aggregate(dummyexp, RDB_COUNT, $3->var.varname, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(dummyexp, _RDB_parse_ecp);
        }
        RDB_drop_expr($3, _RDB_parse_ecp);
    }
    | TOK_SUM '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_sum($3, $6->var.varname, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
        }
        RDB_drop_expr($6, _RDB_parse_ecp);
    }
    | TOK_AVG '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_avg($3, $6->var.varname, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
        }
        RDB_drop_expr($6, _RDB_parse_ecp);
    }
    | TOK_MAX '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_max($3, $6->var.varname, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
        }
        RDB_drop_expr($6, _RDB_parse_ecp);
    }
    | TOK_MIN '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_min($3, $6->var.varname, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
        }
        RDB_drop_expr($6, _RDB_parse_ecp);
    }
    | TOK_ALL '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_all($3, $6->var.varname, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
        }
        RDB_drop_expr($6, _RDB_parse_ecp);
    }
    | TOK_ANY '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_any($3, $6->var.varname, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
        }
        RDB_drop_expr($6, _RDB_parse_ecp);
    }
    ;

wrap: expression TOK_WRAP '(' wrapping_list ')' {
        int i;
        RDB_expression *argv[DURO_MAX_LLEN + 1];

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        for (i = 0; i < $4.expc; i++) {
            argv[1 + i] = $4.expv[i];
        }

        $$ = RDB_ro_op("WRAP", $4.expc + 1, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < $4.expc + 1; i++) {
                RDB_drop_expr(argv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }
    }
    ;

wrapping_list: wrapping {
        $$.expv[0] = $1.expv[0];
        $$.expv[1] = $1.expv[1];
        $$.expc = 2;
    }
    | wrapping_list ',' wrapping {
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
        RDB_expression *argv[DURO_MAX_LLEN + 1];

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        for (i = 0; i < $4.expc; i++) {
            argv[i + 1] = $4.expv[i];
        }
        $$ = RDB_ro_op("UNWRAP", argc, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < argc; i++) {
                RDB_drop_expr(argv[i], _RDB_parse_ecp);
            }
            YYERROR;
        }
    }
    ;

group: expression TOK_GROUP '{' attribute_name_list '}' TOK_AS TOK_ID {
        int i;
        int argc = $4.expc + 2;
        RDB_expression *argv[DURO_MAX_LLEN + 2];

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            RDB_drop_expr($7, _RDB_parse_ecp);
            YYERROR;
        }
        for (i = 0; i < $4.expc; i++) {
            argv[1 + i] = $4.expv[i];
        }
        argv[1 + $4.expc] = RDB_string_to_expr($7->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($7, _RDB_parse_ecp);
        if (argv[1 + $4.expc] == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            for (i = 0; i < $4.expc; i++)
                RDB_drop_expr($4.expv[i], _RDB_parse_ecp);
            YYERROR;
        }
        $$ = RDB_ro_op("GROUP", argc, argv, _RDB_parse_ecp);
        if ($$ == NULL) {
            for (i = 0; i < argc; i++)
                RDB_drop_expr(argv[i], _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

ungroup: expression TOK_UNGROUP TOK_ID {
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        $$ = RDB_ro_op_va("UNGROUP", _RDB_parse_ecp, texp,
                RDB_string_to_expr($3->var.varname, _RDB_parse_ecp),
                (RDB_expression *) NULL);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;    

or_expression: and_expression
    | or_expression TOK_OR and_expression {
        $$ = RDB_ro_op_va("OR", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

and_expression: not_expression
    | and_expression TOK_AND not_expression {
        $$ = RDB_ro_op_va("AND", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

not_expression: rel_expression
    | TOK_NOT rel_expression {
        $$ = RDB_ro_op_va("NOT", _RDB_parse_ecp, $2, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

rel_expression: add_expression
    | add_expression '=' add_expression {
        $$ = RDB_ro_op_va("=", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression TOK_NE add_expression {
        $$ = RDB_ro_op_va("<>", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression TOK_GE add_expression {
        $$ = RDB_ro_op_va(">=", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression TOK_LE add_expression {
        $$ = RDB_ro_op_va("<=", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression '>' add_expression {
        $$ = RDB_ro_op_va(">", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression '<' add_expression {
        $$ = RDB_ro_op_va("<", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression TOK_IN add_expression {
        /* If $1 is a name, try to find a table with that name */
        RDB_expression *exp = _RDB_parse_lookup_table($1);
        if (exp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op_va("IN", _RDB_parse_ecp, exp, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression TOK_MATCHES add_expression {
        $$ = RDB_ro_op_va("MATCHES", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("SUBSET_OF", _RDB_parse_ecp, ex1p, ex2p,
                (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(ex1p, _RDB_parse_ecp);
            RDB_drop_expr(ex2p, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

add_expression: mul_expression
    | '+' mul_expression {
        $$ = $2;
    }
    | '-' mul_expression {
        $$ = RDB_ro_op_va("-", _RDB_parse_ecp, $2, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression '+' mul_expression {
        $$ = RDB_ro_op_va("+", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression '-' mul_expression {
        $$ = RDB_ro_op_va("-", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | add_expression TOK_CONCAT mul_expression {
        $$ = RDB_ro_op_va("||", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }            
    ;

mul_expression: primary_expression
    | mul_expression '*' primary_expression {
        $$ = RDB_ro_op_va("*", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | mul_expression '/' primary_expression {
        $$ = RDB_ro_op_va("/", _RDB_parse_ecp, $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("TO_TUPLE", _RDB_parse_ecp, texp, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            YYERROR;
        }
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

        $$ = RDB_ro_op_va("IF", _RDB_parse_ecp, $2, ex1p, ex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr(ex1p, _RDB_parse_ecp);
            RDB_drop_expr(ex2p, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        RDB_expression *exp = _RDB_parse_lookup_table($3);
        if (exp == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op_va("COUNT", _RDB_parse_ecp, exp, NULL);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
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
            char *varname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_VAR) {
                    RDB_drop_expr(texp, _RDB_parse_ecp);
                    RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
                    RDB_raise_invalid_argument("invalid SUM arguments",
                            _RDB_parse_ecp);
                    YYERROR;
                }
                varname = $3.expv[1]->var.varname;
            }

            $$ = RDB_expr_sum(texp, varname, _RDB_parse_ecp);
            RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
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
            char *varname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);
            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_VAR) {
                    RDB_drop_expr(texp, _RDB_parse_ecp);
                    RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
                    RDB_raise_invalid_argument("invalid AVG arguments",
                           _RDB_parse_ecp);
                    YYERROR;
                }
                varname = $3.expv[1]->var.varname;
            }

            $$ = RDB_expr_avg(texp, varname, _RDB_parse_ecp);
            RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
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
            char *varname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_VAR) {
                    RDB_drop_expr(texp, _RDB_parse_ecp);
                    RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
                    RDB_raise_invalid_argument("invalid MAX arguments",
                           _RDB_parse_ecp);
                    YYERROR;
                }
                varname = $3.expv[1]->var.varname;
            }

            $$ = RDB_expr_max(texp, varname, _RDB_parse_ecp);
            RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
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
            char *varname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_VAR) {
                    RDB_drop_expr(texp, _RDB_parse_ecp);
                    RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
                    RDB_raise_invalid_argument("invalid MIN arguments",
                           _RDB_parse_ecp);
                    YYERROR;
                }
                varname = $3.expv[1]->var.varname;
            }

            $$ = RDB_expr_min(texp, varname, _RDB_parse_ecp);
            RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
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
            char *varname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_VAR) {
                    RDB_drop_expr(texp, _RDB_parse_ecp);
                    RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
                    RDB_raise_invalid_argument("invalid ALL arguments",
                           _RDB_parse_ecp);
                    YYERROR;
                }
                varname = $3.expv[1]->var.varname;
            }

            $$ = RDB_expr_all(texp, varname, _RDB_parse_ecp);
            RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
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
            char *varname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_VAR) {
                    RDB_drop_expr(texp, _RDB_parse_ecp);
                    RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
                    RDB_raise_invalid_argument("invalid ANY arguments",
                           _RDB_parse_ecp);
                    YYERROR;
                }
                varname = $3.expv[1]->var.varname;
            }

            $$ = RDB_expr_any(texp, varname, _RDB_parse_ecp);
            RDB_drop_expr($3.expv[1], _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
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

            $$ = RDB_ro_op($1->var.varname, $3.expc, $3.expv, _RDB_parse_ecp);
            if ($$ == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
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
        RDB_attr *attrv;
        RDB_hashtable_iter hiter;
        tuple_entry *entryp;
        RDB_table *tbp;
        RDB_object obj;
        RDB_object *tplp = RDB_expr_obj($3.expv[0]);

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

        tbp = RDB_create_table(NULL, RDB_FALSE, attrc, attrv, 0, NULL,
                _RDB_parse_ecp, _RDB_parse_txp);
        free(attrv);
        if (tbp == NULL) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        if (RDB_insert(tbp, tplp, _RDB_parse_ecp, _RDB_parse_txp) != RDB_OK) {
            for (i = 0; i < $3.expc; i++)
                RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
            YYERROR;
        }

        for (i = 1; i < $3.expc; i++) {
            tplp = RDB_expr_obj($3.expv[i]);
            if (tplp == NULL) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }
            if (tplp->kind != RDB_OB_TUPLE) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                RDB_raise_type_mismatch("tuple required", _RDB_parse_ecp);
                YYERROR;
            }
            if (RDB_insert(tbp, tplp, _RDB_parse_ecp, _RDB_parse_txp)
                    != RDB_OK) {
                for (i = 0; i < $3.expc; i++)
                    RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
                YYERROR;
            }
        }
        RDB_init_obj(&obj);
        RDB_table_to_obj(&obj, tbp, _RDB_parse_ecp);
        $$ = RDB_obj_to_expr(&obj, _RDB_parse_ecp);

        for (i = 0; i < $3.expc; i++)
            RDB_drop_expr($3.expv[i], _RDB_parse_ecp);
        RDB_destroy_obj(&obj, _RDB_parse_ecp);
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
        ret = RDB_insert(exp->var.obj.var.tbp, &tpl, _RDB_parse_ecp,
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
    | TOK_TUPLE '{' tuple_item_list '}' {
        $$ = $3;
    } 
    | TOK_LIT_STRING
    | TOK_LIT_INTEGER
    | TOK_LIT_FLOAT
    | TOK_LIT_BOOLEAN
    ;

tuple_item_list: TOK_ID expression {
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
        if (ret != RDB_OK) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_obj_to_expr(&obj, _RDB_parse_ecp);
        RDB_destroy_obj(&obj, _RDB_parse_ecp);
        if ($$ == NULL) {
             YYERROR;
        }
    }
    | tuple_item_list ',' TOK_ID expression {
        int ret;
        RDB_object *valp = RDB_expr_obj($4);
        if (valp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            RDB_raise_type_mismatch("", _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_obj_to_expr(RDB_expr_obj($1), _RDB_parse_ecp);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
            
        ret = RDB_tuple_set(RDB_expr_obj($$), $3->var.varname,
                valp, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
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

RDB_table *
RDB_get_ltable(void *arg);

/*
 * Check if exp refers to a table. If yes, destroy exp and return
 * an expression that wraps the table. Otherwise, return exp.
 */
RDB_expression *
_RDB_parse_lookup_table(RDB_expression *exp)
{
    if (exp->kind == RDB_EX_VAR) {
        RDB_table *tbp;

        /* Try to find local table first */
        tbp = (*_RDB_parse_ltfp)(exp->var.varname, _RDB_parse_arg);
        if (tbp != NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            return RDB_table_to_expr(tbp, _RDB_parse_ecp);
        }

        /* Local table not found, try to find global table */
        tbp = RDB_get_table(exp->var.varname, _RDB_parse_ecp,
                _RDB_parse_txp);
        if (tbp != NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            return RDB_table_to_expr(tbp, _RDB_parse_ecp);
        }
    }
    return exp;
}

static RDB_expression *
table_dum_expr(void)
{
     RDB_object obj;
     RDB_table *tbp;
     RDB_expression *exp;

     tbp = RDB_create_table(NULL, RDB_FALSE, 0, NULL, 0, NULL,
             _RDB_parse_ecp, _RDB_parse_txp);
     if (tbp == NULL)
         return NULL;

     RDB_init_obj(&obj);

     RDB_table_to_obj(&obj, tbp, _RDB_parse_ecp);
     exp = RDB_obj_to_expr(&obj, _RDB_parse_ecp);

     RDB_destroy_obj(&obj, _RDB_parse_ecp);
     return exp;
}
