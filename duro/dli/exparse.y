/* $Id$ */

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
extern int _RDB_parse_ret;
extern RDB_ltablefn *_RDB_parse_ltfp;
extern void *_RDB_parse_arg;

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

%expect 12

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

%type <explist> expression_list attribute_name_list extend_add_list
        extend_add renaming renaming_list summarize_add_list
        wrapping wrapping_list

%destructor {
    int i;

    for (i = 0; i < $$.expc; i++) {
        RDB_drop_expr($$.expv[i]);
    }
} expression_list attribute_name_list extend_add_list extend_add
        renaming renaming_list summarize_add_list

%destructor {
    RDB_drop_expr($$);
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
        RDB_expression **argv = malloc(sizeof (RDB_expression *) * argc);

        if (argv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        for (i = 0; i < $3.expc; i++) {
            argv[i + 1] = $3.expv[i];
            if (argv[i + 1] == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
        $$ = RDB_ro_op("PROJECT", argc, argv);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | expression '{' TOK_ALL TOK_BUT attribute_name_list '}' {
        int i;
        int argc = $5.expc + 1;
        RDB_expression **argv = malloc(sizeof (RDB_expression *) * argc);

        if (argv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        for (i = 0; i < $5.expc; i++) {
            argv[i + 1] = $5.expv[i];
            if (argv[i + 1] == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
        $$ = RDB_ro_op("REMOVE", argc, argv);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

attribute_name_list: TOK_ID {
        $$.expc = 1;
        $$.expv[0] = RDB_string_to_expr($1->var.attrname);
        if ($$.expv[0] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }            
        RDB_drop_expr($1);
    }
    | attribute_name_list ',' TOK_ID {
        int i;

        /* Copy old attributes */
        if ($1.expc >= DURO_MAX_LLEN)
            YYERROR;
        for (i = 0; i < $1.expc; i++)
            $$.expv[i] = $1.expv[i];
        $$.expv[$1.expc] = RDB_string_to_expr($3->var.attrname);
        if ($$.expv[0] == NULL) {
            /* !! */
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }            
        RDB_drop_expr($3);
        $$.expc = $1.expc + 1;
    }
    ;

select: expression TOK_WHERE or_expression {
        RDB_expression *texp;

        texp = _RDB_parse_lookup_table($1);
        if (texp == NULL)
        {
            YYERROR;
        }

        $$ = RDB_ro_op_va("WHERE", texp, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(texp);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

rename: expression TOK_RENAME '(' renaming_list ')' {
        int i;
        int argc = 1 + $4.expc;
        RDB_expression *exp = _RDB_parse_lookup_table($1);

        RDB_expression **argv = malloc(argc * sizeof (RDB_expression *));
        if (argv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        argv[0] = exp;
        for (i = 0; i < $4.expc; i++) {
            argv[1 + i] = $4.expv[i];
        }

        $$ = RDB_ro_op("RENAME", argc, argv);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
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

            if ($1.expc >= DURO_MAX_LLEN)
                YYERROR;
            for (i = 0; i < $1.expc; i++) {
                $$.expv[i] = $1.expv[i];
            }
            $$.expv[$1.expc] = $3.expv[0];
            $$.expv[$1.expc + 1] = $3.expv[1];
            $$.expc = $1.expc + 2;
        }
        ;

renaming: TOK_ID TOK_AS TOK_ID {
            $$.expv[0] = RDB_string_to_expr($1->var.attrname);
            $$.expv[1] = RDB_string_to_expr($3->var.attrname);
            RDB_drop_expr($1);
            RDB_drop_expr($3);
            if ($$.expv[0] == NULL || $$.expv[1] == NULL) {
                if ($$.expv[0] != NULL)
                    RDB_drop_expr($$.expv[0]);
                if ($$.expv[1] != NULL)
                    RDB_drop_expr($$.expv[1]);
                _RDB_parse_ret = RDB_NO_MEMORY;
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
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        $$ = RDB_ro_op_va("UNION", tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p);
            RDB_drop_expr(tex2p);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | expression TOK_INTERSECT primary_expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        $$ = RDB_ro_op_va("INTERSECT", tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p);
            RDB_drop_expr(tex2p);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | expression TOK_MINUS primary_expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        $$ = RDB_ro_op_va("MINUS", tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p);
            RDB_drop_expr(tex2p);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | expression TOK_JOIN primary_expression {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($1);
        if (tex1p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        $$ = RDB_ro_op_va("JOIN", tex1p, tex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p);
            RDB_drop_expr(tex2p);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

extend: TOK_EXTEND expression TOK_ADD '(' extend_add_list ')' {
        int i;
        int argc = $5.expc + 1;
        RDB_expression **argv = malloc(sizeof (RDB_expression *) * argc);
        if (argv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        argv[0] = _RDB_parse_lookup_table($2);
        if (argv[0] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
        for (i = 0; i < $5.expc; i++) {
            argv[1 + i] = $5.expv[i];
        }

        $$ = RDB_ro_op("EXTEND", argc, argv);
        free(argv);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
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

        if ($$.expc >= DURO_MAX_LLEN)
            YYERROR;

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
        $$.expv[1] = RDB_string_to_expr($3->var.attrname);
        RDB_drop_expr($3);
        if ($$.expv[1] == NULL) {
            RDB_drop_expr($1);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

summarize: TOK_SUMMARIZE expression TOK_PER expression
           TOK_ADD '(' summarize_add_list ')' {
        int i;
        int argc = $7.expc + 2;
        RDB_expression **argv = malloc(sizeof (RDB_expression *) * argc);
        if (argv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
        argv[0] = _RDB_parse_lookup_table($2);
        if (argv[0] == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        argv[1] = _RDB_parse_lookup_table($4);
        if (argv[1] == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        for (i = 0; i < $7.expc; i++) {
            argv[i + 2] = $7.expv[i];
        }
        $$ = RDB_ro_op("SUMMARIZE", argc, argv);
        free(argv);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
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
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($3);
        if (tex2p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        tex3p = _RDB_parse_lookup_table($5);
        if (tex3p == NULL)
        {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        $$ = RDB_ro_op_va("DIVIDE_BY_PER", tex1p, tex2p, tex3p,
                (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p);
            RDB_drop_expr(tex2p);
            RDB_drop_expr(tex3p);
            _RDB_parse_ret = RDB_NO_MEMORY;
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

        if ($1.expc >= DURO_MAX_LLEN)
            YYERROR;

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
        RDB_expression *dummyexp = RDB_ro_op_va("", NULL);
        if (dummyexp == NULL) {
            RDB_drop_expr($3);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        $$ = RDB_expr_aggregate(dummyexp, RDB_COUNT, $3->var.attrname);
    }
    | TOK_SUM '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_sum($3, $6->var.attrname);
    }
    | TOK_AVG '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_avg($3, $6->var.attrname);
    }
    | TOK_MAX '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_max($3, $6->var.attrname);
    }
    | TOK_MIN '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_min($3, $6->var.attrname);
    }
    | TOK_ALL '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_all($3, $6->var.attrname);
    }
    | TOK_ANY '(' expression ')' TOK_AS TOK_ID {
        $$ = RDB_expr_any($3, $6->var.attrname);
    }
    ;

wrap: expression TOK_WRAP '(' wrapping_list ')' {
        int i;

        RDB_expression **argv = malloc(sizeof (RDB_expression *)
                * ($4.expc + 1));
        if (argv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            free(argv);
            YYERROR;
        }

        for (i = 0; i < $4.expc; i++) {
            argv[1 + i] = $4.expv[i];
        }

        $$ = RDB_ro_op("WRAP", $4.expc + 1, argv);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            /* !! */
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

        if ($1.expc >= DURO_MAX_LLEN)
            YYERROR;

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

        $$.expv[0] = RDB_obj_to_expr(&eobj);
        RDB_destroy_obj(&eobj);
        if ($$.expv[0] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        _RDB_parse_ret = RDB_set_array_length(RDB_expr_obj($$.expv[0]),
                (RDB_int) $2.expc);
        if (_RDB_parse_ret != RDB_OK) {
            RDB_drop_expr($$.expv[0]);
            YYERROR;
        }

        for (i = 0; i < $2.expc; i++) {
            if (_RDB_parse_ret != RDB_OK) {
                /* !! */
                YYERROR;
            }
            _RDB_parse_ret = RDB_array_set(RDB_expr_obj($$.expv[0]),
                    (RDB_int) i, RDB_expr_obj($2.expv[i]));
            if (_RDB_parse_ret != RDB_OK) {
                /* !! */
                YYERROR;
            }
        }

        $$.expv[1] = RDB_string_to_expr($5->var.attrname);
        RDB_drop_expr($5);

        $$.expc = 2;
    }
    ;

unwrap: expression TOK_UNWRAP '(' attribute_name_list ')' {
        int i;
        int argc = $4.expc + 1;
        RDB_expression **argv = malloc(sizeof (RDB_expression *) * argc);

        if (argv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        for (i = 0; i < $4.expc; i++) {
            argv[i + 1] = $4.expv[i];
            if (argv[i + 1] == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
        $$ = RDB_ro_op("UNWRAP", argc, argv);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

group: expression TOK_GROUP '{' attribute_name_list '}' TOK_AS TOK_ID {
        int i;
        int argc = $4.expc + 2;
        RDB_expression **argv = malloc(sizeof (RDB_expression *) * argc);

        argv[0] = _RDB_parse_lookup_table($1);
        if (argv[0] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
        for (i = 0; i < $4.expc; i++) {
            argv[1 + i] = $4.expv[i];
            if (argv[1 + i] == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
        argv[1 + $4.expc] = RDB_string_to_expr($7->var.attrname);
        if (argv[1 + $4.expc] == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
        $$ = RDB_ro_op("GROUP", argc, argv);
        if ($$ == NULL) {
            for (i = 0; i < argc; i++)
                RDB_drop_expr(argv[i]);
            free(argv);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
        free(argv);
    }
    ;

ungroup: expression TOK_UNGROUP TOK_ID {
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
        $$ = RDB_ro_op_va("UNGROUP", texp,
                RDB_string_to_expr($3->var.attrname), (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;    

or_expression: and_expression
    | or_expression TOK_OR and_expression {
        $$ = RDB_ro_op_va("OR", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

and_expression: not_expression
    | and_expression TOK_AND not_expression {
        $$ = RDB_ro_op_va("AND", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

not_expression: rel_expression
    | TOK_NOT rel_expression {
        $$ = RDB_ro_op_va("NOT", $2, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

rel_expression: add_expression
    | add_expression '=' add_expression {
        $$ = RDB_ro_op_va("=", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL)
            YYERROR;
    }
    | add_expression TOK_NE add_expression {
        $$ = RDB_ro_op_va("<>", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression TOK_GE add_expression {
        $$ = RDB_ro_op_va(">=", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression TOK_LE add_expression {
        $$ = RDB_ro_op_va("<=", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression '>' add_expression {
        $$ = RDB_ro_op_va(">", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression '<' add_expression {
        $$ = RDB_ro_op_va("<", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression TOK_IN add_expression {
        /* If $1 is a name, try to find a table with that name */
        RDB_expression *exp = _RDB_parse_lookup_table($1);;

        $$ = RDB_ro_op_va("IN", exp, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression TOK_MATCHES add_expression {
        $$ = RDB_ro_op_va("MATCHES", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression TOK_SUBSET_OF add_expression {
        RDB_expression *ex1p, *ex2p;

        ex1p = _RDB_parse_lookup_table($1);
        if (ex1p == NULL) {
           _RDB_parse_ret = RDB_NO_MEMORY;
           YYERROR;
        }

        ex2p = _RDB_parse_lookup_table($3);
        if (ex2p == NULL) {
           _RDB_parse_ret = RDB_NO_MEMORY;
           YYERROR;
        }

        $$ = RDB_ro_op_va("SUBSET_OF", ex1p, ex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

add_expression: mul_expression
    | '+' mul_expression {
        $$ = $2;
    }
    | '-' mul_expression {
        $$ = RDB_ro_op_va("-", $2, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression '+' mul_expression {
        $$ = RDB_ro_op_va("+", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression '-' mul_expression {
        $$ = RDB_ro_op_va("-", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | add_expression TOK_CONCAT mul_expression {
        $$ = RDB_ro_op_va("||", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }            
    ;

mul_expression: primary_expression
    | mul_expression '*' primary_expression {
        $$ = RDB_ro_op_va("*", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | mul_expression '/' primary_expression {
        $$ = RDB_ro_op_va("/", $1, $3, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

primary_expression: TOK_ID
    | primary_expression '.' TOK_ID {
        $$ = RDB_tuple_attr($1, $3->var.attrname);
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
        if (texp == NULL)
            YYERROR;

        $$ = RDB_ro_op_va("TO_TUPLE", texp, (RDB_expression *) NULL);
        if ($$ == NULL) {
            RDB_drop_expr(texp);
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

ifthenelse: TOK_IF or_expression TOK_THEN add_expression TOK_ELSE add_expression {
        RDB_expression *ex1p, *ex2p;

        ex1p = _RDB_parse_lookup_table($4);
        if (ex1p == NULL) {
            YYERROR;
        }
        ex2p = _RDB_parse_lookup_table($6);
        if (ex2p == NULL) {
            RDB_drop_expr(ex1p);
            YYERROR;
        }

        $$ = RDB_ro_op_va("IF", $2, ex1p, ex2p, (RDB_expression *) NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        RDB_expression *exp = _RDB_parse_lookup_table($3);

        if (exp == NULL)
            YYERROR;

        $$ = RDB_ro_op_va("COUNT", exp, NULL);
        if ($$ == NULL)
            YYERROR;
    }
    ;

sum_invocation: TOK_SUM '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            char *attrname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_sum(texp, attrname);
            if ($$ == NULL) {
                RDB_drop_expr(texp);
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
    }
    ;

avg_invocation: TOK_AVG '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            char *attrname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_avg(texp, attrname);
            if ($$ == NULL) {
                RDB_drop_expr(texp);
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
    }
    ;

max_invocation: TOK_MAX '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            char *attrname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_max(texp, attrname);
            if ($$ == NULL) {
                RDB_drop_expr(texp);
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
    }
    ;

min_invocation: TOK_MIN '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            char *attrname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_min(texp, attrname);
            if ($$ == NULL) {
                RDB_drop_expr(texp);
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
    }
    ;

all_invocation: TOK_ALL '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            char *attrname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_all(texp, attrname);
            if ($$ == NULL) {
                RDB_drop_expr(texp);
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
    }
    ;

any_invocation: TOK_ANY '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            char *attrname = NULL;
            RDB_expression *texp = _RDB_parse_lookup_table($3.expv[0]);

            if (texp == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_any(texp, attrname);
            if ($$ == NULL) {
                RDB_drop_expr(texp);
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
    }
    ;

operator_invocation: TOK_ID '(' ')' {
        $$ = RDB_ro_op($1->var.attrname, 0, NULL);
        if ($$ == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }
    }
    | TOK_ID '(' expression_list ')' {
        if ($3.expc == 1
                && strlen($1->var.attrname) > 4
                && strncmp($1->var.attrname, "THE_", 4) == 0) {
            /* THE_ operator - requires special treatment */
            $$ = RDB_expr_comp($3.expv[0], $1->var.attrname + 4);
            if ($$ == NULL)
                YYERROR;
        } else {
            int i;

            for (i = 0; i < $3.expc; i++) {
                RDB_expression *exp = _RDB_parse_lookup_table($3.expv[i]);
                if (exp == NULL) {
                    _RDB_parse_ret = RDB_NO_MEMORY;
                    YYERROR;
                }
                $3.expv[i] = exp;
            }

            $$ = RDB_ro_op($1->var.attrname, $3.expc, $3.expv);
            if ($$ == NULL) {
                _RDB_parse_ret = RDB_NO_MEMORY;
                YYERROR;
            }
        }
    }
    ;

expression_list: expression {
        $$.expc = 1;
        $$.expv[0] = $1;
    }
    | expression_list ',' expression {
        int i;

        if ($1.expc >= DURO_MAX_LLEN)
            YYERROR;

        for (i = 0; i < $1.expc; i++) {
            $$.expv[i] = $1.expv[i];
        }
        $$.expv[$1.expc] = $3;
        $$.expc = $1.expc + 1;
    }
    ;

literal: TOK_RELATION '{' expression_list '}' {
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
            _RDB_parse_ret = RDB_TYPE_MISMATCH;
            YYERROR;
        }
        attrc = RDB_tuple_size(tplp);
        attrv = malloc(sizeof (RDB_attr) * attrc);
        if (attrv == NULL) {
            _RDB_parse_ret = RDB_NO_MEMORY;
            YYERROR;
        }

        RDB_init_hashtable_iter(&hiter, &tplp->var.tpl_tab);
        for (i = 0; i < attrc; i++) {
            /* Get next attribute */
            entryp = RDB_hashtable_next(&hiter);

            attrv[i].name = entryp->key;
            if (entryp->obj.typ == NULL) {
                _RDB_parse_ret = RDB_NOT_SUPPORTED;
                free(attrv);
                YYERROR;
            }
            attrv[i].typ = entryp->obj.typ;
            attrv[i].defaultp = NULL;
        }
        RDB_destroy_hashtable_iter(&hiter);        

        _RDB_parse_ret = RDB_create_table(NULL, RDB_FALSE, attrc, attrv, 0, NULL,
                _RDB_parse_txp, &tbp);
        free(attrv);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }

        _RDB_parse_ret = RDB_insert(tbp, tplp, _RDB_parse_txp);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }

        for (i = 1; i < $3.expc; i++) {
            tplp = RDB_expr_obj($3.expv[i]);
            if (tplp == NULL)
                YYERROR;
            if (tplp->kind != RDB_OB_TUPLE) {
                _RDB_parse_ret = RDB_TYPE_MISMATCH;
                YYERROR;
            }
            _RDB_parse_ret = RDB_insert(tbp, tplp, _RDB_parse_txp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
        }
        RDB_init_obj(&obj);
        RDB_table_to_obj(&obj, tbp);
        $$ = RDB_obj_to_expr(&obj);
        RDB_destroy_obj(&obj);
    }
/*     | TOK_RELATION '{' attribute_name_type_list '}'
       '{' opt_expression_list '}' {
    }
    | TOK_RELATION '{' '}'
       '{' opt_expression_list '}' {
    }
    */ | TOK_TABLE_DEE {
        RDB_object tpl;
        RDB_expression *exp = table_dum_expr();

        if (exp == NULL)
            YYERROR;

        RDB_init_obj(&tpl);
        _RDB_parse_ret = RDB_insert(exp->var.obj.var.tbp, &tpl, _RDB_parse_txp);
        RDB_destroy_obj(&tpl);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }

        $$ = exp;
    }
    | TOK_TABLE_DUM {
        RDB_expression *exp = table_dum_expr();

        if (exp == NULL)
            YYERROR;
        $$ = exp;
    }
    | TOK_TUPLE '{' '}' {
        RDB_object obj;

        RDB_init_obj(&obj);

        $$ = RDB_obj_to_expr(&obj);
        RDB_destroy_obj(&obj);
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

        RDB_init_obj(&obj);
        valp = RDB_expr_obj($2);
        if (valp == NULL)
            YYERROR;
        _RDB_parse_ret = RDB_tuple_set(&obj, $1->var.attrname, valp);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;

        $$ = RDB_obj_to_expr(&obj);
        RDB_destroy_obj(&obj);
    }
    | tuple_item_list ',' TOK_ID expression {
        RDB_object obj;
        RDB_object *valp;

        RDB_init_obj(&obj);
        valp = RDB_expr_obj($4);
        if (valp == NULL)
            YYERROR;

        $$ = RDB_obj_to_expr(RDB_expr_obj($1));
        _RDB_parse_ret = RDB_tuple_set(RDB_expr_obj($$), $3->var.attrname, valp);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        RDB_destroy_obj(&obj);
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

opt_expression_list:
    | expression_list
    ;

*/
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
    if (exp->kind == RDB_EX_ATTR) {
        RDB_table *tbp;

        /* Try to find local table first */
        tbp = (*_RDB_parse_ltfp)(exp->var.attrname, _RDB_parse_arg);
        if (tbp != NULL) {
            RDB_drop_expr(exp);
            return RDB_table_to_expr(tbp);
        }

        /* Local table not found, try to find global table */
        _RDB_parse_ret = RDB_get_table(exp->var.attrname, _RDB_parse_txp,
                &tbp);
        if (_RDB_parse_ret == RDB_OK) {
            RDB_drop_expr(exp);
            return RDB_table_to_expr(tbp);
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

     _RDB_parse_ret = RDB_create_table(NULL, RDB_FALSE, 0, NULL, 0, NULL,
             _RDB_parse_txp, &tbp);
     if (_RDB_parse_ret != RDB_OK)
         return NULL;

     RDB_init_obj(&obj);

     RDB_table_to_obj(&obj, tbp);
     exp = RDB_obj_to_expr(&obj);

     RDB_destroy_obj(&obj);
     return exp;
}
