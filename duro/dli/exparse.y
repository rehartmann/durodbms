/* $Id$ */

%{
#define YYDEBUG 1

#include <dli/parse.h>
#include <rel/rdb.h>
#include <gen/strfns.h>
#include <string.h>

extern RDB_transaction *expr_txp;
extern RDB_expression *resultp;
extern int expr_ret;
extern RDB_ltablefunc *expr_ltfp;
extern void *expr_arg;

static RDB_table *
expr_to_table (const RDB_expression *exp);

int
yylex(void);

void
yyerror(const char *);

enum {
    DURO_MAX_LLEN = 200
};

%}

%union {
    RDB_expression *exp;
    struct {
        int attrc;
        char *attrv[DURO_MAX_LLEN];
    } attrlist;
    struct {
        int extc;
        RDB_virtual_attr extv[DURO_MAX_LLEN];
    } extlist;
    struct {
        int addc;
        RDB_summarize_add addv[DURO_MAX_LLEN];
    } addlist;
    struct {
        int renc;
        RDB_renaming renv[DURO_MAX_LLEN];
    } renlist;
    struct {
        int argc;
        RDB_expression *argv[DURO_MAX_LLEN];
    } arglist;
    struct {
        int wrapc;
        RDB_wrapping wrapv[DURO_MAX_LLEN];
    } wraplist;
}

%token <exp> TOK_ID
%token <exp> TOK_INTEGER
%token <exp> TOK_STRING
%token <exp> TOK_DECIMAL
%token <exp> TOK_FLOAT
%token <exp> TOK_TRUE
%token <exp> TOK_FALSE
%token TOK_WHERE
%token TOK_UNION
%token TOK_INTERSECT
%token TOK_MINUS
%token TOK_JOIN
%token TOK_FROM
%token TOK_TUPLE
%token TOK_BUT
%token TOK_AS
%token TOK_RENAME
%token TOK_EXTEND
%token TOK_SUMMARIZE
%token TOK_DIVIDEBY
%token TOK_WRAP
%token TOK_UNWRAP
%token TOK_PER
%token TOK_ADD
%token TOK_MATCHES
%token TOK_OR
%token TOK_AND
%token TOK_NOT
%token TOK_CONCAT
%token TOK_IS_EMPTY
%token TOK_COUNT
%token TOK_COUNTD
%token TOK_SUM
%token TOK_SUMD
%token TOK_AVG
%token TOK_AVGD
%token TOK_MAX
%token TOK_MIN
%token TOK_ALL
%token TOK_ANY
%token INVALID

%type <exp> relation project select rename extend summarize wrap unwrap
        sdivideby expression or_expression and_expression not_expression
        primary_expression rel_expression add_expression mul_expression
        literal operator_invocation is_empty_invocation count_invocation
        sum_invocation avg_invocation min_invocation max_invocation
        all_invocation any_invocation extractor

%type <attrlist> attribute_name_list

%type <extlist> extend_add_list extend_add

%type <addlist> summarize_add summarize_add_list summary summary_type

%type <renlist> renaming renaming_list

%type <arglist> argument_list

%type <wraplist> wrapping wrapping_list

%%

expression: or_expression { resultp = $1; }
    | extractor { resultp = $1; }
    | relation { resultp = $1; }
    | project { resultp = $1; }
    | select { resultp = $1; }
    | rename { resultp = $1; }
    | extend { resultp = $1; }
    | summarize { resultp = $1; }
    | wrap { resultp = $1; }
    | unwrap { resultp = $1; }
    | sdivideby { resultp = $1; }
    ;

project: primary_expression '{' attribute_name_list '}' {
        RDB_table *tbp, *restbp;
        int i;

        tbp = expr_to_table($1);
        if (tbp == NULL)
        {
            YYERROR;
        }
        expr_ret = RDB_project(tbp, $3.attrc, $3.attrv, &restbp);
        for (i = 0; i < $3.attrc; i++)
            free($3.attrv[i]);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    | primary_expression '{' TOK_ALL TOK_BUT attribute_name_list '}' {
        RDB_table *tbp, *restbp;
        int i;

        tbp = expr_to_table($1);
        if (tbp == NULL)
        {
            YYERROR;
        }
        expr_ret = RDB_remove(tbp, $5.attrc, $5.attrv, &restbp);
        for (i = 0; i < $5.attrc; i++)
            free($5.attrv[i]);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;

attribute_name_list: TOK_ID {
        $$.attrc = 1;
        $$.attrv[0] = RDB_dup_str($1->var.attr.name);
        RDB_drop_expr($1);
    }
    | attribute_name_list ',' TOK_ID {
        int i;

        /* Copy old attributes */
        if ($1.attrc >= DURO_MAX_LLEN)
            YYERROR;
        for (i = 0; i < $1.attrc; i++)
            $$.attrv[i] = $1.attrv[i];
        $$.attrv[$1.attrc] = RDB_dup_str($3->var.attr.name);
        $$.attrc = $1.attrc + 1;
        RDB_drop_expr($3);
    }
    ;

select: primary_expression TOK_WHERE or_expression {
        RDB_table *tbp, *restbp;

        tbp = expr_to_table($1);
        if (tbp == NULL)
        {
            YYERROR;
        }
        expr_ret = RDB_select(tbp, $3, &restbp);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;

rename: primary_expression TOK_RENAME '(' renaming_list ')' {
        RDB_table *tbp, *restbp;
        int i;

        tbp = expr_to_table($1);
        if (tbp == NULL)
        {
            YYERROR;
        }
        expr_ret = RDB_rename(tbp, $4.renc, $4.renv, &restbp);
        for (i = 0; i < $4.renc; i++) {
            free($4.renv[i].to);
            free($4.renv[i].from);
        }
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;

renaming_list: renaming {
            $$.renv[0].from = $1.renv[0].from;
            $$.renv[0].to = $1.renv[0].to;
            $$.renc = 1;
        }
        | renaming_list ',' renaming {
            int i;

            if ($1.renc >= DURO_MAX_LLEN)
                YYERROR;
            for (i = 0; i < $1.renc; i++) {
                $$.renv[i].from = $1.renv[i].from;
                $$.renv[i].to = $1.renv[i].to;
            }
            $$.renv[$1.renc].from = $3.renv[0].from;
            $$.renv[$1.renc].to = $3.renv[0].to;
            $$.renc = $1.renc + 1;
        }
        ;

renaming: TOK_ID TOK_AS TOK_ID {
            $$.renv[0].from = RDB_dup_str($1->var.attr.name);
            $$.renv[0].to = RDB_dup_str($3->var.attr.name);
            RDB_drop_expr($1);
            RDB_drop_expr($3);
        }
/*
        | "PREFIX" STRING AS STRING
        | "SUFFIX" STRING AS STRING
*/
        ;

relation: primary_expression TOK_UNION primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = expr_to_table($1);
        if (tb1p == NULL) {
            YYERROR;
        }
        tb2p = expr_to_table($3);
        if (tb2p == NULL) {
            YYERROR;
        }

        expr_ret = RDB_union(tb1p, tb2p, &restbp);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
        RDB_drop_expr($1);
        RDB_drop_expr($3);
    }
    | primary_expression TOK_INTERSECT primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = expr_to_table($1);
        if (tb1p == NULL) {
            YYERROR;
        }
        tb2p = expr_to_table($3);
        if (tb2p == NULL) {
            YYERROR;
        }

        expr_ret = RDB_intersect(tb1p, tb2p, &restbp);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
        RDB_drop_expr($1);
        RDB_drop_expr($3);
    }
    | primary_expression TOK_MINUS primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = expr_to_table($1);
        if (tb1p == NULL) {
            YYERROR;
        }
        tb2p = expr_to_table($3);
        if (tb2p == NULL) {
            YYERROR;
        }

        expr_ret = RDB_minus(tb1p, tb2p, &restbp);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
        RDB_drop_expr($1);
        RDB_drop_expr($3);
    }
    | primary_expression TOK_JOIN primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = expr_to_table($1);
        if (tb1p == NULL) {
            YYERROR;
        }
        tb2p = expr_to_table($3);
        if (tb2p == NULL) {
            YYERROR;
        }

        expr_ret = RDB_join(tb1p, tb2p, &restbp);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
        RDB_drop_expr($1);
        RDB_drop_expr($3);
    }
    ;

extend: TOK_EXTEND primary_expression TOK_ADD '(' extend_add_list ')' {
        RDB_table *tbp, *restbp;
        int i;

        tbp = expr_to_table($2);
        if (tbp == NULL)
        {
            YYERROR;
        }
        expr_ret = RDB_extend(tbp, $5.extc, $5.extv, &restbp);
        for (i = 0; i < $5.extc; i++) {
            free($5.extv[i].name);
            /* Expressions are not dropped since they are now owned
             * by the new table.
             */
        }
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;

extend_add_list: extend_add {
        $$.extv[0].exp = $1.extv[0].exp;
        $$.extv[0].name = $1.extv[0].name;
        $$.extc = 1;
    }
    | extend_add_list ',' extend_add {
        int i;

        if ($$.extc >= DURO_MAX_LLEN)
            YYERROR;

        /* Copy old attributes */
        for (i = 0; i < $1.extc; i++) {
            $$.extv[i].name = $1.extv[i].name;
            $$.extv[i].exp = $1.extv[i].exp;
        }

        /* Add new attribute */
        $$.extv[$1.extc].name = $3.extv[0].name;
        $$.extv[$1.extc].exp = $3.extv[0].exp;
    
        $$.extc = $1.extc + 1;
    }
    ;

extend_add: expression TOK_AS TOK_ID {
        $$.extv[0].name = RDB_dup_str($3->var.attr.name);
        $$.extv[0].exp = $1;
    }
    ;

summarize: TOK_SUMMARIZE primary_expression TOK_PER expression
           TOK_ADD '(' summarize_add_list ')' {
        RDB_table *tb1p, *tb2p, *restbp;
        int i;

        tb1p = expr_to_table($2);
        if (tb1p == NULL)
        {
            YYERROR;
        }

        tb2p = expr_to_table($4);
        if (tb2p == NULL)
        {
            YYERROR;
        }

        expr_ret = RDB_summarize(tb1p, tb2p, $7.addc, $7.addv, &restbp);
        for (i = 0; i < $7.addc; i++) {
            free($7.addv[i].name);
        }
        if (expr_ret != RDB_OK) {
            for (i = 0; i < $7.addc; i++) {
                RDB_drop_expr($7.addv[i].exp);
            }
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;

sdivideby: primary_expression TOK_DIVIDEBY primary_expression
           TOK_PER primary_expression {
        RDB_table *tb1p, *tb2p, *tb3p, *restbp;

        tb1p = expr_to_table($1);
        if (tb1p == NULL)
        {
            YYERROR;
        }

        tb2p = expr_to_table($3);
        if (tb2p == NULL)
        {
            YYERROR;
        }

        tb3p = expr_to_table($5);
        if (tb3p == NULL)
        {
            YYERROR;
        }

        expr_ret = RDB_sdivide(tb1p, tb2p, tb3p, &restbp);
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;    

summarize_add_list: summarize_add {
        $$.addv[0].op = $1.addv[0].op;
        $$.addv[0].exp = $1.addv[0].exp;
        $$.addv[0].name = $1.addv[0].name;
        $$.addc = 1;
    }
    | summarize_add_list ',' summarize_add {
        int i;

        if ($1.addc >= DURO_MAX_LLEN)
            YYERROR;

        /* Copy old elements */
        for (i = 0; i < $1.addc; i++) {
            $$.addv[i].op = $1.addv[i].op;
            $$.addv[i].name = $1.addv[i].name;
            $$.addv[i].exp = $1.addv[i].exp;
        }

        /* Add new element */
        $$.addv[i].op = $3.addv[0].op;
        $$.addv[$1.addc].name = $3.addv[0].name;
        $$.addv[$1.addc].exp = $3.addv[0].exp;
    
        $$.addc = $1.addc + 1;
    }
    ;

summarize_add: summary TOK_AS TOK_ID {
        $$.addv[0].op = $1.addv[0].op;
        $$.addv[0].exp = $1.addv[0].exp;
        $$.addv[0].name = RDB_dup_str($3->var.attr.name);
        RDB_drop_expr($3);
    }
    ;

summary: TOK_COUNT {
        $$.addv[0].op = RDB_COUNT;
    }
    | TOK_COUNTD {
        $$.addv[0].op = RDB_COUNT;
    }
    | summary_type '(' expression ')' {
        $$.addv[0].op = $1.addv[0].op;
        $$.addv[0].exp = $3;
    }
    ;

summary_type: TOK_SUM {
        $$.addv[0].op = RDB_SUM;
    }
    | TOK_SUMD {
        $$.addv[0].op = RDB_SUMD;
    }
    | TOK_AVG {
        $$.addv[0].op = RDB_AVG;
    }
    | TOK_AVGD {
        $$.addv[0].op = RDB_AVGD;
    }
    | TOK_MAX {
        $$.addv[0].op = RDB_MAX;
    }
    | TOK_MIN {
        $$.addv[0].op = RDB_MIN;
    }
    | TOK_ALL {
        $$.addv[0].op = RDB_ALL;
    }
    | TOK_ANY {
        $$.addv[0].op = RDB_ANY;
    }
    ;

wrap:   primary_expression TOK_WRAP
        '(' wrapping_list ')' {
        RDB_table *tbp, *restbp;
        int i, j;

        tbp = expr_to_table($1);
        if (tbp == NULL)
        {
            YYERROR;
        }
        expr_ret = RDB_wrap(tbp, $4.wrapc, $4.wrapv, &restbp);
        for (i = 0; i < $4.wrapc; i++) {
            free($4.wrapv[i].attrname);
            for (j = 0; j < $4.wrapv[i].attrc; i++)
                free($4.wrapv[i].attrv[j]);
        }
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;

wrapping_list: wrapping {
        $$.wrapv[0].attrname = $1.wrapv[0].attrname;
        $$.wrapv[0].attrv = $1.wrapv[0].attrv;
        $$.wrapv[0].attrc = $1.wrapv[0].attrc;
        $$.wrapc = 1;
    }
    | wrapping_list ',' wrapping {
        int i;

        if ($1.wrapc >= DURO_MAX_LLEN)
            YYERROR;

        /* Copy old elements */
        for (i = 0; i < $1.wrapc; i++) {
            $$.wrapv[i] = $1.wrapv[i];
        }

        /* Add new element */
        $$.wrapv[$1.wrapc] = $3.wrapv[0];
    
        $$.wrapc = $1.wrapc + 1;
    }
    ;

wrapping: '(' attribute_name_list ')' TOK_AS TOK_ID {
        int i;

        $$.wrapv[0].attrc = $2.attrc;
        $$.wrapv[0].attrv = malloc(sizeof(char *) * $2.attrc);

        for (i = 0; i < $2.attrc; i++) {
            $$.wrapv[0].attrv[i] = $2.attrv[i];
        }
        $$.wrapv[0].attrname = RDB_dup_str($5->var.attr.name);
        RDB_drop_expr($5);
    }
    ;

unwrap: primary_expression TOK_UNWRAP '(' attribute_name_list ')' {
        RDB_table *tbp, *restbp;
        int i;

        tbp = expr_to_table($1);
        if (tbp == NULL)
        {
            YYERROR;
        }
        expr_ret = RDB_unwrap(tbp, $4.attrc, $4.attrv, &restbp);
        for (i = 0; i < $4.attrc; i++) {
            free($4.attrv[i]);
        }
        if (expr_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_expr_table(restbp);
    }
    ;    

or_expression: and_expression
        | or_expression TOK_OR and_expression {
            $$ = RDB_or($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        ;

and_expression: not_expression
        | and_expression TOK_AND not_expression {
            $$ = RDB_and($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        ;

not_expression: rel_expression
        | TOK_NOT rel_expression {
            $$ = RDB_not($2);
            if ($$ == NULL)
                YYERROR;
        }
        ;

rel_expression: add_expression
        | add_expression '=' add_expression {
            $$ = RDB_eq($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression "<>" add_expression {
            $$ = RDB_neq($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression ">=" add_expression {
            $$ = RDB_get($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression "<=" add_expression {
            $$ = RDB_let($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression '>' add_expression {
            $$ = RDB_gt($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression '<' add_expression {
            $$ = RDB_lt($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression "IN" add_expression
        | add_expression TOK_MATCHES add_expression {
            $$ = RDB_regmatch($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        ;

add_expression: mul_expression
        | '+' mul_expression {
            $$ = $2;
        }
        | '-' mul_expression {
            $$ = RDB_negate($2);
        }
        | add_expression '+' mul_expression {
            $$ = RDB_add($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression '-' mul_expression {
            $$ = RDB_subtract($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | add_expression TOK_CONCAT mul_expression {
            $$ = RDB_concat($1, $3);
            if ($$ == NULL)
                YYERROR;
        }            
        ;

mul_expression: primary_expression
        | mul_expression '*' primary_expression {
            $$ = RDB_multiply($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        | mul_expression '/' primary_expression {
            $$ = RDB_divide($1, $3);
            if ($$ == NULL)
                YYERROR;
        }
        ;

primary_expression: TOK_ID
    | primary_expression '.' TOK_ID {
        $$ = RDB_tuple_attr($1, $3->var.attr.name);
        RDB_drop_expr($3);
    }
    | literal
    | count_invocation
    | sum_invocation
    | avg_invocation
    | max_invocation
    | min_invocation
    | all_invocation
    | any_invocation
    | is_empty_invocation
    | operator_invocation
    | '(' expression ')' {
        $$ = $2;
    }
    ;

extractor: TOK_TUPLE TOK_FROM expression {
        RDB_table *tbp;
        RDB_object tpl;
        int ret;

        tbp = expr_to_table($3);
        if (tbp == NULL)
            YYERROR;

        RDB_init_obj(&tpl);
        ret = RDB_extract_tuple(tbp, &tpl, expr_txp);
        if (ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            YYERROR;
        }
        $$ = RDB_obj_const(&tpl);
        RDB_destroy_obj(&tpl);
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        RDB_table *tbp = expr_to_table($3);

        if (tbp == NULL)
            YYERROR;

        $$ = RDB_expr_cardinality(RDB_expr_table(tbp));
        if ($$ == NULL)
            YYERROR;
    }
    ;

sum_invocation: TOK_SUM '(' argument_list ')' {
        if ($3.argc == 0 || $3.argc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = expr_to_table($3.argv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.argc == 2) {
                if ($3.argv[1]->kind != RDB_ATTR)
                    YYERROR;
                attrname = $3.argv[1]->var.attr.name;
            }

            $$ = RDB_expr_sum(RDB_expr_table(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
        }
    }
    ;

avg_invocation: TOK_AVG '(' argument_list ')' {
        if ($3.argc == 0 || $3.argc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = expr_to_table($3.argv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.argc == 2) {
                if ($3.argv[1]->kind != RDB_ATTR)
                    YYERROR;
                attrname = $3.argv[1]->var.attr.name;
            }

            $$ = RDB_expr_avg(RDB_expr_table(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
        }
    }
    ;

max_invocation: TOK_MAX '(' argument_list ')' {
        if ($3.argc == 0 || $3.argc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = expr_to_table($3.argv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.argc == 2) {
                if ($3.argv[1]->kind != RDB_ATTR)
                    YYERROR;
                attrname = $3.argv[1]->var.attr.name;
            }

            $$ = RDB_expr_max(RDB_expr_table(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
        }
    }
    ;

min_invocation: TOK_MIN '(' argument_list ')' {
        if ($3.argc == 0 || $3.argc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = expr_to_table($3.argv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.argc == 2) {
                if ($3.argv[1]->kind != RDB_ATTR)
                    YYERROR;
                attrname = $3.argv[1]->var.attr.name;
            }

            $$ = RDB_expr_min(RDB_expr_table(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
        }
    }
    ;

all_invocation: TOK_ALL '(' argument_list ')' {
        if ($3.argc == 0 || $3.argc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = expr_to_table($3.argv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.argc == 2) {
                if ($3.argv[1]->kind != RDB_ATTR)
                    YYERROR;
                attrname = $3.argv[1]->var.attr.name;
            }

            $$ = RDB_expr_all(RDB_expr_table(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
        }
    }
    ;

any_invocation: TOK_ANY '(' argument_list ')' {
        if ($3.argc == 0 || $3.argc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = expr_to_table($3.argv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.argc == 2) {
                if ($3.argv[1]->kind != RDB_ATTR)
                    YYERROR;
                attrname = $3.argv[1]->var.attr.name;
            }

            $$ = RDB_expr_any(RDB_expr_table(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
        }
    }
    ;

is_empty_invocation: TOK_IS_EMPTY '(' expression ')' {
        RDB_table *tbp = expr_to_table($3);

        if (tbp == NULL)
            YYERROR;

        $$ = RDB_expr_is_empty(RDB_expr_table(tbp));
        if ($$ == NULL)
            YYERROR;
    }
    ;

operator_invocation: TOK_ID '(' ')' {
        expr_ret = RDB_user_op($1->var.attr.name, 0, NULL,
                expr_txp, &$$);
        if (expr_ret != RDB_OK)
            YYERROR;
    }
    | TOK_ID '(' argument_list ')' {
        expr_ret = RDB_user_op($1->var.attr.name, $3.argc, $3.argv,
                expr_txp, &$$);
        if (expr_ret != RDB_OK)
            YYERROR;
    }
    ;

argument_list: expression {
        $$.argc = 1;
        $$.argv[0] = $1;
    }
    | argument_list ',' expression {
        int i;

        if ($1.argc >= DURO_MAX_LLEN)
            YYERROR;

        for (i = 0; i < $1.argc; i++) {
            $$.argv[i] = $1.argv[i];
        }
        $$.argv[$1.argc] = $3;
        $$.argc = $1.argc + 1;
    }
    ;

literal: /* "RELATION" '{' expression_list '}' {
        }
        | "RELATION" '{' attribute_name_type_list '}'
          '{' opt_expression_list '}' {
        }
        | "RELATION" '{' '}'
          '{' opt_expression_list '}' {
        }
        | "TABLE_DEE" {
        }
        | "TABLE_DUM" {
        }
        | TUPLE '{' opt_tuple_item_list '}' {
        } 
        | */ TOK_STRING
        | TOK_INTEGER
        | TOK_DECIMAL
        | TOK_FLOAT
        | TOK_TRUE
        | TOK_FALSE
        ;
/*
opt_tuple_item_list:
        | tuple_item_list
        ;

tuple_item_list: tuple_item
        | tuple_item_list ',' tuple_item
        ;

tuple_item: TOK_ID expression
        ;

attribute_name_type_list: attribute_name_type
        | attribute_name_type_list ',' attribute_name_type
        ;

attribute_name_type: TOK_ID type
        ;

type: TOK_ID
        | "SAME_TYPE_AS" '(' expression ')'
        | "RELATION" '{' attribute_name_type_list '}'
        | "RELATION" '{' '}'
        | TUPLE '{' attribute_name_type_list '}'
        | TUPLE '{' '}'
        ;

opt_expression_list:
        | expression_list
        ;

expression_list: expression
        | expression_list ',' expression
        ;
*/
%%

RDB_table *
RDB_get_ltable(void *arg);

static RDB_table *
expr_to_table(const RDB_expression *exp)
{
    if (exp->kind == RDB_TABLE)
        return exp->var.tbp;
    if (exp->kind == RDB_ATTR) {
        RDB_table *tbp;

        /* Try to find local table first */
        tbp = (*expr_ltfp)(exp->var.attr.name, expr_arg);
        if (tbp != NULL)
            return tbp;

        /* Local table not found, try to find global table */
        expr_ret = RDB_get_table(exp->var.attr.name, expr_txp, &tbp);
        if (expr_ret != RDB_OK)
            return NULL;
        return tbp;
    }
    expr_ret = RDB_INVALID_ARGUMENT;
    return NULL;
}
