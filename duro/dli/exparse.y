/* $Id$ */

%{
#define YYDEBUG 1
#define YYSTYPE RDB_expression *
#include <rel/rdb.h>

extern RDB_transaction *expr_txp;
extern RDB_expression *resultp;

static RDB_table *
expr_to_table (RDB_expression *exp)
{
    int ret;

    if (exp->kind == RDB_TABLE)
        return exp->var.tbp;
    if (exp->kind == RDB_ATTR) {
        RDB_table *tbp;

        ret = RDB_get_table(exp->var.attr.name, expr_txp, &tbp);
        if (ret != RDB_OK)
            return NULL;
        return tbp;
    }
    return NULL;
}

%}

%token ID
%token INTEGER
%token STRING
%token DECIMAL
%token FLOAT
%token WHERE
%token UNION
%token INTERSECT
%token MINUS
%token JOIN
%token FROM
%token TUPLE
%token ALL_BUT
%token AS
%token RENAME
%token SUMMARIZE
%token PER
%token ADD
%token TRUE
%token FALSE
%token MATCHES

%%

expression: or_expression { resultp = $1; }
        | extractor { resultp = $1; }
        | relation { resultp = $1; }
        | project { resultp = $1; }
        | select { resultp = $1; }
        | rename { resultp = $1; }
        | extend { resultp = $1; }
        | summarize { resultp = $1; }
        ;

extractor: ID FROM expression
        | TUPLE FROM expression
        ;

project: primary_expression '{' attribute_name_list '}'
        | primary_expression '{' ALL_BUT attribute_name_list '}'
        ;

attribute_name_list: ID
        | attribute_name_list ',' ID
        ;

select: primary_expression WHERE or_expression {
            RDB_table *tbp, *restbp;
            int ret;

            if ($1->kind == RDB_ATTR) {
                ret = RDB_get_table($1->var.attr.name, expr_txp, &tbp);
                if (ret != RDB_OK) {
                    yyerror(RDB_strerror(ret));
                    YYERROR;
                }
            } else if ($1->kind == RDB_TABLE) {
                tbp = $1->var.tbp;
            } else {
                yyerror("select: table expected\n");
                printf("kind is %d\n", $1->kind);
                YYERROR;
            }
            ret = RDB_select(tbp, $3, &restbp);
            if (ret != RDB_OK) {
                yyerror(RDB_strerror(ret));
                YYERROR;
            }
            $$ = RDB_expr_table(restbp);
            }
        ;

rename: primary_expression RENAME '(' renaming_list ')'
        ;

renaming_list: renaming
        | renaming_list ',' renaming
        ;

renaming: ID AS ID
        | "PREFIX" STRING AS STRING
        | "SUFFIX" STRING AS STRING
        ;

relation: primary_expression UNION primary_expression {
            int ret;
            RDB_table *restbp, *tb1p, *tb2p;

            tb1p = expr_to_table($1);
            if (tb1p == NULL) {
                YYERROR;
            }
            tb2p = expr_to_table($3);
            if (tb2p == NULL) {
                YYERROR;
            }

            ret = RDB_union(tb1p, tb2p, &restbp);
            if (ret != RDB_OK) {
                yyerror(RDB_strerror(ret));
                YYERROR;
            }
            $$ = RDB_expr_table(restbp);
            RDB_drop_expr($1);
            RDB_drop_expr($3);
        }
        | primary_expression INTERSECT primary_expression {
            int ret;
            RDB_table *restbp, *tb1p, *tb2p;

            tb1p = expr_to_table($1);
            if (tb1p == NULL) {
                YYERROR;
            }
            tb2p = expr_to_table($3);
            if (tb2p == NULL) {
                YYERROR;
            }

            ret = RDB_intersect(tb1p, tb2p, &restbp);
            if (ret != RDB_OK) {
                yyerror(RDB_strerror(ret));
                YYERROR;
            }
            $$ = RDB_expr_table(restbp);
            RDB_drop_expr($1);
            RDB_drop_expr($3);
        }
        | primary_expression MINUS primary_expression {
            int ret;
            RDB_table *restbp, *tb1p, *tb2p;

            tb1p = expr_to_table($1);
            if (tb1p == NULL) {
                YYERROR;
            }
            tb2p = expr_to_table($3);
            if (tb2p == NULL) {
                YYERROR;
            }

            ret = RDB_minus(tb1p, tb2p, &restbp);
            if (ret != RDB_OK) {
                yyerror(RDB_strerror(ret));
                YYERROR;
            }
            $$ = RDB_expr_table(restbp);
            RDB_drop_expr($1);
            RDB_drop_expr($3);
        }
        | primary_expression JOIN primary_expression {
            int ret;
            RDB_table *restbp, *tb1p, *tb2p;

            tb1p = expr_to_table($1);
            if (tb1p == NULL) {
                YYERROR;
            }
            tb2p = expr_to_table($3);
            if (tb2p == NULL) {
                YYERROR;
            }

            ret = RDB_join(tb1p, tb2p, &restbp);
            if (ret != RDB_OK) {
                yyerror(RDB_strerror(ret));
                YYERROR;
            }
            $$ = RDB_expr_table(restbp);
            RDB_drop_expr($1);
            RDB_drop_expr($3);
        }
        ;

extend: "EXTEND" primary_expression ADD '(' extend_add_list ')'
        ;

extend_add_list: extend_add
        | extend_add_list ',' extend_add
        ;

extend_add: expression AS ID
        ;

summarize: SUMMARIZE primary_expression PER expression
           ADD '(' summarize_add_list ')'
        ;

summarize_add_list: summarize_add
         | summarize_add_list ',' summarize_add
        ;

summarize_add: summary AS ID
        ;

summary: "COUNT"
        | "COUNTD"
        | summary_type '(' expression ')'
        ;

summary_type: "SUM"
        | "SUMD"
        | "AVG"
        | "AVGD"
        | "MAX"
        | "MIN"
        ;

or_expression: and_expression
        | or_expression "OR" and_expression {
            $$ = RDB_or($1, $3);
        }
        ;

and_expression: not_expression
        | and_expression "AND" not_expression {
            $$ = RDB_and($1, $3);
        }
        ;

not_expression: rel_expression
        | "NOT" rel_expression {
            $$ = RDB_not($1);
        }
        ;

rel_expression: add_expression
        | add_expression '=' add_expression {
                $$ = RDB_eq($1, $3);
        }
        | add_expression "<>" add_expression {
                $$ = RDB_neq($1, $3);
        }
        | add_expression ">=" add_expression {
                $$ = RDB_get($1, $3);
        }
        | add_expression "<=" add_expression {
                $$ = RDB_let($1, $3);
        }
        | add_expression '>' add_expression {
                $$ = RDB_gt($1, $3);
        }
        | add_expression '<' add_expression {
                $$ = RDB_lt($1, $3);
        }
        | add_expression "IN" add_expression
        | add_expression MATCHES add_expression {
                $$ = RDB_regmatch($1, $3);
        }
        ;

add_expression: mul_expression
        | '+' mul_expression
        | '-' mul_expression
        | add_expression '+' mul_expression {
            $$ = RDB_add($1, $3);
        }
        | add_expression '-' mul_expression {
            $$ = RDB_subtract($1, $3);
        }
        | add_expression "||" mul_expression
        ;

mul_expression: primary_expression
        | mul_expression '/' primary_expression
        | mul_expression '*' primary_expression
        ;

primary_expression: ID
        | literal
        | operator_invocation
        | '(' expression ')'
        ;

operator_invocation: ID '(' opt_argument_list ')'
        ;

opt_argument_list:
        | argument_list
        ;

argument_list: expression
        | argument_list ',' expression
        ;

literal: "RELATION" '{' expression_list '}'
        | "RELATION" '{' attribute_name_type_list '}'
          '{' opt_expression_list '}'
        | "RELATION" '{' '}'
          '{' opt_expression_list '}'
        | TUPLE '{' opt_tuple_item_list '}'
        | "TABLE_DEE"
        | "TABLE_DUM"
        | STRING
        | INTEGER
        | DECIMAL
        | FLOAT
        | TRUE
        | FALSE
        ;

opt_tuple_item_list:
        | tuple_item_list
        ;

tuple_item_list: tuple_item
        | tuple_item_list ',' tuple_item
        ;

tuple_item: ID expression
        ;

attribute_name_type_list: attribute_name_type
        | attribute_name_type_list ',' attribute_name_type
        ;

attribute_name_type: ID type
        ;

type: ID
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

%%
