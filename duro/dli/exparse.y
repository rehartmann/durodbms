/* $Id$ */

%{
#define YYDEBUG 1

#include <dli/parse.h>
#include <rel/rdb.h>
#include <rel/internal.h>
#include <gen/strfns.h>
#include <gen/hashmapit.h>
#include <string.h>

extern RDB_transaction *_RDB_parse_txp;
extern RDB_expression *_RDB_parse_resultp;
extern int _RDB_parse_ret;
extern RDB_ltablefn *_RDB_parse_ltfp;
extern void *_RDB_parse_arg;

typedef struct explink {
    RDB_expression *exp;
    struct explink *nextp;
} explink;

int
_RDB_parse_add_exp(RDB_expression *exp);

void
_RDB_parse_remove_exp(RDB_expression *exp);

explink *_RDB_parse_first_exp = NULL;

RDB_table *
_RDB_parse_expr_to_table(const RDB_expression *exp);

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
        int expc;
        RDB_expression *expv[DURO_MAX_LLEN];
    } explist;
    struct {
        int wrapc;
        RDB_wrapping wrapv[DURO_MAX_LLEN];
    } wraplist;
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
%token TOK_INTEGER
%token TOK_RATIONAL
%token TOK_STRING
%token TOK_COUNT
%token TOK_SUM
%token TOK_AVG
%token TOK_MAX
%token TOK_MIN
%token TOK_ALL
%token TOK_ANY
%token TOK_TABLE_DEE
%token TOK_TABLE_DUM
%token INVALID

%type <exp> relation project select rename extend summarize wrap unwrap
        group ungroup sdivideby expression or_expression and_expression
        not_expression primary_expression rel_expression add_expression
        mul_expression literal operator_invocation
        integer_invocation rational_invocation string_invocation
        count_invocation sum_invocation avg_invocation min_invocation
        max_invocation all_invocation any_invocation extractor tuple_item_list

%type <attrlist> attribute_name_list

%type <extlist> extend_add_list extend_add

%type <addlist> summarize_add summarize_add_list summary summary_type

%type <renlist> renaming renaming_list

%type <explist> expression_list

%type <wraplist> wrapping wrapping_list

%%

expression: or_expression { _RDB_parse_resultp = $1; }
    | extractor { _RDB_parse_resultp = $1; }
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
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL)
        {
            RDB_object *valp = RDB_expr_obj($1);
            RDB_object dstobj;

            if (valp == NULL)
                YYERROR;
            RDB_init_obj(&dstobj);
            _RDB_parse_ret = RDB_project_tuple(valp, $3.attrc, $3.attrv, &dstobj);
            if (_RDB_parse_ret != RDB_OK) {
                RDB_destroy_obj(&dstobj);
                YYERROR;
            }
            $$ = RDB_obj_to_expr(&dstobj);
            RDB_destroy_obj(&dstobj);
            if ($$ == NULL) {
                YYERROR;
            }
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        } else {
            _RDB_parse_ret = RDB_project(tbp, $3.attrc, $3.attrv, &restbp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
            tbp->refcount++;
            $$ = RDB_table_to_expr(restbp);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    | expression '{' TOK_ALL TOK_BUT attribute_name_list '}' {
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL)
        {
            RDB_object *valp = RDB_expr_obj($1);
            RDB_object dstobj;

            RDB_init_obj(&dstobj);
            if (valp == NULL)
                YYERROR;
            _RDB_parse_ret = RDB_remove_tuple(valp, $5.attrc, $5.attrv, &dstobj);
            if (_RDB_parse_ret != RDB_OK) {
                RDB_destroy_obj(&dstobj);
                YYERROR;
            }
            $$ = RDB_obj_to_expr(&dstobj);
            RDB_destroy_obj(&dstobj);
            if ($$ == NULL) {
                YYERROR;
            }
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        } else {
            _RDB_parse_ret = RDB_remove(tbp, $5.attrc, $5.attrv, &restbp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
            tbp->refcount++;
            $$ = RDB_table_to_expr(restbp);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

attribute_name_list: TOK_ID {
        $$.attrc = 1;
        $$.attrv[0] = $1->var.attrname;
    }
    | attribute_name_list ',' TOK_ID {
        int i;

        /* Copy old attributes */
        if ($1.attrc >= DURO_MAX_LLEN)
            YYERROR;
        for (i = 0; i < $1.attrc; i++)
            $$.attrv[i] = $1.attrv[i];
        $$.attrv[$1.attrc] = $3->var.attrname;
        $$.attrc = $1.attrc + 1;
    }
    ;

select: expression TOK_WHERE or_expression {
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL)
        {
            YYERROR;
        }
        _RDB_parse_ret = RDB_select(tbp, $3, _RDB_parse_txp, &restbp);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }
        tbp->refcount++;
        _RDB_parse_remove_exp($3);
        $$ = RDB_table_to_expr(restbp);
        if ($$ == NULL)
            YYERROR;
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

rename: expression TOK_RENAME '(' renaming_list ')' {
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL)
        {
            RDB_object *valp = RDB_expr_obj($1);
            RDB_object dstobj;

            RDB_init_obj(&dstobj);
            if (valp == NULL)
                YYERROR;
            _RDB_parse_ret = RDB_rename_tuple(valp, $4.renc, $4.renv, &dstobj);
            if (_RDB_parse_ret != RDB_OK) {
                RDB_destroy_obj(&dstobj);
                YYERROR;
            }
            $$ = RDB_obj_to_expr(&dstobj);
            RDB_destroy_obj(&dstobj);
            if ($$ == NULL) {
                YYERROR;
            }
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        } else {
            _RDB_parse_ret = RDB_rename(tbp, $4.renc, $4.renv, &restbp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
            tbp->refcount++;
            $$ = RDB_table_to_expr(restbp);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
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
            $$.renv[0].from = $1->var.attrname;
            $$.renv[0].to = $3->var.attrname;
        }
/*
        | "PREFIX" STRING AS STRING
        | "SUFFIX" STRING AS STRING
*/
        ;

relation: expression TOK_UNION primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = _RDB_parse_expr_to_table($1);
        if (tb1p == NULL) {
            YYERROR;
        }
        tb2p = _RDB_parse_expr_to_table($3);
        if (tb2p == NULL) {
            YYERROR;
        }

        _RDB_parse_ret = RDB_union(tb1p, tb2p, &restbp);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }
        tb1p->refcount++;
        tb2p->refcount++;
        $$ = RDB_table_to_expr(restbp);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
    }
    | expression TOK_INTERSECT primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = _RDB_parse_expr_to_table($1);
        if (tb1p == NULL) {
            YYERROR;
        }
        tb2p = _RDB_parse_expr_to_table($3);
        if (tb2p == NULL) {
            YYERROR;
        }

        _RDB_parse_ret = RDB_intersect(tb1p, tb2p, &restbp);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }
        tb1p->refcount++;
        tb2p->refcount++;
        $$ = RDB_table_to_expr(restbp);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | expression TOK_MINUS primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = _RDB_parse_expr_to_table($1);
        if (tb1p == NULL) {
            YYERROR;
        }
        tb2p = _RDB_parse_expr_to_table($3);
        if (tb2p == NULL) {
            YYERROR;
        }

        _RDB_parse_ret = RDB_minus(tb1p, tb2p, &restbp);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }
        tb1p->refcount++;
        tb2p->refcount++;
        $$ = RDB_table_to_expr(restbp);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | expression TOK_JOIN primary_expression {
        RDB_table *restbp, *tb1p, *tb2p;

        tb1p = _RDB_parse_expr_to_table($1);
        if (tb1p == NULL) {
            RDB_object *val1p = RDB_expr_obj($1);
            RDB_object *val2p = RDB_expr_obj($3);
            RDB_object dstobj;

            if (val1p == NULL || val2p == NULL)
                YYERROR;
            RDB_init_obj(&dstobj);
            _RDB_parse_ret = RDB_join_tuples(val1p, val2p, _RDB_parse_txp,
                    &dstobj);
            if (_RDB_parse_ret != RDB_OK) {
                RDB_destroy_obj(&dstobj);
                YYERROR;
            }
            $$ = RDB_obj_to_expr(&dstobj);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
            RDB_destroy_obj(&dstobj);
            if ($$ == NULL)
                YYERROR;
        } else {
            tb2p = _RDB_parse_expr_to_table($3);
            if (tb2p == NULL) {
                YYERROR;
            }

            _RDB_parse_ret = RDB_join(tb1p, tb2p, &restbp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
            tb1p->refcount++;
            tb2p->refcount++;
            $$ = RDB_table_to_expr(restbp);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

extend: TOK_EXTEND expression TOK_ADD '(' extend_add_list ')' {
        RDB_table *tbp, *restbp;
        int i;

        tbp = _RDB_parse_expr_to_table($2);
        if (tbp == NULL)
        {
            RDB_object *valp = RDB_expr_obj($2);
            if (valp == NULL)
                YYERROR;

            _RDB_parse_ret = RDB_extend_tuple(valp, $5.extc, $5.extv, _RDB_parse_txp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
            $$ = RDB_obj_to_expr(valp);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        } else {
            _RDB_parse_ret = RDB_extend(tbp, $5.extc, $5.extv, _RDB_parse_txp,
                    &restbp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
            tbp->refcount++;
            $$ = RDB_table_to_expr(restbp);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
        for (i = 0; i < $5.extc; i++) {
            _RDB_parse_remove_exp($5.extv[i].exp);
        }
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
        $$.extv[0].name = $3->var.attrname;
        $$.extv[0].exp = $1;
    }
    ;

summarize: TOK_SUMMARIZE expression TOK_PER expression
           TOK_ADD '(' summarize_add_list ')' {
        RDB_table *tb1p, *tb2p, *restbp;
        int i;

        tb1p = _RDB_parse_expr_to_table($2);
        if (tb1p == NULL)
        {
            YYERROR;
        }

        tb2p = _RDB_parse_expr_to_table($4);
        if (tb2p == NULL)
        {
            YYERROR;
        }

        _RDB_parse_ret = RDB_summarize(tb1p, tb2p, $7.addc, $7.addv, _RDB_parse_txp,
                &restbp);
        for (i = 0; i < $7.addc; i++) {
            _RDB_parse_remove_exp($7.addv[i].exp);
        }
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }
        tb1p->refcount++;
        tb2p->refcount++;

        $$ = RDB_table_to_expr(restbp);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

sdivideby: expression TOK_DIVIDEBY expression
           TOK_PER primary_expression {
        RDB_table *tb1p, *tb2p, *tb3p, *restbp;

        tb1p = _RDB_parse_expr_to_table($1);
        if (tb1p == NULL)
        {
            YYERROR;
        }

        tb2p = _RDB_parse_expr_to_table($3);
        if (tb2p == NULL)
        {
            YYERROR;
        }

        tb3p = _RDB_parse_expr_to_table($5);
        if (tb3p == NULL)
        {
            YYERROR;
        }

        _RDB_parse_ret = RDB_sdivide(tb1p, tb2p, tb3p, &restbp);
        if (_RDB_parse_ret != RDB_OK) {
            YYERROR;
        }
        $$ = RDB_table_to_expr(restbp);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        tb1p->refcount++;
        tb2p->refcount++;
        tb3p->refcount++;
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
        $$.addv[0].name = $3->var.attrname;
    }
    ;

summary: TOK_COUNT {
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
    | TOK_AVG {
        $$.addv[0].op = RDB_AVG;
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

wrap: expression TOK_WRAP '(' wrapping_list ')' {
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL)
        {
            RDB_object *valp = RDB_expr_obj($1);
            RDB_object dstobj;

            RDB_init_obj(&dstobj);
            if (valp == NULL)
                YYERROR;
            _RDB_parse_ret = RDB_wrap_tuple(valp, $4.wrapc, $4.wrapv, &dstobj);
            if (_RDB_parse_ret != RDB_OK) {
                RDB_destroy_obj(&dstobj);
                YYERROR;
            }
            $$ = RDB_obj_to_expr(&dstobj);
            RDB_destroy_obj(&dstobj);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        } else {
            _RDB_parse_ret = RDB_wrap(tbp, $4.wrapc, $4.wrapv, &restbp);
            if (_RDB_parse_ret != RDB_OK) {
                YYERROR;
            }
            tbp->refcount++;
            $$ = RDB_table_to_expr(restbp);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
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

wrapping: '{' attribute_name_list '}' TOK_AS TOK_ID {
        int i;

        $$.wrapv[0].attrc = $2.attrc;
        $$.wrapv[0].attrv = malloc(sizeof(char *) * $2.attrc);

        for (i = 0; i < $2.attrc; i++) {
            $$.wrapv[0].attrv[i] = $2.attrv[i];
        }
        $$.wrapv[0].attrname = $5->var.attrname;
    }
    ;

unwrap: expression TOK_UNWRAP '(' attribute_name_list ')' {
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL) {
            RDB_object *valp = RDB_expr_obj($1);
            RDB_object dstobj;

            RDB_init_obj(&dstobj);
            if (valp == NULL)
                YYERROR;
            _RDB_parse_ret = RDB_unwrap_tuple(valp, $4.attrc, $4.attrv,
                    &dstobj);
            if (_RDB_parse_ret != RDB_OK) {
                RDB_destroy_obj(&dstobj);
                YYERROR;
            }
            $$ = RDB_obj_to_expr(&dstobj);
            RDB_destroy_obj(&dstobj);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        } else {
            _RDB_parse_ret = RDB_unwrap(tbp, $4.attrc, $4.attrv, &restbp);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
            tbp->refcount++;
            $$ = RDB_table_to_expr(restbp);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;    

group: expression TOK_GROUP '{' attribute_name_list '}' TOK_AS TOK_ID {
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL)
            YYERROR;
        _RDB_parse_ret = RDB_group(tbp, $4.attrc, $4.attrv, $7->var.attrname,
                &restbp);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        tbp->refcount++;
        $$ = RDB_table_to_expr(restbp);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;    

ungroup: expression TOK_UNGROUP TOK_ID {
        RDB_table *tbp, *restbp;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp == NULL)
            YYERROR;
        _RDB_parse_ret = RDB_ungroup(tbp, $3->var.attrname, &restbp);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        tbp->refcount++;
        $$ = RDB_table_to_expr(restbp);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;    

or_expression: and_expression
    | or_expression TOK_OR and_expression {
        _RDB_parse_ret = RDB_ro_op_2("OR", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

and_expression: not_expression
    | and_expression TOK_AND not_expression {
        _RDB_parse_ret = RDB_ro_op_2("AND", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        if ($$ == NULL)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

not_expression: rel_expression
    | TOK_NOT rel_expression {
        _RDB_parse_ret = RDB_ro_op_1("NOT", $2, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($2);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

rel_expression: add_expression
    | add_expression '=' add_expression {
        $$ = RDB_eq($1, $3);
        if ($$ == NULL)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression TOK_NE add_expression {
        _RDB_parse_ret = RDB_ro_op_2("<>", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression TOK_GE add_expression {
        _RDB_parse_ret = RDB_ro_op_2(">=", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression TOK_LE add_expression {
        _RDB_parse_ret = RDB_ro_op_2("<=", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression '>' add_expression {
        _RDB_parse_ret = RDB_ro_op_2(">", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression '<' add_expression {
        _RDB_parse_ret = RDB_ro_op_2("<", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression TOK_IN add_expression {
        /* If $1 is a name, try to find a table with that name */
        RDB_table *tbp = _RDB_parse_expr_to_table($1);
        RDB_expression *exp = tbp != NULL ? RDB_table_to_expr(tbp) : $1;

        _RDB_parse_ret = RDB_ro_op_2("IN", exp, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        if (tbp != NULL)
            tbp->refcount++;
        else
            _RDB_parse_remove_exp(exp);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression TOK_MATCHES add_expression {
        _RDB_parse_ret = RDB_ro_op_2("MATCHES", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression TOK_SUBSET_OF add_expression {
        RDB_table *tbp;
        RDB_expression *ex1p, *ex2p;

        tbp = _RDB_parse_expr_to_table($1);
        if (tbp != NULL)
           ex1p = RDB_table_to_expr(tbp);
        else
           ex1p = $1;

        tbp = _RDB_parse_expr_to_table($3);
        if (tbp != NULL)
           ex2p = RDB_table_to_expr(tbp);
        else
           ex2p = $3;

        _RDB_parse_ret = RDB_ro_op_2("SUBSET_OF", ex1p, ex2p, _RDB_parse_txp,
                &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

add_expression: mul_expression
    | '+' mul_expression {
        $$ = $2;
        _RDB_parse_remove_exp($2);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | '-' mul_expression {
        _RDB_parse_ret = RDB_ro_op_1("-", $2, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($2);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression '+' mul_expression {
        _RDB_parse_ret = RDB_ro_op_2("+", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression '-' mul_expression {
        _RDB_parse_ret = RDB_ro_op_2("-", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | add_expression TOK_CONCAT mul_expression {
        _RDB_parse_ret = RDB_ro_op_2("||", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }            
    ;

mul_expression: primary_expression
    | mul_expression '*' primary_expression {
        _RDB_parse_ret = RDB_ro_op_2("*", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | mul_expression '/' primary_expression {
        _RDB_parse_ret = RDB_ro_op_2("/", $1, $3, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_remove_exp($1);
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

primary_expression: TOK_ID
    | primary_expression '.' TOK_ID {
        $$ = RDB_tuple_attr($1, $3->var.attrname);
        _RDB_parse_remove_exp($1);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | literal
    | count_invocation
    | sum_invocation
    | avg_invocation
    | max_invocation
    | min_invocation
    | all_invocation
    | any_invocation
    | integer_invocation
    | rational_invocation
    | string_invocation
    | operator_invocation
    | '(' expression ')' {
        $$ = $2;
    }
    ;

extractor: TOK_TUPLE TOK_FROM expression {
        RDB_table *tbp;
        RDB_object tpl;

        tbp = _RDB_parse_expr_to_table($3);
        if (tbp == NULL)
            YYERROR;

        RDB_init_obj(&tpl);
        _RDB_parse_ret = RDB_extract_tuple(tbp, _RDB_parse_txp, &tpl);
        if (_RDB_parse_ret != RDB_OK) {
            RDB_destroy_obj(&tpl);
            YYERROR;
        }
        $$ = RDB_obj_to_expr(&tpl);
        RDB_destroy_obj(&tpl);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        RDB_table *tbp = _RDB_parse_expr_to_table($3);

        $$ = RDB_expr_cardinality(tbp != NULL ? RDB_table_to_expr(tbp) : $3);
        if ($$ == NULL)
            YYERROR;
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

sum_invocation: TOK_SUM '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = _RDB_parse_expr_to_table($3.expv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_sum(RDB_table_to_expr(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

avg_invocation: TOK_AVG '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = _RDB_parse_expr_to_table($3.expv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_avg(RDB_table_to_expr(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

max_invocation: TOK_MAX '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = _RDB_parse_expr_to_table($3.expv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_max(RDB_table_to_expr(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

min_invocation: TOK_MIN '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = _RDB_parse_expr_to_table($3.expv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_min(RDB_table_to_expr(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

all_invocation: TOK_ALL '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = _RDB_parse_expr_to_table($3.expv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_all(RDB_table_to_expr(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

any_invocation: TOK_ANY '(' expression_list ')' {
        if ($3.expc == 0 || $3.expc > 2) {
            YYERROR;
        } else {
            RDB_table *tbp = _RDB_parse_expr_to_table($3.expv[0]);
            char *attrname = NULL;

            if (tbp == NULL)
                YYERROR;

            if ($3.expc == 2) {
                if ($3.expv[1]->kind != RDB_EX_ATTR)
                    YYERROR;
                attrname = $3.expv[1]->var.attrname;
            }

            $$ = RDB_expr_any(RDB_table_to_expr(tbp), attrname);
            if ($$ == NULL)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        }
    }
    ;

integer_invocation: TOK_INTEGER  '(' expression ')' {
        $$ = RDB_to_int($3);
        if ($$ == NULL)
            YYERROR;
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

rational_invocation: TOK_RATIONAL  '(' expression ')' {
        $$ = RDB_to_rational($3);
        if ($$ == NULL)
            YYERROR;
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

string_invocation: TOK_STRING  '(' expression ')' {
        $$ = RDB_to_string($3);
        if ($$ == NULL)
            YYERROR;
        _RDB_parse_remove_exp($3);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    ;

operator_invocation: TOK_ID '(' ')' {
        _RDB_parse_ret = RDB_ro_op($1->var.attrname, 0, NULL, _RDB_parse_txp, &$$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | TOK_ID '(' expression_list ')' {
        if ($3.expc == 1
                && strlen($1->var.attrname) > 4
                && strncmp($1->var.attrname, "THE_", 4) == 0) {
            int i;

            /* THE_ operator - requires special treatment */
            $$ = RDB_expr_comp($3.expv[0], $1->var.attrname + 4);
            if ($$ == NULL)
                YYERROR;
            for (i = 0; i < $3.expc; i++)
                _RDB_parse_remove_exp($3.expv[0]);
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
        } else {
            int i;
            RDB_table *tbp;

            for (i = 0; i < $3.expc; i++) {
                tbp = _RDB_parse_expr_to_table($3.expv[i]);
                if (tbp != NULL) {
                   $3.expv[i] = RDB_table_to_expr(tbp);

                   _RDB_parse_ret = _RDB_parse_add_exp($3.expv[i]);
                   if (_RDB_parse_ret != RDB_OK)
                       YYERROR;
                }
            }

            _RDB_parse_ret = RDB_ro_op($1->var.attrname, $3.expc, $3.expv,
                    _RDB_parse_txp, &$$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
            _RDB_parse_ret = _RDB_parse_add_exp($$);
            if (_RDB_parse_ret != RDB_OK)
                YYERROR;
            for (i = 0; i < $3.expc; i++) {
                _RDB_parse_remove_exp($3.expv[i]);
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
        RDB_hashmap_iter hiter;
        RDB_object *attrp;
        char *key;
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

        RDB_init_hashmap_iter(&hiter, &tplp->var.tpl_map);
        for (i = 0; i < attrc; i++) {
            /* Get next attribute */
            attrp = (RDB_object *) RDB_hashmap_next(&hiter, &key, NULL);

            attrv[i].name = key;
            if (attrp->typ == NULL) {
                _RDB_parse_ret = RDB_NOT_SUPPORTED;
                free(attrv);
                YYERROR;
            }
            attrv[i].typ = attrp->typ;
            attrv[i].defaultp = NULL;
        }
        RDB_destroy_hashmap_iter(&hiter);        

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
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
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
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | TOK_TABLE_DUM {
        RDB_expression *exp = table_dum_expr();

        if (exp == NULL)
            YYERROR;
        $$ = exp;
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
    }
    | TOK_TUPLE '{' '}' {
        RDB_object obj;

        RDB_init_obj(&obj);

        $$ = RDB_obj_to_expr(&obj);
        RDB_destroy_obj(&obj);
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
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
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
            YYERROR;
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
        _RDB_parse_ret = _RDB_parse_add_exp($$);
        if (_RDB_parse_ret != RDB_OK)
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

opt_expression_list:
    | expression_list
    ;

*/
%%

RDB_table *
RDB_get_ltable(void *arg);

RDB_table *
_RDB_parse_expr_to_table(const RDB_expression *exp)
{
    RDB_object val;
    RDB_table *tbp;

    if (exp->kind == RDB_EX_ATTR) {
        RDB_table *tbp;

        /* Try to find local table first */
        tbp = (*_RDB_parse_ltfp)(exp->var.attrname, _RDB_parse_arg);
        if (tbp != NULL)
            return tbp;

        /* Local table not found, try to find global table */
        _RDB_parse_ret = RDB_get_table(exp->var.attrname, _RDB_parse_txp, &tbp);
        if (_RDB_parse_ret != RDB_OK)
            return NULL;
        return tbp;
    }

    RDB_init_obj(&val);
    _RDB_parse_ret = RDB_evaluate((RDB_expression *) exp, NULL, _RDB_parse_txp, &val);
    if (_RDB_parse_ret != RDB_OK) {
        RDB_destroy_obj(&val);
        return NULL;
    }

    if (val.kind != RDB_OB_TABLE) {
        _RDB_parse_ret = RDB_INVALID_ARGUMENT;
        RDB_destroy_obj(&val);
        return NULL;
    }
    tbp = val.var.tbp;
    RDB_destroy_obj(&val);
    return tbp;
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

int
_RDB_parse_add_exp(RDB_expression *exp)
{
    explink *exlp = malloc(sizeof(explink));

    if (exlp == NULL)
        return RDB_NO_MEMORY;

    exlp->exp = exp;
    exlp->nextp = _RDB_parse_first_exp;
    _RDB_parse_first_exp = exlp;
    return RDB_OK;
}

void
_RDB_parse_remove_exp(RDB_expression *exp)
{
    /*
     * Find link
     */
    explink *prev_exlp = NULL;
    explink *exlp = _RDB_parse_first_exp;

    while (exlp->exp != exp) {
        prev_exlp = exlp;
        exlp = exlp->nextp;
    }

    /*
     * Remove link
     */
    if (prev_exlp == NULL) {
        _RDB_parse_first_exp = exlp->nextp;
    } else {
        prev_exlp->nextp = exlp->nextp;
    }
    free(exlp);
}

void
_RDB_parse_free(void)
{
    explink *nextlp;
    explink *exlp = _RDB_parse_first_exp;

    while (exlp != NULL) {
        nextlp = exlp->nextp;
        RDB_drop_expr(exlp->exp);
        free(exlp);
        exlp = nextlp;
    }
    _RDB_parse_first_exp = NULL;
}
