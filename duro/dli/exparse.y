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
extern RDB_getobjfn *_RDB_parse_getobjfp;
extern void *_RDB_parse_arg;
extern RDB_exec_context *_RDB_parse_ecp;
extern void *_RDB_parse_lookup_arg;

RDB_expression *
_RDB_parse_lookup_table(RDB_expression *exp);

typedef struct {
    RDB_expression *namexp;
    RDB_type *typ;
} parse_attribute;

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

int
yylex(void);

void
yyerror(const char *);

static RDB_parse_statement *
new_call(char *name, RDB_expr_list *explistp) {
    int ret;
    RDB_parse_statement *stmtp = malloc(sizeof(RDB_parse_statement));
    if (stmtp == NULL)
        return NULL;

    stmtp->kind = RDB_STMT_CALL;
    RDB_init_obj(&stmtp->var.call.opname);
  	ret = RDB_string_to_obj(&stmtp->var.call.opname, name, _RDB_parse_ecp);
  	if (ret != RDB_OK) {
   	    RDB_destroy_obj(&stmtp->var.call.opname, NULL);
   	    return NULL;
  	}
    stmtp->var.call.arglist = *explistp;
   	return stmtp;
}

static RDB_type *
get_type(const char *attrname, void *arg)
{
    RDB_object *objp = (*_RDB_parse_getobjfp) (attrname, arg);

    return RDB_obj_type(objp);
}

%}

%error-verbose
%locations

%union {
    RDB_expression *exp;
    RDB_expr_list explist;
    RDB_type *type;
    RDB_parse_statement *stmt;
    struct {
        RDB_parse_statement *firstp;
        RDB_parse_statement *lastp;
    } stmtlist;
    RDB_parse_assign assign;
    RDB_parse_attr_assign *assignlist;
    parse_attribute attr;
    struct {
        int attrc;
        parse_attribute attrv[DURO_MAX_LLEN];
    } attrlist;
    struct {
        RDB_parse_keydef *firstp;
        RDB_parse_keydef *lastp;
    } keylist;
}

%token TOK_START_EXP TOK_START_STMT
%token <exp> TOK_ID TOK_LIT_INTEGER TOK_LIT_STRING TOK_LIT_FLOAT TOK_LIT_BOOLEAN
%token TOK_WHERE TOK_UNION TOK_INTERSECT TOK_MINUS TOK_SEMIMINUS TOK_SEMIJOIN
        TOK_JOIN TOK_RENAME TOK_EXTEND TOK_SUMMARIZE TOK_DIVIDEBY TOK_WRAP
        TOK_UNWRAP TOK_GROUP TOK_UNGROUP
        TOK_CALL TOK_FROM TOK_TUPLE TOK_RELATION TOK_BUT TOK_AS TOK_PER TOK_VAR
        TOK_DROP TOK_INIT TOK_ADD TOK_BEGIN TOK_TX TOK_REAL TOK_VIRTUAL TOK_KEY
        TOK_COMMIT TOK_ROLLBACK
        TOK_MATCHES TOK_IN TOK_SUBSET_OF TOK_OR TOK_AND TOK_NOT
        TOK_CONCAT TOK_NE TOK_LE TOK_GE
        TOK_COUNT TOK_SUM TOK_AVG TOK_MAX TOK_MIN TOK_ALL TOK_ANY
        TOK_SAME_TYPE_AS
        TOK_IF TOK_THEN TOK_ELSE TOK_END TOK_FOR TOK_TO TOK_WHILE
        TOK_TABLE_DEE TOK_TABLE_DUM
        TOK_ASSIGN TOK_INSERT TOK_DELETE TOK_UPDATE
        TOK_INVALID

%type <exp> expression literal ro_op_invocation count_invocation
        sum_invocation avg_invocation min_invocation max_invocation
        all_invocation any_invocation ne_tuple_item_list dot_invocation

%type <explist> expression_list ne_expression_list
        ne_attribute_name_list attribute_name_list
        extend_add_list ne_extend_add_list extend_add summarize_add
        renaming ne_renaming_list renaming_list
        summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list

%type <stmt> statement statement_body assignment

%type <type> type rel_type

%type <stmtlist> ne_statement_list

%type <assign> simple_assign assign;

%type <assignlist> ne_attr_assign_list;

%type <attr> attribute

%type <attrlist> ne_attribute_list

%type <keylist> key_list ne_key_list

%destructor {
    RDB_destroy_expr_list(&$$, _RDB_parse_ecp);
} expression_list ne_expression_list
        ne_attribute_name_list attribute_name_list
        extend_add_list ne_extend_add_list extend_add
        ne_renaming_list renaming_list renaming
        summarize_add summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list

%destructor {
    RDB_drop_expr($$, _RDB_parse_ecp);
} expression

%destructor {
    RDB_parse_del_stmt($$, _RDB_parse_ecp);
} statement statement_body assignment

%destructor {
    if (!RDB_type_is_scalar($$)) {
        RDB_drop_type($$, _RDB_parse_ecp, NULL);
    }
} type rel_type

%destructor {
    RDB_parse_del_keydef_list($$.firstp, _RDB_parse_ecp);
} key_list ne_key_list

%destructor {
    RDB_parse_del_stmtlist($$.firstp, _RDB_parse_ecp);
} ne_statement_list

%destructor {
    RDB_parse_destroy_assign(&$$, _RDB_parse_ecp);
} simple_assign assign

%destructor {
    RDB_parse_del_assignlist($$, _RDB_parse_ecp);
} ne_attr_assign_list

%destructor {
    RDB_drop_expr($$.namexp, _RDB_parse_ecp);
    if (!RDB_type_is_scalar($$.typ))
        RDB_drop_type($$.typ, _RDB_parse_ecp, NULL);
} attribute

%destructor {
    int i;

    for (i = 0; i < $$.attrc; i++) {
        RDB_drop_expr($$.attrv[i].namexp, _RDB_parse_ecp);
        if (!RDB_type_is_scalar($$.attrv[i].typ))
            RDB_drop_type($$.attrv[i].typ, _RDB_parse_ecp, NULL);
    }
} ne_attribute_list

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

statement_body: /* empty */ {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
    	$$->kind = RDB_STMT_NOOP;
    }
    | TOK_CALL TOK_ID '(' expression_list ')' {
        $$ = new_call($2->var.varname, &$4);
        RDB_drop_expr($2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | TOK_ID '(' expression_list ')' {
        $$ = new_call($1->var.varname, &$3);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    ;
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
    | TOK_VAR TOK_ID TOK_REAL rel_type ne_key_list {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_parse_del_keydef_list($5.firstp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF_REAL;
        RDB_init_obj(&$$->var.vardef_real.varname);
    	RDB_string_to_obj(&$$->var.vardef_real.varname,
    	        $2->var.varname, _RDB_parse_ecp);
        $$->var.vardef_real.typ = $4;
        $$->var.vardef_real.firstkeyp = $5.firstp;
    	$$->var.vardef_real.initexp = NULL;
    }
    | TOK_VAR TOK_ID TOK_REAL rel_type TOK_INIT expression key_list {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_drop_expr($6, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($7.firstp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF_REAL;
        RDB_init_obj(&$$->var.vardef_real.varname);
    	RDB_string_to_obj(&$$->var.vardef_real.varname,
    	        $2->var.varname, _RDB_parse_ecp);
        $$->var.vardef_real.typ = $4;
    	$$->var.vardef_real.initexp = $6;
        $$->var.vardef_real.firstkeyp = $7.firstp;
    }
    | TOK_VAR TOK_ID TOK_REAL TOK_INIT expression key_list {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF_REAL;
        RDB_init_obj(&$$->var.vardef_real.varname);
    	RDB_string_to_obj(&$$->var.vardef_real.varname,
    	        $2->var.varname, _RDB_parse_ecp);
        $$->var.vardef_real.typ = NULL;
    	$$->var.vardef_real.initexp = $5;
        $$->var.vardef_real.firstkeyp = $6.firstp;
    }
    | TOK_VAR TOK_ID TOK_VIRTUAL expression {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF_VIRTUAL;
        RDB_init_obj(&$$->var.vardef_real.varname);
    	RDB_string_to_obj(&$$->var.vardef_virtual.varname,
    	        $2->var.varname, _RDB_parse_ecp);
    	/* !! */
        $$->var.vardef_virtual.exp = $4;
    }
    | TOK_DROP TOK_VAR TOK_ID {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DROP;
        RDB_init_obj(&$$->var.vardrop.varname);
    	RDB_string_to_obj(&$$->var.vardef_virtual.varname,
    	        $3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
    	/* !! */
    }
    | assignment
    | TOK_BEGIN TOK_TX {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_BEGIN_TX;
    }    
    | TOK_COMMIT {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_COMMIT;
    }    
    | TOK_ROLLBACK {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_ROLLBACK;
    }    

ne_key_list: TOK_KEY '{' attribute_name_list '}' {
        RDB_parse_keydef *kdp = malloc(sizeof(RDB_parse_keydef));
        if (kdp == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        kdp->attrlist = $3;
        kdp->nextp = NULL;
        $$.firstp = $$.lastp = kdp;
    }
    | ne_key_list TOK_KEY '{' attribute_name_list '}' {
        RDB_parse_keydef *kdp = malloc(sizeof(RDB_parse_keydef));
        if (kdp == NULL) {
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        kdp->attrlist = $4;
        kdp->nextp = NULL;
        $$.firstp = $1.firstp;
        $1.lastp->nextp = kdp;
        $$.lastp = kdp;
    }   

key_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_key_list {
        $$ = $1;
    }

assignment: assign {
        $$ = malloc(sizeof(RDB_parse_statement));
        if ($$ == NULL) {
        	RDB_drop_expr($1.var.copy.dstp, _RDB_parse_ecp);
        	RDB_drop_expr($1.var.copy.srcp, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_ASSIGN;
        $$->var.assignment.ac = 1;
        $$->var.assignment.av[0].kind = $1.kind;
        switch ($$->var.assignment.av[0].kind) {
            case RDB_STMT_COPY:
                $$->var.assignment.av[0].var.copy.dstp = $1.var.copy.dstp;
                $$->var.assignment.av[0].var.copy.srcp = $1.var.copy.srcp;
                break;
            case RDB_STMT_INSERT:
                $$->var.assignment.av[0].var.ins.dstp = $1.var.ins.dstp;
                $$->var.assignment.av[0].var.ins.srcp = $1.var.ins.srcp;
                break;
            case RDB_STMT_UPDATE:
                $$->var.assignment.av[0].var.upd.dstp = $1.var.upd.dstp;
                $$->var.assignment.av[0].var.upd.condp = $1.var.upd.condp;
                $$->var.assignment.av[0].var.upd.assignlp = $1.var.upd.assignlp;
                break;
            case RDB_STMT_DELETE:
                $$->var.assignment.av[0].var.del.dstp = $1.var.del.dstp;
                $$->var.assignment.av[0].var.del.condp = $1.var.del.condp;
                break;
        }
        $$->var.assignment.av[0].var.copy.dstp = $1.var.copy.dstp;
        $$->var.assignment.av[0].var.copy.srcp = $1.var.copy.srcp;
    }
    | assignment ',' assign {
        $$ = $1;
        $$->var.assignment.av[$1->var.assignment.ac] = $3;
        $$->var.assignment.ac++;
    }

assign: simple_assign
    | TOK_INSERT TOK_ID expression {
        $$.kind = RDB_STMT_INSERT;
        $$.var.ins.dstp = $2;
        $$.var.ins.srcp = $3;
    }
    | TOK_DELETE TOK_ID {
        $$.kind = RDB_STMT_DELETE;
        $$.var.del.dstp = $2;
        $$.var.del.condp = NULL;
    }
    | TOK_DELETE TOK_ID TOK_WHERE expression {
        $$.kind = RDB_STMT_DELETE;
        $$.var.del.dstp = $2;
        $$.var.del.condp = $4;
    }
    | TOK_UPDATE TOK_ID '{' ne_attr_assign_list '}' {
        $$.kind = RDB_STMT_UPDATE;
        $$.var.upd.dstp = $2;
        $$.var.upd.assignlp = $4;
        $$.var.upd.condp = NULL;
    }
    | TOK_UPDATE TOK_ID TOK_WHERE expression '{' ne_attr_assign_list '}' {
        $$.kind = RDB_STMT_UPDATE;
        $$.var.upd.dstp = $2;
        $$.var.upd.assignlp = $6;
        $$.var.upd.condp = $4;
    }

ne_attr_assign_list: simple_assign {
		$$ = malloc(sizeof (RDB_parse_attr_assign));
		/* !! */
		$$->dstp = $1.var.copy.dstp;
		$$->srcp = $1.var.copy.srcp;
		$$->nextp = NULL;
    }
	| ne_attr_assign_list ',' simple_assign {
		$$ = malloc(sizeof (RDB_parse_attr_assign));

        $$->dstp = $3.var.copy.dstp;
        $$->srcp = $3.var.copy.srcp;
        $$->nextp = $1;
	}

simple_assign: TOK_ID TOK_ASSIGN expression {
        $$.kind = RDB_STMT_COPY;
        $$.var.copy.dstp = $1;
        $$.var.copy.srcp = $3;
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
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("PROJECT", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_join_expr_lists(&$$->var.op.args, &$3);
    }
    | expression '{' TOK_ALL TOK_BUT attribute_name_list '}' {
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_destroy_expr_list(&$5, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("REMOVE", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$5, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_join_expr_lists(&$$->var.op.args, &$5);
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

        $$ = RDB_ro_op("WHERE", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_add_arg($$, $3);
    }
    | expression TOK_RENAME '(' renaming_list ')' {
        RDB_expression *exp = _RDB_parse_lookup_table($1);
        if (exp == NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("RENAME", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, exp);
        RDB_join_expr_lists(&$$->var.op.args, &$4);
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

        $$ = RDB_ro_op("UNION", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("INTERSECT", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("MINUS", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("SEMIMINUS", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("SEMIJOIN", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("JOIN", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
    }
    | TOK_EXTEND expression TOK_ADD '(' extend_add_list ')' {
        RDB_expression *texp = _RDB_parse_lookup_table($2);
        if (texp == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
 	        RDB_destroy_expr_list(&$5, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("EXTEND", _RDB_parse_ecp);
        if ($$ == NULL) {
 	        RDB_drop_expr(texp, _RDB_parse_ecp);
 	        RDB_destroy_expr_list(&$5, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_join_expr_lists(&$$->var.op.args, &$5);
    }
    | TOK_UPDATE expression '{' ne_attr_assign_list '}' {
        RDB_parse_attr_assign *ap, *hap;
        RDB_expression *texp = _RDB_parse_lookup_table($2);
        if (texp == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_parse_del_assignlist($4, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("UPDATE", _RDB_parse_ecp);
        if ($$ == NULL) {
 	        RDB_drop_expr(texp, _RDB_parse_ecp);
            RDB_parse_del_assignlist($4, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        ap = $4;
        do {
            if (ap->dstp->kind != RDB_EX_VAR) {
                RDB_raise_invalid_argument("invalid UPDATE argument",
                        _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, RDB_string_to_expr(ap->dstp->var.varname,
                    _RDB_parse_ecp));
            RDB_add_arg($$, ap->srcp);
            RDB_drop_expr(ap->dstp, _RDB_parse_ecp);
            hap = ap;
            ap = ap->nextp;
            free(hap);
        } while (ap != NULL);
    }
    | TOK_SUMMARIZE expression TOK_PER expression
           TOK_ADD '(' summarize_add_list ')' {
        RDB_expression *tex1p, *tex2p;

        tex1p = _RDB_parse_lookup_table($2);
        if (tex1p == NULL)
        {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$7, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = _RDB_parse_lookup_table($4);
        if (tex2p == NULL)
        {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$7, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("SUMMARIZE", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr(tex2p, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$7, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, tex1p);
        RDB_add_arg($$, tex2p);
        RDB_join_expr_lists(&$$->var.op.args, &$7);
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

        $$ = RDB_ro_op("DIVIDE", _RDB_parse_ecp);
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
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("WRAP", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_join_expr_lists(&$$->var.op.args, &$4);
    }
    | expression TOK_UNWRAP '(' attribute_name_list ')' {
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("UNWRAP", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }

        RDB_add_arg($$, texp);
        RDB_join_expr_lists(&$$->var.op.args, &$4);
    }
    | expression TOK_GROUP '{' attribute_name_list '}' TOK_AS TOK_ID {
        RDB_expression *lexp;
        RDB_expression *texp = _RDB_parse_lookup_table($1);
        if (texp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            RDB_drop_expr($7, _RDB_parse_ecp);
            YYERROR;
        }
        $$ = RDB_ro_op("GROUP", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(texp, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            RDB_drop_expr($7, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, texp);
        RDB_join_expr_lists(&$$->var.op.args, &$4);
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
        $$ = RDB_ro_op("UNGROUP", _RDB_parse_ecp);
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
        $$ = RDB_ro_op("OR", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_AND expression {
        $$ = RDB_ro_op("AND", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | TOK_NOT expression {
        $$ = RDB_ro_op("NOT", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $2);
    }
    | expression '=' expression {
        $$ = RDB_ro_op("=", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_NE expression {
        $$ = RDB_ro_op("<>", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_GE expression {
        $$ = RDB_ro_op(">=", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_LE expression {
        $$ = RDB_ro_op("<=", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '>' expression {
        $$ = RDB_ro_op(">", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '<' expression {
        $$ = RDB_ro_op("<", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("IN", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(exp, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, exp);
        RDB_add_arg($$, $3);
    }
    | expression TOK_MATCHES expression {
        $$ = RDB_ro_op("MATCHES", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("SUBSET_OF", _RDB_parse_ecp);
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
        $$ = RDB_ro_op("-", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $2);
    }
    | expression '+' expression {
        $$ = RDB_ro_op("+", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '-' expression {
        $$ = RDB_ro_op("-", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression TOK_CONCAT expression {
        $$ = RDB_ro_op("||", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }            
    | expression '*' expression {
        $$ = RDB_ro_op("*", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | expression '/' expression {
        $$ = RDB_ro_op("/", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
    }
    | TOK_ID
    | dot_invocation
    | literal
    | count_invocation
    | sum_invocation
    | avg_invocation
    | max_invocation
    | min_invocation
    | all_invocation
    | any_invocation
    | ro_op_invocation
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

        $$ = RDB_ro_op("TO_TUPLE", _RDB_parse_ecp);
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

        $$ = RDB_ro_op("IF", _RDB_parse_ecp);
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

dot_invocation: expression '.' TOK_ID {
        $$ = RDB_tuple_attr($1, $3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$ == NULL)
            RDB_drop_expr($1, _RDB_parse_ecp);
    }

attribute_name_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_attribute_name_list
    ;

ne_attribute_name_list: TOK_ID {
        $$.firstp = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$.firstp == NULL) {
            YYERROR;
        }
        $$.lastp = $$.firstp;
        $$.firstp->nextp = NULL;
    }
    | ne_attribute_name_list ',' TOK_ID {
        $$.firstp = $1.firstp;
        $1.lastp->nextp = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($1.lastp->nextp == NULL) {
            RDB_destroy_expr_list(&$1, _RDB_parse_ecp);
            YYERROR;
        }
        $$.lastp = $1.lastp->nextp;
        $$.lastp->nextp = NULL;
    }
    ;

renaming_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_renaming_list
    ;

ne_renaming_list: renaming
    | ne_renaming_list ',' renaming {
        $$.firstp = $1.firstp;
        $1.lastp->nextp = $3.firstp;
        $$.lastp = $3.lastp;
    }
    ;

renaming: TOK_ID TOK_AS TOK_ID {
    	$$.firstp = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
    	$$.lastp = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
    	RDB_drop_expr($1, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$.firstp == NULL || $$.lastp == NULL) {
            if ($$.firstp != NULL)
                RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            if ($$.lastp != NULL)
                RDB_drop_expr($$.lastp, _RDB_parse_ecp);
            YYERROR;
        }
    	
    	$$.firstp->nextp = $$.lastp;
    	$$.lastp->nextp = NULL;
    }
/*
    | "PREFIX" STRING AS STRING
    | "SUFFIX" STRING AS STRING
*/
    ;


extend_add_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_extend_add_list
    ;

ne_extend_add_list: extend_add
    | ne_extend_add_list ',' extend_add {
        $$.firstp = $1.firstp;
        $1.lastp->nextp = $3.firstp;
        $$.lastp = $3.lastp;
    }
    ;

extend_add: expression TOK_AS TOK_ID {
        $$.firstp = $1;
        $$.lastp = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            YYERROR;
        }
        
        $1->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    ;

summarize_add_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_summarize_add_list
    ;

ne_summarize_add_list: summarize_add
    | ne_summarize_add_list ',' summarize_add {
        $$.firstp = $1.firstp;
        $1.lastp->nextp = $3.firstp;
        $$.lastp = $3.lastp;
    }
    ;

summarize_add: TOK_COUNT '(' ')' TOK_AS TOK_ID {
        $$.firstp = RDB_ro_op("COUNT", _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        $$.lastp = RDB_string_to_expr($5->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($5, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    | TOK_SUM '(' expression ')' TOK_AS TOK_ID {
        $$.firstp = RDB_ro_op("SUM", _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        RDB_add_arg($$.firstp, $3);
        $$.lastp = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    | TOK_AVG '(' expression ')' TOK_AS TOK_ID {
        $$.firstp = RDB_ro_op("AVG", _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        RDB_add_arg($$.firstp, $3);
        $$.lastp = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    | TOK_MAX '(' expression ')' TOK_AS TOK_ID {
        $$.firstp = RDB_ro_op("MAX", _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        RDB_add_arg($$.firstp, $3);
        $$.lastp = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    | TOK_MIN '(' expression ')' TOK_AS TOK_ID {
        $$.firstp = RDB_ro_op("MIN", _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        RDB_add_arg($$.firstp, $3);
        $$.lastp = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    | TOK_ALL '(' expression ')' TOK_AS TOK_ID {
        $$.firstp = RDB_ro_op("ALL", _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        RDB_add_arg($$.firstp, $3);
        $$.lastp = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    | TOK_ANY '(' expression ')' TOK_AS TOK_ID {
        $$.firstp = RDB_ro_op("ANY", _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        RDB_add_arg($$.firstp, $3);
        $$.lastp = RDB_string_to_expr($6->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($6, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    ;

wrapping_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_wrapping_list
    ;

ne_wrapping_list: wrapping
    | ne_wrapping_list ',' wrapping {
        $$.firstp = $1.firstp;
        $1.lastp->nextp = $3.firstp;
        $$.lastp = $3.lastp;
    }
    ;

wrapping: '{' attribute_name_list '}' TOK_AS TOK_ID {
        int i;
        RDB_expression *exp;

        /* Empty object, used to create an expression */
        RDB_object eobj; 

        RDB_init_obj(&eobj);

        $$.firstp = RDB_obj_to_expr(&eobj, _RDB_parse_ecp);
        RDB_destroy_obj(&eobj, _RDB_parse_ecp);
        if ($$.firstp == NULL) {
            RDB_destroy_expr_list(&$2, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }

        if (RDB_set_array_length(RDB_expr_obj($$.firstp),
                (RDB_int) RDB_expr_list_length(&$2), _RDB_parse_ecp) != RDB_OK) {
            RDB_destroy_expr_list(&$2, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_expr_obj($$.firstp)->typ = RDB_create_array_type(&RDB_STRING,
                _RDB_parse_ecp); /* !! */

        exp = $2.firstp;
        i = 0;
        while (exp != NULL) {
            if (RDB_array_set(RDB_expr_obj($$.firstp),
                    (RDB_int) i, RDB_expr_obj(exp), _RDB_parse_ecp) != RDB_OK) {
	            RDB_destroy_expr_list(&$2, _RDB_parse_ecp);
                RDB_drop_expr($5, _RDB_parse_ecp);
                RDB_drop_expr($$.firstp, _RDB_parse_ecp);
                YYERROR;
            }
            exp = exp->nextp;
            i++;
        }

        $$.lastp = RDB_string_to_expr($5->var.varname, _RDB_parse_ecp);
        RDB_destroy_expr_list(&$2, _RDB_parse_ecp);
        RDB_drop_expr($5, _RDB_parse_ecp);
        if ($$.lastp == NULL) {
            RDB_drop_expr($$.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    ;

count_invocation: TOK_COUNT '(' expression ')' {
        RDB_expression *exp = _RDB_parse_lookup_table($3);
        if (exp == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        $$ = RDB_ro_op("COUNT", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, exp);
    }
    ;

sum_invocation: TOK_SUM '(' ne_expression_list ')' {
        int len = RDB_expr_list_length(&$3);

        if (len > 2) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid SUM arguments",
                    _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *arg2p = $3.firstp->nextp;
            RDB_expression *texp = _RDB_parse_lookup_table($3.firstp);
            if (texp == NULL) {
                RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
	            YYERROR;
            }

            $$ = RDB_ro_op("SUM", _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if (len == 2) {
                RDB_add_arg($$, arg2p);
            }
        }
    }
    ;

avg_invocation: TOK_AVG '(' ne_expression_list ')' {
        int len = RDB_expr_list_length(&$3);

        if (len > 2) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid AVG arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *arg2p = $3.firstp->nextp;
            RDB_expression *texp = _RDB_parse_lookup_table($3.firstp);
            if (texp == NULL) {
	            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("AVG", _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if (len == 2) {
                RDB_add_arg($$, arg2p);
            }
        }
    }
    ;

max_invocation: TOK_MAX '(' ne_expression_list ')' {
        int len = RDB_expr_list_length(&$3);

        if (len > 2) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid MAX arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *arg2p = $3.firstp->nextp;
            RDB_expression *texp = _RDB_parse_lookup_table($3.firstp);
            if (texp == NULL) {
                RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("MAX", _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if (len == 2) {
                RDB_add_arg($$, arg2p);
            }
        }
    }
    ;

min_invocation: TOK_MIN '(' ne_expression_list ')' {
        int len = RDB_expr_list_length(&$3);

        if (len > 2) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid MIN arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *arg2p = $3.firstp->nextp;
            RDB_expression *texp = _RDB_parse_lookup_table($3.firstp);
            if (texp == NULL) {
	            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("MIN", _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if (len == 2) {
                RDB_add_arg($$, arg2p);
            }
        }
    }
    ;

all_invocation: TOK_ALL '(' ne_expression_list ')' {
        int len = RDB_expr_list_length(&$3);

        if (len > 2) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid ALL arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *arg2p = $3.firstp->nextp;
            RDB_expression *texp = _RDB_parse_lookup_table($3.firstp);
            if (texp == NULL) {
	            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("ALL", _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if (len == 2) {
                RDB_add_arg($$, arg2p);
            }
        }
    }
    ;

any_invocation: TOK_ANY '(' ne_expression_list ')' {
        int len = RDB_expr_list_length(&$3);

        if (len > 2) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_invalid_argument("invalid ANY arguments",
                   _RDB_parse_ecp);
            YYERROR;
        } else {
            RDB_expression *arg2p = $3.firstp->nextp;
            RDB_expression *texp = _RDB_parse_lookup_table($3.firstp);
            if (texp == NULL) {
	            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }

            $$ = RDB_ro_op("ANY", _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr(texp, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, texp);
            if (len == 2) {
                RDB_add_arg($$, arg2p);
            }
        }
    }
    ;

ro_op_invocation: TOK_ID '(' expression_list ')' {
        int len = RDB_expr_list_length(&$3);
        if (len == 1
                && strlen($1->var.varname) > 4
                && strncmp($1->var.varname, "THE_", 4) == 0) {
            /* THE_ operator - requires special treatment */
            $$ = RDB_expr_comp($3.firstp, $1->var.varname + 4,
                    _RDB_parse_ecp);
            RDB_drop_expr($1, _RDB_parse_ecp);
            if ($$ == NULL) {
                RDB_drop_expr($3.firstp, _RDB_parse_ecp);
                YYERROR;
            }
        } else {
            RDB_expression *argp;
            $$ = RDB_ro_op($1->var.varname, _RDB_parse_ecp);
            RDB_drop_expr($1, _RDB_parse_ecp);
            if ($$ == NULL) {
	            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }
            argp = $3.firstp;
            while (argp != NULL)
            {
                RDB_expression *nextp = argp->nextp;
                RDB_expression *exp = _RDB_parse_lookup_table(argp);
                if (exp == NULL) {
                    RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                    YYERROR;
                }
            	RDB_add_arg($$, exp);
                argp = nextp;
            }
        }
    }
    ;

ne_expression_list: expression {
        $$.firstp = $$.lastp = $1;
        $1->nextp = NULL;
    }
    | ne_expression_list ',' expression {
        $$.firstp = $1.firstp;
        $1.lastp->nextp = $$.lastp = $3;
        $3->nextp = NULL;
    }
    ;

literal: TOK_RELATION '{' ne_expression_list '}' {
        int attrc;
        int i;
        int ret;
        RDB_attr *attrv;
        RDB_hashtable_iter hiter;
        tuple_entry *entryp;
        RDB_expression *argp;
        RDB_object obj;
        RDB_object *tplp = RDB_expr_obj($3.firstp);

        /*
         * Get type from first tuple
         */
        if (tplp == NULL)
            YYERROR;
        if (tplp->kind != RDB_OB_TUPLE) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_type_mismatch("tuple required", _RDB_parse_ecp);
            YYERROR;
        }
        attrc = RDB_tuple_size(tplp);
        attrv = malloc(sizeof (RDB_attr) * attrc);
        if (attrv == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            RDB_raise_no_memory(_RDB_parse_ecp);
            YYERROR;
        }

        RDB_init_hashtable_iter(&hiter, &tplp->var.tpl_tab);
        for (i = 0; i < attrc; i++) {
            /* Get next attribute */
            entryp = RDB_hashtable_next(&hiter);

            attrv[i].name = entryp->key;
            if (entryp->obj.typ == NULL) {
                RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
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
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            free(attrv);
            YYERROR;
        }

        ret = RDB_init_table(RDB_expr_obj($$), NULL, attrc, attrv, 0, NULL,
                _RDB_parse_ecp);
        free(attrv);
        if (ret != RDB_OK) {
            RDB_drop_expr($$, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }

        if (RDB_insert(RDB_expr_obj($$), tplp, _RDB_parse_ecp, _RDB_parse_txp)
                != RDB_OK) {
            RDB_drop_expr($$, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }

        argp = $3.firstp->nextp;
        while (argp != NULL) {
            tplp = RDB_expr_obj(argp);
            if (tplp == NULL) {
                RDB_drop_expr($$, _RDB_parse_ecp);
                RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }
            if (tplp->kind != RDB_OB_TUPLE) {
                RDB_drop_expr($$, _RDB_parse_ecp);
                RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                RDB_raise_type_mismatch("tuple required", _RDB_parse_ecp);
                YYERROR;
            }
            if (RDB_insert(RDB_expr_obj($$), tplp, _RDB_parse_ecp,
                    _RDB_parse_txp) != RDB_OK) {
                RDB_drop_expr($$, _RDB_parse_ecp);
                RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
                YYERROR;
            }
            argp = argp->nextp;
        }
        RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
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
    | TOK_SAME_TYPE_AS '(' expression ')' {
		$$ = RDB_expr_type($3, &get_type, _RDB_parse_arg, _RDB_parse_ecp,
		        _RDB_parse_txp);
		if ($$ == NULL)
		    YYERROR;
	}
/*
    | "SAME_HEADING_AS" '(' expression ')'
    ;
*/

rel_type: TOK_RELATION '{' ne_attribute_list '}' {
        int i;
        RDB_attr attrv[DURO_MAX_LLEN];

        for (i = 0; i < $3.attrc; i++) {
            attrv[i].name = $3.attrv[i].namexp->var.varname;
            attrv[i].typ = RDB_dup_nonscalar_type($3.attrv[i].typ,
            		_RDB_parse_ecp);
        }
        $$ = RDB_create_relation_type($3.attrc, attrv, _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
    }

expression_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
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

        if (_RDB_parse_getobjfp != NULL) {
	        /* Try to find local table first */
	        tbp = (*_RDB_parse_getobjfp)(exp->var.varname, _RDB_parse_arg);
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
