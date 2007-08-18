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

typedef struct parse_attribute {
    RDB_expression *namexp;
    RDB_type *typ;
    struct parse_attribute *nextp;
} parse_attribute;

typedef struct parse_possrep {
    RDB_expression *namexp;
    parse_attribute *attrlistp;
    struct parse_possrep *nextp;
} parse_possrep;

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

static int
attrlist_length(parse_attribute *attrlistp)
{
    int res = 0;
    while (attrlistp != NULL) {
        res++;
        attrlistp = attrlistp->nextp;
    }
    return res;
}

static RDB_parse_statement *
new_deftype(const char *name, parse_possrep *possreplistp,
        RDB_expression *constrp)
{
    int i;
    parse_possrep *rep;
    parse_attribute *attrlistp;
    int repc = 0;
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL) {
        return NULL;
    }

    stmtp->kind = RDB_STMT_TYPE_DEF;
    rep = possreplistp;
    while (rep != NULL) {
        repc++;
        rep = rep->nextp;
    }

    stmtp->var.deftype.repc = repc;
    stmtp->var.deftype.repv = RDB_alloc(sizeof(RDB_possrep) * repc, _RDB_parse_ecp);
    if (stmtp->var.deftype.repv == NULL) {
   	    RDB_free(stmtp);
        return NULL;
    }
    rep = possreplistp;
    for (i = 0; i < repc; i++) {
        int j;
        int compc = attrlist_length(rep->attrlistp);

        stmtp->var.deftype.repv[i].compc = compc;
        stmtp->var.deftype.repv[i].compv = RDB_alloc(sizeof(RDB_attr) * compc,
        		_RDB_parse_ecp);
        if (stmtp->var.deftype.repv[i].compv == NULL) {
            return NULL;
        }

        attrlistp = rep->attrlistp;
        for (j = 0; j < compc; j++) {
            stmtp->var.deftype.repv[i].compv[j].name = attrlistp->namexp->var.varname;
            stmtp->var.deftype.repv[i].compv[j].typ = attrlistp->typ;
            attrlistp = attrlistp->nextp;
        }
        if (rep->namexp != NULL) {
            stmtp->var.deftype.repv[i].name = rep->namexp->var.varname;
        } else {
            stmtp->var.deftype.repv[i].name = NULL;
        }
        /* !! free attrlist */
    }
    RDB_init_obj(&stmtp->var.deftype.typename);
  	if (RDB_string_to_obj(&stmtp->var.deftype.typename, name,
  	        _RDB_parse_ecp) != RDB_OK)
  	    return NULL;
  	stmtp->var.deftype.constraintp = constrp;
    return stmtp;
}

static RDB_parse_statement *
new_var_def(const char *name, RDB_type *typ, RDB_expression *exp)
{
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL) {
       	return NULL;
    }
    stmtp->kind = RDB_STMT_VAR_DEF;
    RDB_init_obj(&stmtp->var.vardef.varname);
	if (RDB_string_to_obj(&stmtp->var.vardef.varname, name, _RDB_parse_ecp)
	        != RDB_OK) {
	    RDB_destroy_obj(&stmtp->var.vardef.varname, NULL);
	    return NULL;
	}
	stmtp->var.vardef.typ = typ;
   	stmtp->var.vardef.exp = exp;
   	return stmtp;
}

static RDB_parse_statement *
new_var_def_real(const char *name, RDB_type *rtyp, RDB_expression *exp,
        RDB_parse_keydef *firstkeyp)
{
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL) {
       	return NULL;
    }
    stmtp->kind = RDB_STMT_VAR_DEF_REAL;
    RDB_init_obj(&stmtp->var.vardef.varname);
	if (RDB_string_to_obj(&stmtp->var.vardef.varname, name, _RDB_parse_ecp)
	        != RDB_OK) {
	    RDB_destroy_obj(&stmtp->var.vardef.varname, NULL);
	    return NULL;
	}
	stmtp->var.vardef.typ = rtyp;
   	stmtp->var.vardef.exp = exp;
   	stmtp->var.vardef.firstkeyp = firstkeyp;
   	return stmtp;
}

static RDB_parse_statement *
new_var_def_private(const char *name, RDB_type *rtyp, RDB_expression *exp,
        RDB_parse_keydef *firstkeyp)
{
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL) {
       	return NULL;
    }
    stmtp->kind = RDB_STMT_VAR_DEF_PRIVATE;
    RDB_init_obj(&stmtp->var.vardef.varname);
	if (RDB_string_to_obj(&stmtp->var.vardef.varname, name, _RDB_parse_ecp)
	        != RDB_OK) {
	    RDB_destroy_obj(&stmtp->var.vardef.varname, NULL);
	    return NULL;
	}
	stmtp->var.vardef.typ = rtyp;
   	stmtp->var.vardef.exp = exp;
   	stmtp->var.vardef.firstkeyp = firstkeyp;
   	return stmtp;
}

static RDB_parse_statement *
new_ro_op_def(const char *name, parse_attribute *arglistp, RDB_type *rtyp,
		RDB_parse_statement *stmtlistp)
{
    int i;
    parse_attribute *attrlistp;
    int argc = attrlist_length(arglistp);
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL)
        return NULL;

    stmtp->kind = RDB_STMT_RO_OP_DEF;
    stmtp->var.opdef.rtyp = rtyp;
    RDB_init_obj(&stmtp->var.opdef.opname);
    stmtp->var.opdef.argv = NULL;
    stmtp->var.opdef.bodyp = stmtlistp;
   	if (RDB_string_to_obj(&stmtp->var.opdef.opname, name, _RDB_parse_ecp)
      	        != RDB_OK) {
      	goto error;
  	}
  	stmtp->var.opdef.argc = argc;
  	stmtp->var.opdef.argv = RDB_alloc(argc * sizeof(RDB_parse_arg), _RDB_parse_ecp);
  	if (stmtp->var.opdef.argv == NULL)
  	    goto error;
  	attrlistp = arglistp;
    for (i = 0; i < argc; i++) {
        RDB_init_obj(&stmtp->var.opdef.argv[i].name);
        if (RDB_string_to_obj(&stmtp->var.opdef.argv[i].name,
                arglistp->namexp->var.varname, _RDB_parse_ecp) != RDB_OK) {
      	    goto error;
      	}
      	stmtp->var.opdef.argv[i].typ = arglistp->typ;
      	arglistp = arglistp->nextp;
  	}

  	return stmtp;

error:
    RDB_destroy_obj(&stmtp->var.opdef.opname, _RDB_parse_ecp);
    RDB_free(stmtp->var.opdef.argv); /* ... !! */
    RDB_parse_del_stmtlist(stmtp->var.opdef.bodyp, _RDB_parse_ecp);
    RDB_free(stmtp);
    return NULL;
}

static RDB_bool
argname_in_list(parse_attribute *argp, RDB_expr_list *listp)
{
    RDB_expression *exp = listp->firstp;
    while(exp != NULL) {
        if (strcmp(argp->namexp->var.varname, RDB_obj_string(RDB_expr_obj(exp))) == 0)
            return RDB_TRUE;
        exp = exp->nextp;
    }
    return RDB_FALSE;
}

static RDB_parse_statement *
new_update_op_def(const char *name, parse_attribute *arglistp,
        RDB_expr_list *updlistp, RDB_parse_statement *stmtlistp)
{
    int i;
    parse_attribute *attrlistp;
    int argc = attrlist_length(arglistp);
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL)
        return NULL;

    stmtp->kind = RDB_STMT_UPD_OP_DEF;
    RDB_init_obj(&stmtp->var.opdef.opname);
    stmtp->var.opdef.argv = NULL;
    stmtp->var.opdef.bodyp = stmtlistp;
   	if (RDB_string_to_obj(&stmtp->var.opdef.opname, name, _RDB_parse_ecp)
      	        != RDB_OK) {
      	goto error;
  	}
  	stmtp->var.opdef.argc = argc;
  	stmtp->var.opdef.argv = RDB_alloc(argc * sizeof(RDB_parse_arg), _RDB_parse_ecp);
  	if (stmtp->var.opdef.argv == NULL)
  	    goto error;
  	for (i = 0; i < argc; i++) {
  	    stmtp->var.opdef.argv[i].upd = RDB_FALSE;
  	}
  	attrlistp = arglistp;
    for (i = 0; i < argc; i++) {
        RDB_init_obj(&stmtp->var.opdef.argv[i].name);
        if (RDB_string_to_obj(&stmtp->var.opdef.argv[i].name,
                arglistp->namexp->var.varname, _RDB_parse_ecp) != RDB_OK) {
      	    goto error;
      	}
      	stmtp->var.opdef.argv[i].typ = arglistp->typ;
      	if (argname_in_list(arglistp, updlistp))
      	    stmtp->var.opdef.argv[i].upd = RDB_TRUE;
      	arglistp = arglistp->nextp;
  	}

  	return stmtp;

error:
    RDB_destroy_obj(&stmtp->var.opdef.opname, _RDB_parse_ecp);
    RDB_free(stmtp->var.opdef.argv); /* ... !! */
    RDB_parse_del_stmtlist(stmtp->var.opdef.bodyp, _RDB_parse_ecp);
    RDB_free(stmtp);
    return NULL;
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
        parse_attribute *firstp;
        parse_attribute *lastp;
    } attrlist;
    struct {
        RDB_parse_keydef *firstp;
        RDB_parse_keydef *lastp;
    } keylist;
    parse_possrep possrep;
    struct {
        parse_possrep *firstp;
        parse_possrep *lastp;
    } possreplist;
}

%token TOK_START_EXP TOK_START_STMT
%token <exp> TOK_ID TOK_LIT_INTEGER TOK_LIT_STRING TOK_LIT_FLOAT TOK_LIT_BOOLEAN
%token TOK_WHERE TOK_UNION TOK_INTERSECT TOK_MINUS TOK_SEMIMINUS TOK_SEMIJOIN
        TOK_JOIN TOK_RENAME TOK_EXTEND TOK_SUMMARIZE TOK_DIVIDEBY TOK_WRAP
        TOK_UNWRAP TOK_GROUP TOK_UNGROUP
        TOK_CALL TOK_FROM TOK_TUPLE TOK_RELATION TOK_BUT TOK_AS TOK_PER TOK_VAR
        TOK_DROP TOK_INIT TOK_ADD TOK_BEGIN TOK_TX 
        TOK_REAL TOK_VIRTUAL TOK_PRIVATE TOK_KEY
        TOK_COMMIT TOK_ROLLBACK
        TOK_MATCHES TOK_IN TOK_SUBSET_OF TOK_OR TOK_AND TOK_NOT
        TOK_CONCAT TOK_NE TOK_LE TOK_GE
        TOK_COUNT TOK_SUM TOK_AVG TOK_MAX TOK_MIN TOK_ALL TOK_ANY
        TOK_SAME_TYPE_AS
        TOK_IF TOK_THEN TOK_ELSE TOK_END TOK_FOR TOK_TO TOK_WHILE
        TOK_TABLE_DEE TOK_TABLE_DUM
        TOK_ASSIGN TOK_INSERT TOK_DELETE TOK_UPDATE
        TOK_TYPE TOK_POSSREP TOK_CONSTRAINT TOK_OPERATOR TOK_RETURNS TOK_UPDATES
        TOK_RETURN
        TOK_INVALID

%type <exp> expression literal ro_op_invocation count_invocation
        sum_invocation avg_invocation min_invocation max_invocation
        all_invocation any_invocation dot_invocation

%type <explist> expression_list ne_expression_list
        ne_id_list id_list ne_tuple_item_list
        extend_add_list ne_extend_add_list extend_add summarize_add
        renaming ne_renaming_list renaming_list
        summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list

%type <stmt> statement statement_body assignment

%type <type> type

%type <stmtlist> ne_statement_list

%type <assign> simple_assign assign;

%type <assignlist> ne_attr_assign_list;

%type <attr> attribute

%type <attrlist> attribute_list ne_attribute_list

%type <keylist> key_list ne_key_list

%type <possrep> possrep_def

%type <possreplist> possrep_def_list;

%destructor {
    RDB_destroy_expr_list(&$$, _RDB_parse_ecp);
} expression_list ne_expression_list
        ne_id_list id_list
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
} type

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
    parse_attribute *attrp = $$.firstp;
    while (attrp != NULL) {
        if (!RDB_type_is_scalar(attrp->typ))
            RDB_drop_type(attrp->typ, _RDB_parse_ecp, NULL);
        RDB_drop_expr(attrp->namexp, _RDB_parse_ecp);
        attrp = attrp->nextp;
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
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($4.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_IF;
        $$->var.ifthen.condp = $2;
        $$->var.ifthen.ifp = $4.firstp;
    	$$->var.ifthen.elsep = NULL;
    }
    | TOK_IF expression TOK_THEN ne_statement_list TOK_ELSE
            ne_statement_list TOK_END TOK_IF {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($4.firstp, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($6.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_IF;
        $$->var.ifthen.condp = $2;
        $$->var.ifthen.ifp = $4.firstp;
    	$$->var.ifthen.elsep = $6.firstp;
    }
    | TOK_FOR TOK_ID TOK_ASSIGN expression TOK_TO expression ';'
            ne_statement_list TOK_END TOK_FOR {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_drop_expr($6, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($8.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_FOR;
        $$->var.forloop.varexp = $2;
        $$->var.forloop.fromp = $4;
        $$->var.forloop.top = $6;
        $$->var.forloop.bodyp = $8.firstp;
    }
    | TOK_WHILE expression ';' ne_statement_list TOK_END TOK_WHILE {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($4.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_WHILE;
        $$->var.whileloop.condp = $2;
        $$->var.whileloop.bodyp = $4.firstp;
    }
    | TOK_OPERATOR TOK_ID '(' attribute_list ')' TOK_RETURNS type
            ne_statement_list TOK_END TOK_OPERATOR {
        $$ = new_ro_op_def($2->var.varname, $4.firstp, $7, $8.firstp);
        if ($$ == NULL)
            YYERROR;
    }
    | TOK_OPERATOR TOK_ID '(' attribute_list ')' TOK_UPDATES '{' id_list '}'
            ne_statement_list TOK_END TOK_OPERATOR {
        $$ = new_update_op_def($2->var.varname, $4.firstp, &$8, $10.firstp);
        if ($$ == NULL)
            YYERROR;
    }

possrep_def: TOK_POSSREP '{' attribute_list '}' {
	    $$.namexp = NULL;
        $$.attrlistp = $3.firstp;
	}
	| TOK_POSSREP TOK_ID '{' attribute_list '}' {
	    $$.namexp = $2;
        $$.attrlistp = $4.firstp;
	}

possrep_def_list: possrep_def {
        $$.firstp = RDB_alloc(sizeof(parse_possrep), _RDB_parse_ecp);
	    if ($$.firstp == NULL)
	        YYERROR;
        $$.firstp->attrlistp = $1.attrlistp;
        $$.firstp->namexp = $1.namexp;
        $$.firstp->nextp = NULL;
        $$.lastp = $$.firstp;
    }
	| possrep_def_list possrep_def {
	    $1.lastp->nextp = RDB_alloc(sizeof(parse_possrep), _RDB_parse_ecp);
	    if ($1.lastp->nextp == NULL)
	        YYERROR;
	    $1.lastp->nextp->attrlistp = $2.attrlistp;
	    $1.lastp->nextp->namexp = $2.namexp;
	    $$.firstp = $1.firstp;
	    $$.lastp = $1.lastp->nextp;
	}

statement_body: /* empty */ {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
    	$$->kind = RDB_STMT_NOOP;
    }
    | TOK_CALL TOK_ID '(' expression_list ')' {
        $$ = RDB_parse_new_call($2->var.varname, &$4);
        RDB_drop_expr($2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | TOK_ID '(' expression_list ')' {
        $$ = RDB_parse_new_call($1->var.varname, &$3);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | TOK_VAR TOK_ID type {
        $$ = new_var_def($2->var.varname, $3, NULL);
        if ($$ == NULL) {
            if (!RDB_type_is_scalar($3)) {
                RDB_drop_type($3, _RDB_parse_ecp, NULL);
            }
            YYERROR;
        }
    }
    | TOK_VAR TOK_ID type TOK_INIT expression {
        $$ = new_var_def($2->var.varname, $3, $5);
        if ($$ == NULL) {
            if (!RDB_type_is_scalar($3)) {
                RDB_drop_type($3, _RDB_parse_ecp, NULL);
            }
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_INIT expression {
        $$ = new_var_def($2->var.varname, NULL, $4);
        if ($$ == NULL) {
            RDB_drop_expr($4, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_REAL type ne_key_list {
        if (!RDB_type_is_relation($4)) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_parse_del_keydef_list($5.firstp, _RDB_parse_ecp);
            RDB_raise_type_mismatch("relation type required", _RDB_parse_ecp);
            YYERROR;
        }
        $$ = new_var_def_real($2->var.varname, $4, NULL, $5.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_parse_del_keydef_list($5.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_REAL type TOK_INIT expression key_list {
        if (!RDB_type_is_relation($4)) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_drop_expr($6, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($7.firstp, _RDB_parse_ecp);
            RDB_raise_type_mismatch("relation type required", _RDB_parse_ecp);
            YYERROR;
        }
        $$ = new_var_def_real($2->var.varname, $4, $6, $7.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_drop_expr($6, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($7.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_REAL TOK_INIT expression key_list {
        $$ = new_var_def_real($2->var.varname, NULL, $5, $6.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($6.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_PRIVATE type ne_key_list {
        if (!RDB_type_is_relation($4)) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_parse_del_keydef_list($5.firstp, _RDB_parse_ecp);
            RDB_raise_type_mismatch("relation type required", _RDB_parse_ecp);
            YYERROR;
        }
        $$ = new_var_def_private($2->var.varname, $4, NULL, $5.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_parse_del_keydef_list($5.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_PRIVATE type TOK_INIT expression key_list {
        if (!RDB_type_is_relation($4)) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_drop_expr($6, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($7.firstp, _RDB_parse_ecp);
            RDB_raise_type_mismatch("relation type required", _RDB_parse_ecp);
            YYERROR;
        }
        $$ = new_var_def_private($2->var.varname, $4, $6, $7.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_type($4, _RDB_parse_ecp, NULL);
            RDB_drop_expr($6, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($7.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_PRIVATE TOK_INIT expression key_list {
        $$ = new_var_def_private($2->var.varname, NULL, $5, $6.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($6.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_VIRTUAL expression {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DEF_VIRTUAL;
        RDB_init_obj(&$$->var.vardef.varname);
    	if (RDB_string_to_obj(&$$->var.vardef.varname,
    	        $2->var.varname, _RDB_parse_ecp) != RDB_OK)
    	    YYERROR;
        $$->var.vardef.exp = $4;
    }
    | TOK_DROP TOK_VAR TOK_ID {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_VAR_DROP;
        RDB_init_obj(&$$->var.vardrop.varname);
    	if (RDB_string_to_obj(&$$->var.vardrop.varname,
    	        $3->var.varname, _RDB_parse_ecp) != RDB_OK)
    	    YYERROR;
        if (RDB_drop_expr($3, _RDB_parse_ecp) != RDB_OK)
            YYERROR;
    }
    | assignment
    | TOK_BEGIN TOK_TX {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_BEGIN_TX;
    }    
    | TOK_COMMIT {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_COMMIT;
    }    
    | TOK_ROLLBACK {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_ROLLBACK;
    }
    | TOK_TYPE TOK_ID possrep_def_list {
        $$ = new_deftype($2->var.varname, $3.firstp, NULL);
        if ($$ == NULL)
             YYERROR;
    }
    | TOK_TYPE TOK_ID possrep_def_list TOK_CONSTRAINT expression {
        $$ = new_deftype($2->var.varname, $3.firstp, $5);
        if ($$ == NULL)
             YYERROR;
    }
    | TOK_DROP TOK_TYPE TOK_ID {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_TYPE_DROP;
        RDB_init_obj(&$$->var.typedrop.typename);
    	if (RDB_string_to_obj(&$$->var.typedrop.typename,
    	        $3->var.varname, _RDB_parse_ecp) != RDB_OK)
    	    YYERROR;
        if (RDB_drop_expr($3, _RDB_parse_ecp) != RDB_OK)
            YYERROR;
    }
    | TOK_DROP TOK_OPERATOR TOK_ID {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_OP_DROP;
        RDB_init_obj(&$$->var.opdrop.opname);
    	if (RDB_string_to_obj(&$$->var.opdrop.opname,
    	        $3->var.varname, _RDB_parse_ecp) != RDB_OK)
    	    YYERROR;
        if (RDB_drop_expr($3, _RDB_parse_ecp) != RDB_OK)
            YYERROR;
    }
    | TOK_RETURN expression {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_RETURN;
        $$->var.retexp = $2;
    }
    | TOK_RETURN {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_RETURN;
        $$->var.retexp = NULL;
    }

ne_key_list: TOK_KEY '{' id_list '}' {
        RDB_parse_keydef *kdp = RDB_alloc(sizeof(RDB_parse_keydef), _RDB_parse_ecp);
        if (kdp == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }
        kdp->attrlist = $3;
        kdp->nextp = NULL;
        $$.firstp = $$.lastp = kdp;
    }
    | ne_key_list TOK_KEY '{' id_list '}' {
        RDB_parse_keydef *kdp = RDB_alloc(sizeof(RDB_parse_keydef), _RDB_parse_ecp);
        if (kdp == NULL) {
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
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
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
        	RDB_drop_expr($1.var.copy.dstp, _RDB_parse_ecp);
        	RDB_drop_expr($1.var.copy.srcp, _RDB_parse_ecp);
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
		$$ = RDB_alloc(sizeof (RDB_parse_attr_assign), _RDB_parse_ecp);
		if ($$ == NULL) {
		    YYERROR;
		}
		$$->dstp = $1.var.copy.dstp;
		$$->srcp = $1.var.copy.srcp;
		$$->nextp = NULL;
    }
	| ne_attr_assign_list ',' simple_assign {
		$$ = RDB_alloc(sizeof (RDB_parse_attr_assign), _RDB_parse_ecp);
		if ($$ == NULL) {
		    YYERROR;
		}

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

expression: expression '{' id_list '}' {
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
    | expression '{' TOK_ALL TOK_BUT id_list '}' {
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
            RDB_free(hap);
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
    | expression TOK_UNWRAP '(' id_list ')' {
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
    | expression TOK_GROUP '{' id_list '}' TOK_AS TOK_ID {
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

id_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_id_list
    ;

ne_id_list: TOK_ID {
        $$.firstp = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$.firstp == NULL) {
            YYERROR;
        }
        $$.lastp = $$.firstp;
        $$.firstp->nextp = NULL;
    }
    | ne_id_list ',' TOK_ID {
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

wrapping: '{' id_list '}' TOK_AS TOK_ID {
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
                _RDB_parse_ecp);
        if (RDB_expr_obj($$.firstp)->typ == NULL)
            YYERROR;

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

literal: TOK_RELATION '{' expression_list '}' {
        RDB_expression *argp;

        $$ = RDB_ro_op("RELATION", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }
        argp = $3.firstp;
        while (argp != NULL)
        {
            RDB_expression *nextp = argp->nextp;
        	RDB_add_arg($$, argp);
            argp = nextp;
        }
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
        RDB_expression *exp, *ex2p, *nextp;

        $$ = RDB_ro_op("TUPLE", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }

        exp = $3.firstp;
        while (exp != NULL) {
            ex2p = exp->nextp;
            nextp = exp->nextp->nextp;

            RDB_add_arg($$, exp);
            RDB_add_arg($$, ex2p);
            exp = nextp;
        }
    } 
    | TOK_LIT_STRING
    | TOK_LIT_INTEGER
    | TOK_LIT_FLOAT
    | TOK_LIT_BOOLEAN
    ;

ne_tuple_item_list: TOK_ID expression {
        $$.firstp = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        RDB_drop_expr($1, _RDB_parse_ecp);
        $$.lastp = $2;
        $$.firstp->nextp = $2;
        $2->nextp = NULL;
    }
    | ne_tuple_item_list ',' TOK_ID expression {
        RDB_expression *attrexp = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
        if (attrexp == NULL)
            YYERROR;
        RDB_drop_expr($3, _RDB_parse_ecp);
        $$.firstp = $1.firstp;
        $$.lastp->nextp = attrexp;
        $$.lastp = $4;
        attrexp->nextp = $4;
        $4->nextp = NULL;
    }
    ;

attribute_list: /* empty */ {
	    $$.firstp = NULL;
	}
	| ne_attribute_list {
	    $$ = $1;
	}

ne_attribute_list: attribute {
        $$.firstp = RDB_alloc(sizeof(parse_attribute), _RDB_parse_ecp);
        if ($$.firstp == NULL) {
            YYERROR;
        }

        $$.firstp->namexp = $1.namexp;
        $$.firstp->typ = $1.typ;
        $$.firstp->nextp = NULL;
        $$.lastp = $$.firstp;
    }
    | ne_attribute_list ',' attribute {
        parse_attribute *attrp = RDB_alloc(sizeof(parse_attribute), _RDB_parse_ecp);
        if (attrp == NULL) {
            YYERROR;
        }

        attrp->namexp = $3.namexp;
        attrp->typ = $3.typ;

        $1.lastp->nextp = attrp;
        attrp->nextp = NULL;
        $$.lastp = attrp;
        $$.firstp = $1.firstp;
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
        RDB_attr attrv[DURO_MAX_LLEN];
        parse_attribute *attrp = $3.firstp;
        int i = 0;

        while (attrp != NULL) {
            attrv[i].name = attrp->namexp->var.varname;
            attrv[i].typ = RDB_dup_nonscalar_type(attrp->typ,
            		_RDB_parse_ecp);
            if (attrv[i].typ == NULL)
                YYERROR;
            attrp = attrp->nextp;
            i++;
        }
        $$ = RDB_create_tuple_type(i, attrv, _RDB_parse_ecp);
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

type: TOK_RELATION '{' attribute_list '}' {
        RDB_attr attrv[DURO_MAX_LLEN];
        int i = 0;
        parse_attribute *attrp = $3.firstp;

        while (attrp != NULL) {
            attrv[i].name = attrp->namexp->var.varname;
            attrv[i].typ = RDB_dup_nonscalar_type(attrp->typ,
            		_RDB_parse_ecp);
            if (attrv[i].typ == NULL)
                YYERROR;
            attrp = attrp->nextp;
            i++;
        }
        $$ = RDB_create_relation_type(i, attrv, _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
    }
/*
    | TOK_SAME_TYPE_AS '(' expression ')' {
		$$ = RDB_expr_type($3, &get_type, _RDB_parse_arg, _RDB_parse_ecp,
		        _RDB_parse_txp);
		if ($$ == NULL)
		    YYERROR;
	}
*/

expression_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_expression_list
    ;

%%

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
