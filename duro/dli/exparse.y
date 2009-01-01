/*
 * $Id$
 *
 * Copyright (C) 2004-2008 René Hartmann.
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

extern RDB_expression *_RDB_parse_resultp;
extern RDB_parse_statement *_RDB_parse_stmtp;
extern RDB_exec_context *_RDB_parse_ecp;
extern int yylineno;

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

static RDB_parse_statement *
new_deftype(const char *name, RDB_parse_possrep *possreplistp,
        RDB_expression *constrp)
{
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL) {
        return NULL;
    }

    stmtp->kind = RDB_STMT_TYPE_DEF;
    stmtp->var.deftype.replistp = possreplistp;
    RDB_init_obj(&stmtp->var.deftype.typename);
  	if (RDB_string_to_obj(&stmtp->var.deftype.typename, name,
  	        _RDB_parse_ecp) != RDB_OK)
  	    return NULL;
  	stmtp->var.deftype.constraintp = constrp;
    return stmtp;
}

static RDB_parse_statement *
new_var_def(const char *name, RDB_expression *typexp, RDB_expression *exp)
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
	stmtp->var.vardef.type.exp = typexp;
	stmtp->var.vardef.type.typ = NULL;
   	stmtp->var.vardef.exp = exp;
   	stmtp->lineno = yylineno;
   	return stmtp;
}

static RDB_parse_statement *
new_var_def_real(const char *name, RDB_expression *rtypexp, RDB_expression *exp,
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
	stmtp->var.vardef.type.exp = rtypexp;
	stmtp->var.vardef.type.typ = NULL;
   	stmtp->var.vardef.exp = exp;
   	stmtp->var.vardef.firstkeyp = firstkeyp;
   	stmtp->lineno = yylineno;
   	return stmtp;
}

static RDB_parse_statement *
new_var_def_private(const char *name, RDB_expression *rtypexp, RDB_expression *exp,
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
	stmtp->var.vardef.type.exp = rtypexp;
	stmtp->var.vardef.type.typ = NULL;
   	stmtp->var.vardef.exp = exp;
   	stmtp->var.vardef.firstkeyp = firstkeyp;
   	stmtp->lineno = yylineno;
   	return stmtp;
}

static RDB_parse_statement *
new_ro_op_def(const char *name, RDB_expr_list *arglistp, RDB_expression *rtypexp,
		RDB_parse_statement *stmtlistp)
{
    int i;
    RDB_expression *argp = arglistp->firstp;
    int argc = RDB_expr_list_length(arglistp) / 2;
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL)
        return NULL;

    stmtp->kind = RDB_STMT_RO_OP_DEF;
    stmtp->var.opdef.rtype.exp = rtypexp;
    stmtp->var.opdef.rtype.typ = NULL;
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
        RDB_init_obj(&stmtp->var.opdef.argv[i].name);
      	stmtp->var.opdef.argv[i].type.exp = NULL;
    }
    for (i = 0; i < argc; i++) {
        if (RDB_string_to_obj(&stmtp->var.opdef.argv[i].name,
                argp->var.varname, _RDB_parse_ecp) != RDB_OK) {
      	    goto error;
      	}
      	stmtp->var.opdef.argv[i].type.exp = argp->nextp;
      	stmtp->var.opdef.argv[i].type.typ = NULL;
      	argp = argp->nextp->nextp;
  	}

   	stmtp->lineno = yylineno;
  	return stmtp;

error:
    RDB_destroy_obj(&stmtp->var.opdef.opname, _RDB_parse_ecp);
    if (stmtp->var.opdef.argv != NULL) {
	    for (i = 0; i < argc; i++) {
	        if (stmtp->var.opdef.argv[i].type.exp != NULL)
	    	    RDB_drop_expr(stmtp->var.opdef.argv[i].type.exp, _RDB_parse_ecp);
    	    RDB_destroy_obj(&stmtp->var.opdef.argv[i].name, _RDB_parse_ecp);
    	}
	    RDB_free(stmtp->var.opdef.argv);
    }
    RDB_parse_del_stmtlist(stmtp->var.opdef.bodyp, _RDB_parse_ecp);
    RDB_free(stmtp);
    return NULL;
}

static RDB_bool
argname_in_list(RDB_expression *argp, RDB_expr_list *listp)
{
    RDB_expression *exp = listp->firstp;
    while(exp != NULL) {
        if (strcmp(argp->var.varname, RDB_obj_string(RDB_expr_obj(exp))) == 0)
            return RDB_TRUE;
        exp = exp->nextp;
    }
    return RDB_FALSE;
}

static RDB_parse_statement *
new_update_op_stmt(const char *name)
{
    RDB_parse_statement *stmtp = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
    if (stmtp == NULL)
        return NULL;

    stmtp->kind = RDB_STMT_UPD_OP_DEF;
    RDB_init_obj(&stmtp->var.opdef.opname);
   	if (RDB_string_to_obj(&stmtp->var.opdef.opname, name, _RDB_parse_ecp)
      	        != RDB_OK) {
        RDB_free(stmtp);
      	return NULL;
  	}
   	stmtp->lineno = yylineno;
  	return stmtp;
}

static RDB_parse_statement *
new_update_op_def(const char *name, RDB_expr_list *arglistp,
        RDB_expr_list *updlistp, RDB_parse_statement *stmtlistp)
{
    int i;
    RDB_expression *argp;
    RDB_expression *updexp;
    RDB_parse_statement *stmtp = new_update_op_stmt(name);
    if (stmtp == NULL)
        return NULL;

    stmtp->var.opdef.bodyp = stmtlistp;
  	stmtp->var.opdef.argc = RDB_expr_list_length(arglistp) / 2;
  	stmtp->var.opdef.argv = RDB_alloc(
  			stmtp->var.opdef.argc * sizeof(RDB_parse_arg), _RDB_parse_ecp);
  	if (stmtp->var.opdef.argv == NULL)
  	    goto error;
  	for (i = 0; i < stmtp->var.opdef.argc; i++) {
        RDB_init_obj(&stmtp->var.opdef.argv[i].name);
      	stmtp->var.opdef.argv[i].type.exp = NULL;
  	    stmtp->var.opdef.argv[i].upd = RDB_FALSE;
  	}

  	argp = arglistp->firstp;
    for (i = 0; i < stmtp->var.opdef.argc; i++) {
        if (RDB_string_to_obj(&stmtp->var.opdef.argv[i].name,
                argp->var.varname, _RDB_parse_ecp) != RDB_OK) {
      	    goto error;
      	}
      	stmtp->var.opdef.argv[i].type.exp = argp->nextp;
      	stmtp->var.opdef.argv[i].type.typ = NULL;
      	if (argname_in_list(argp, updlistp))
      	    stmtp->var.opdef.argv[i].upd = RDB_TRUE;
      	argp = argp->nextp->nextp;
  	}

  	/* Check if all UPDATE arguments are valid */
  	updexp = updlistp->firstp;
  	while (updexp != NULL) {
  	    for (i = 0;
  	            i < stmtp->var.opdef.argc
                && strcmp(RDB_obj_string(RDB_expr_obj(updexp)),
                        RDB_obj_string(&stmtp->var.opdef.argv[i].name)) != 0;
  	            i++);
  	    if (i >= stmtp->var.opdef.argc) {
  	        /* Not found */
  	        RDB_raise_invalid_argument("Invalid UPDATES clause",
  	        		_RDB_parse_ecp);
  	        goto error;
  	    }
  	    updexp = updexp->nextp;
  	}
  	
  	return stmtp;

error:
    RDB_destroy_obj(&stmtp->var.opdef.opname, _RDB_parse_ecp);
    if (stmtp->var.opdef.argv != NULL) {
	    for (i = 0; i < stmtp->var.opdef.argc; i++) {
    	    if (stmtp->var.opdef.argv[i].type.exp != NULL)
	    	    RDB_drop_expr(stmtp->var.opdef.argv[i].type.exp, _RDB_parse_ecp);
    	    RDB_destroy_obj(&stmtp->var.opdef.argv[i].name, _RDB_parse_ecp);
    	}
	    RDB_free(stmtp->var.opdef.argv);
    }
    RDB_parse_del_stmtlist(stmtp->var.opdef.bodyp, _RDB_parse_ecp);
    RDB_free(stmtp);
    return NULL;
}

static int
resolve_with(RDB_expression **expp, RDB_expression *texp)
{
    if (texp->nextp->nextp != NULL) {
        if (resolve_with(expp, texp->nextp->nextp) != RDB_OK)
            return RDB_ERROR;
    }
    return _RDB_resolve_exprnames(expp, texp, _RDB_parse_ecp);
}

%}

%error-verbose
%locations

%union {
    RDB_expression *exp;
    RDB_expr_list explist;
    RDB_parse_statement *stmt;
    struct {
        RDB_parse_statement *firstp;
        RDB_parse_statement *lastp;
    } stmtlist;
    RDB_parse_assign *assignlist;
    struct {
        RDB_parse_keydef *firstp;
        RDB_parse_keydef *lastp;
    } keylist;
    RDB_parse_possrep possrep;
    struct {
        RDB_parse_possrep *firstp;
        RDB_parse_possrep *lastp;
    } possreplist;
}

%token TOK_START_EXP TOK_START_STMT
%token <exp> TOK_ID "identifier"
%token <exp> TOK_LIT_INTEGER "integer literal"
%token <exp> TOK_LIT_STRING "character literal"
%token <exp> TOK_LIT_FLOAT "floating point literal"
%token <exp> TOK_LIT_BOOLEAN "boolean literal"
%token TOK_WHERE "WHERE"
%token TOK_UNION "UNION"
%token TOK_INTERSECT "INTERSECT"
%token TOK_MINUS "MINUS"
%token TOK_SEMIMINUS "SEMIMINUS"
%token TOK_SEMIJOIN "SEMIJOIN"
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
%token TOK_INIT "INIT"
%token TOK_ADD "ADD"
%token TOK_BEGIN "BEGIN"
%token TOK_TX "TRANSACTION"
%token TOK_REAL "REAL"
%token TOK_VIRTUAL "VIRTUAL"
%token TOK_PRIVATE "PRIVATE"
%token TOK_KEY "KEY"
%token TOK_COMMIT "COMMIT"
%token TOK_ROLLBACK "ROLLBACK"
%token TOK_MATCHES "MATCHES"
%token TOK_IN "IN"
%token TOK_SUBSET_OF "SUBSET_OF"
%token TOK_OR "OR"
%token TOK_AND "AND"
%token TOK_NOT "NOT"
%token TOK_CONCAT "CONCAT"
%token TOK_NE "<>"
%token TOK_LE "<="
%token TOK_GE ">="
%token TOK_COUNT "COUNT"
%token TOK_SUM "SUM"
%token TOK_AVG AVG
%token TOK_MAX MAX
%token TOK_MIN MIN
%token TOK_ALL ALL
%token TOK_ANY ANY
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
%token TOK_TABLE_DEE "TABLE_DEE"
%token TOK_TABLE_DUM "TABLE_DUM"
%token TOK_ASSIGN ":="
%token TOK_INSERT "INSERT"
%token TOK_DELETE "DELETE"
%token TOK_UPDATE "UPDATE"
%token TOK_TYPE "TYPE"
%token TOK_POSSREP "POSSREP"
%token TOK_CONSTRAINT "CONSTRAINT"
%token TOK_OPERATOR "OPERATOR"
%token TOK_RETURNS "RETURNS"
%token TOK_UPDATES "UPDATES"
%token TOK_RETURN "RETURN"
%token TOK_LOAD "LOAD"
%token TOK_ORDER "ORDER"
%token TOK_ASC "ASC"
%token TOK_DESC "DESC"
%token TOK_WITH "WITH"
%token TOK_INVALID "invalid"

%type <exp> expression literal ro_op_invocation count_invocation
        sum_invocation avg_invocation min_invocation max_invocation
        all_invocation any_invocation vexpr type

%type <explist> expression_list ne_expression_list
        ne_id_list id_list ne_tuple_item_list
        name_intro_list ne_name_intro_list name_intro summarize_add
        renaming ne_renaming_list renaming_list
        summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list
        attribute_list ne_attribute_list
        order_item order_item_list ne_order_item_list

%type <stmt> statement statement_body when_def when_def_list

%type <stmtlist> ne_statement_list

%type <assignlist> simple_assign assign ne_attr_assign_list assignment;

%type <keylist> key_list ne_key_list

%type <possrep> possrep_def

%type <possreplist> possrep_def_list;

%destructor {
    RDB_destroy_expr_list(&$$, _RDB_parse_ecp);
} expression_list ne_expression_list
        ne_id_list id_list
        name_intro_list ne_name_intro_list name_intro
        ne_renaming_list renaming_list renaming
        summarize_add summarize_add_list ne_summarize_add_list
        wrapping wrapping_list ne_wrapping_list

%destructor {
    RDB_drop_expr($$, _RDB_parse_ecp);
} expression

%destructor {
    if ($$ != NULL)
        RDB_parse_del_stmt($$, _RDB_parse_ecp);
} statement statement_body when_def when_def_list

%destructor {
    RDB_parse_del_keydef_list($$.firstp, _RDB_parse_ecp);
} key_list ne_key_list

%destructor {
    RDB_parse_del_stmtlist($$.firstp, _RDB_parse_ecp);
} ne_statement_list

%destructor {
    RDB_parse_del_assignlist($$, _RDB_parse_ecp);
} simple_assign assign ne_attr_assign_list assignment

%left ':'
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
%left '.' UPLUS UMINUS '['

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
    | TOK_BEGIN ne_statement_list TOK_END {
        /*
         * Convert BEGIN ... END into IF TRUE THEN ... END IF
         * BEGIN ... END is not really needed, and only supported
         * for compatibility with TTM
         */
        RDB_expression *condp = RDB_bool_to_expr (RDB_TRUE, _RDB_parse_ecp);
        if (condp == NULL)
            YYERROR;
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr(condp, _RDB_parse_ecp);
            RDB_parse_del_stmtlist($2.firstp, _RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_IF;
        $$->var.ifthen.condp = condp;
        $$->var.ifthen.ifp = $2.firstp;
    	$$->var.ifthen.elsep = NULL;
        $$->lineno = yylineno;
    }
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
        $$->lineno = yylineno;
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
        $$->lineno = yylineno;
    }
    | case_opt_semi when_def_list TOK_END TOK_CASE {
        if ($2 == NULL) {
	        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
	        if ($$ == NULL) {
	            YYERROR;
	        }
	    	$$->kind = RDB_STMT_NOOP;
	        $$->lineno = yylineno;
	    } else {
	        $$ = $2;
	    }
    }
    | case_opt_semi when_def_list TOK_ELSE
            ne_statement_list TOK_END TOK_CASE {
        if ($2 == NULL) {
	        $$ = $4.firstp;
	    } else {
	        /*
	         * Attach ELSE branch to last of WHENs
	         */
            RDB_parse_statement *lastp = $2;

            while (lastp->var.ifthen.elsep != NULL)
                lastp = lastp->var.ifthen.elsep;
	        lastp->var.ifthen.elsep = $4.firstp;

	        $$ = $2;
	    }
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
        $$->lineno = yylineno;
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
        $$->lineno = yylineno;
    }
    | TOK_OPERATOR TOK_ID '(' attribute_list ')' TOK_RETURNS type
            ne_statement_list TOK_END TOK_OPERATOR {
        $$ = new_ro_op_def($2->var.varname, &$4, $7, $8.firstp);
        if ($$ == NULL)
            YYERROR;
    }
    | TOK_OPERATOR TOK_ID '(' attribute_list ')' TOK_UPDATES '{' id_list '}'
            ne_statement_list TOK_END TOK_OPERATOR {
        $$ = new_update_op_def($2->var.varname, &$4, &$8, $10.firstp);
        if ($$ == NULL)
            YYERROR;
    }

case_opt_semi: TOK_CASE ';'
    | TOK_CASE

when_def: TOK_WHEN expression TOK_THEN ne_statement_list {
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
        $$->lineno = yylineno;
    }

when_def_list: /* empty */ {
        $$ = NULL;
    }
    | when_def when_def_list {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_parse_del_stmt($1, _RDB_parse_ecp);
            if ($2 != NULL)
	            RDB_parse_del_stmt($2, _RDB_parse_ecp);
            YYERROR;
        }
        $$ = $1;
    	$$->var.ifthen.elsep = $2;
        $$->lineno = yylineno;
    }

possrep_def: TOK_POSSREP '{' attribute_list '}' {
	    $$.namexp = NULL;
        $$.attrlist = $3;
	}
	| TOK_POSSREP TOK_ID '{' attribute_list '}' {
	    $$.namexp = $2;
        $$.attrlist = $4;
	}

possrep_def_list: possrep_def {
        $$.firstp = RDB_alloc(sizeof(RDB_parse_possrep), _RDB_parse_ecp);
	    if ($$.firstp == NULL)
	        YYERROR;
        $$.firstp->attrlist = $1.attrlist;
        $$.firstp->namexp = $1.namexp;
        $$.firstp->nextp = NULL;
        $$.lastp = $$.firstp;
    }
	| possrep_def_list possrep_def {
	    $1.lastp->nextp = RDB_alloc(sizeof(RDB_parse_possrep), _RDB_parse_ecp);
	    if ($1.lastp->nextp == NULL)
	        YYERROR;
	    $1.lastp->nextp->attrlist = $2.attrlist;
	    $1.lastp->nextp->namexp = $2.namexp;
	    $$.firstp = $1.firstp;
	    $$.lastp = $1.lastp->nextp;
	}

order_item: TOK_ID TOK_ASC {
        $$.firstp = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        $$.lastp = RDB_string_to_expr("ASC", _RDB_parse_ecp);
        if ($$.lastp == NULL)
            YYERROR;
        RDB_drop_expr($1, _RDB_parse_ecp);
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }
    | TOK_ID TOK_DESC {
        $$.firstp = RDB_string_to_expr($1->var.varname, _RDB_parse_ecp);
        if ($$.firstp == NULL)
            YYERROR;
        $$.lastp = RDB_string_to_expr("DESC", _RDB_parse_ecp);
        if ($$.lastp == NULL)
            YYERROR;
        RDB_drop_expr($1, _RDB_parse_ecp);
        $$.firstp->nextp = $$.lastp;
        $$.lastp->nextp = NULL;
    }

ne_order_item_list: order_item 
    | ne_order_item_list ',' order_item {
        $$ = $1;
        RDB_join_expr_lists(&$$, &$3);
    }

order_item_list: ne_order_item_list {
    }
    | /* Empty */ {
        RDB_init_expr_list(&$$);
    }

statement_body: /* empty */ {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
    	$$->kind = RDB_STMT_NOOP;
        $$->lineno = yylineno;
    }
    | TOK_CALL TOK_ID '(' expression_list ')' {
        $$ = RDB_parse_new_call($2->var.varname, &$4);
        RDB_drop_expr($2, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$4, _RDB_parse_ecp);
            YYERROR;
        }
        $$->lineno = yylineno;
    }
    | TOK_ID '(' expression_list ')' {
        $$ = RDB_parse_new_call($1->var.varname, &$3);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_destroy_expr_list(&$3, _RDB_parse_ecp);
            YYERROR;
        }
        $$->lineno = yylineno;
    }
    | TOK_VAR TOK_ID type {
        $$ = new_var_def($2->var.varname, $3, NULL);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
    }
    | TOK_VAR TOK_ID type TOK_INIT expression {
        $$ = new_var_def($2->var.varname, $3, $5);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
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
        $$ = new_var_def_real($2->var.varname, $4, NULL, $5.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($5.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_REAL type TOK_INIT expression key_list {
        $$ = new_var_def_real($2->var.varname, $4, $6, $7.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
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
        $$ = new_var_def_private($2->var.varname, $4, NULL, $5.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_parse_del_keydef_list($5.firstp, _RDB_parse_ecp);
           	YYERROR;
        }
    }
    | TOK_VAR TOK_ID TOK_PRIVATE type TOK_INIT expression key_list {
        $$ = new_var_def_private($2->var.varname, $4, $6, $7.firstp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
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
        $$->lineno = yylineno;
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
        $$->lineno = yylineno;
    }
    | assignment {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_ASSIGN;
        $$->var.assignment.assignp = $1;
        $$->lineno = yylineno;
    }
    | TOK_BEGIN TOK_TX {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_BEGIN_TX;
        $$->lineno = yylineno;
    }    
    | TOK_COMMIT {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_COMMIT;
        $$->lineno = yylineno;
    }    
    | TOK_ROLLBACK {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_ROLLBACK;
        $$->lineno = yylineno;
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
        $$->lineno = yylineno;
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
        $$->lineno = yylineno;
    }
    | TOK_RETURN expression {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            YYERROR;
        }
        $$->kind = RDB_STMT_RETURN;
        $$->var.retexp = $2;
        $$->lineno = yylineno;
    }
    | TOK_RETURN {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            YYERROR;
        }
        $$->kind = RDB_STMT_RETURN;
        $$->var.retexp = NULL;
        $$->lineno = yylineno;
    }
    | TOK_LOAD TOK_ID TOK_FROM expression TOK_ORDER '(' order_item_list ')' {
         RDB_expr_list explist;

         RDB_init_expr_list(&explist);
         RDB_expr_list_append(&explist, $2);
         RDB_expr_list_append(&explist, $4);

         RDB_join_expr_lists(&explist, &$7);

         $$ = RDB_parse_new_call("LOAD", &explist);
    }
    | TOK_CONSTRAINT TOK_ID expression {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_CONSTRAINT_DEF;
        RDB_init_obj(&$$->var.constrdef.constrname);
    	if (RDB_string_to_obj(&$$->var.constrdef.constrname,
    	        $2->var.varname, _RDB_parse_ecp) != RDB_OK)
    	    YYERROR;
        if (RDB_drop_expr($2, _RDB_parse_ecp) != RDB_OK)
            YYERROR;
        $$->var.constrdef.constraintp = $3;
        $$->lineno = yylineno;
    }
    | TOK_DROP TOK_CONSTRAINT TOK_ID {
        $$ = RDB_alloc(sizeof(RDB_parse_statement), _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($3, _RDB_parse_ecp);
           	YYERROR;
        }
        $$->kind = RDB_STMT_CONSTRAINT_DROP;
        RDB_init_obj(&$$->var.constrdrop.constrname);
    	if (RDB_string_to_obj(&$$->var.constrdrop.constrname,
    	        $3->var.varname, _RDB_parse_ecp) != RDB_OK)
    	    YYERROR;
        if (RDB_drop_expr($3, _RDB_parse_ecp) != RDB_OK)
            YYERROR;
        $$->lineno = yylineno;
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

assignment: assign
    | assignment ',' assign {
        $3->nextp = $1;
        $$ = $3;
    }

assign: vexpr TOK_ASSIGN expression {
		$$ = RDB_alloc(sizeof (RDB_parse_assign), _RDB_parse_ecp);
		if ($$ == NULL) {
		    YYERROR;
		}
        $$->nextp = NULL;

        $$->kind = RDB_STMT_COPY;
        $$->var.copy.dstp = $1;
        $$->var.copy.srcp = $3;
    }
    | TOK_INSERT TOK_ID expression {
        $$ = RDB_alloc(sizeof(RDB_parse_assign), _RDB_parse_ecp);
        if ($$ == NULL)
            YYERROR;
        $$->nextp = NULL;

        $$->kind = RDB_STMT_INSERT;
        $$->var.ins.dstp = $2;
        $$->var.ins.srcp = $3;
    }
    | TOK_DELETE TOK_ID {
        $$ = RDB_alloc(sizeof(RDB_parse_assign), _RDB_parse_ecp);
        if ($$ == NULL)
            YYERROR;
        $$->nextp = NULL;

        $$->kind = RDB_STMT_DELETE;
        $$->var.del.dstp = $2;
        $$->var.del.condp = NULL;
    }
    | TOK_DELETE TOK_ID TOK_WHERE expression {
        $$ = RDB_alloc(sizeof(RDB_parse_assign), _RDB_parse_ecp);
        if ($$ == NULL)
            YYERROR;
        $$->nextp = NULL;

        $$->kind = RDB_STMT_DELETE;
        $$->var.del.dstp = $2;
        $$->var.del.condp = $4;
    }
    | TOK_UPDATE TOK_ID '{' ne_attr_assign_list '}' {
        $$ = RDB_alloc(sizeof(RDB_parse_assign), _RDB_parse_ecp);
        if ($$ == NULL)
            YYERROR;
        $$->nextp = NULL;

        $$->kind = RDB_STMT_UPDATE;
        $$->var.upd.dstp = $2;
        $$->var.upd.assignlp = $4;
        $$->var.upd.condp = NULL;
    }
    | TOK_UPDATE TOK_ID TOK_WHERE expression '{' ne_attr_assign_list '}' {
        $$ = RDB_alloc(sizeof(RDB_parse_assign), _RDB_parse_ecp);
        if ($$ == NULL)
            YYERROR;
        $$->nextp = NULL;

        $$->kind = RDB_STMT_UPDATE;
        $$->var.upd.dstp = $2;
        $$->var.upd.assignlp = $6;
        $$->var.upd.condp = $4;
    }

ne_attr_assign_list: simple_assign {
        $$ = $1;
        $$->nextp = NULL;
    }
	| ne_attr_assign_list ',' simple_assign {
	    $$ = $3;
	    $3->nextp = $1;
	}

simple_assign: TOK_ID TOK_ASSIGN expression {
		$$ = RDB_alloc(sizeof (RDB_parse_assign), _RDB_parse_ecp);
		if ($$ == NULL) {
		    YYERROR;
		}

        $$->kind = RDB_STMT_COPY;
        $$->var.copy.dstp = $1;
        $$->var.copy.srcp = $3;
    }

vexpr: TOK_ID
    | ro_op_invocation
    | vexpr '[' expression ']' {
        $$ = RDB_ro_op("[]", _RDB_parse_ecp);
        if ($$ == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }
        RDB_add_arg($$, $1);
        RDB_add_arg($$, $3);
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
        RDB_expression *texp = $1;
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
        RDB_expression *texp = $1;
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

        texp = $1;
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
        RDB_expression *exp = $1;
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

        tex1p = $1;
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $3;
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

        tex1p = $1;
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $3;
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

        tex1p = $1;
        if (tex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $3;
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

        tex1p = $1;
        if (tex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $3;
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

        tex1p = $1;
        if (tex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $3;
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

        tex1p = $1;
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $3;
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
    | TOK_EXTEND expression TOK_ADD '(' name_intro_list ')' {
        RDB_expression *texp = $2;
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
        RDB_parse_assign *ap, *hap;
        RDB_expression *texp = $2;
        if (texp == NULL) {
            texp = $2;
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
            if (ap->var.copy.dstp->kind != RDB_EX_VAR) {
                RDB_raise_invalid_argument("invalid UPDATE argument",
                        _RDB_parse_ecp);
                YYERROR;
            }
            RDB_add_arg($$, RDB_string_to_expr(ap->var.copy.dstp->var.varname,
                    _RDB_parse_ecp));
            RDB_add_arg($$, ap->var.copy.srcp);
            RDB_drop_expr(ap->var.copy.dstp, _RDB_parse_ecp);
            hap = ap;
            ap = ap->nextp;
            RDB_free(hap);
        } while (ap != NULL);
    }
    | TOK_SUMMARIZE expression TOK_PER expression
           TOK_ADD '(' summarize_add_list ')' {
        RDB_expression *tex1p, *tex2p;

        tex1p = $2;
        if (tex1p == NULL)
        {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_destroy_expr_list(&$7, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $4;
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

        tex1p = $1;
        if (tex1p == NULL)
        {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }

        tex2p = $3;
        if (tex2p == NULL)
        {
            RDB_drop_expr(tex1p, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
            RDB_drop_expr($5, _RDB_parse_ecp);
            YYERROR;
        }

        tex3p = $5;
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
        RDB_expression *texp = ($1);
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
        RDB_expression *texp = ($1);
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
        RDB_expression *texp = ($1);
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
        RDB_expression *texp = ($1);
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
        RDB_expression *exp = ($1);
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

        ex1p = ($1);
        if (ex1p == NULL) {
            RDB_drop_expr($1, _RDB_parse_ecp);
            RDB_drop_expr($3, _RDB_parse_ecp);
           YYERROR;
        }

        ex2p = ($3);
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
    | vexpr
    | literal
    | count_invocation
    | sum_invocation
    | avg_invocation
    | max_invocation
    | min_invocation
    | all_invocation
    | any_invocation
    | '(' expression ')' {
        $$ = $2;
    }
    | expression '.' TOK_ID {
        $$ = RDB_tuple_attr($1, $3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if ($$ == NULL)
            RDB_drop_expr($1, _RDB_parse_ecp);
    }
    | TOK_ID TOK_FROM expression {
        $$ = RDB_tuple_attr($3, $1->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($1, _RDB_parse_ecp);
        if ($$ == NULL)
            RDB_drop_expr($3, _RDB_parse_ecp);
    }
    | TOK_TUPLE TOK_FROM expression {
        RDB_expression *texp;

        texp = ($3);
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

        ex1p = ($4);
        if (ex1p == NULL) {
            RDB_drop_expr($2, _RDB_parse_ecp);
            RDB_drop_expr($4, _RDB_parse_ecp);
            RDB_drop_expr($6, _RDB_parse_ecp);
            YYERROR;
        }
        ex2p = ($6);
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
    | TOK_WITH name_intro_list ':' expression {
        if ($2.firstp != NULL) {
            if (resolve_with(&$4, $2.firstp) != RDB_OK) {
                RDB_destroy_expr_list(&$2, _RDB_parse_ecp);
                RDB_drop_expr($4, _RDB_parse_ecp);
                YYERROR;
            }
            RDB_destroy_expr_list(&$2, _RDB_parse_ecp);
        }
        $$ = $4;
    }
    ;

id_list: /* empty */ {
        RDB_init_expr_list(&$$);
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
        RDB_expression *exp = RDB_string_to_expr($3->var.varname, _RDB_parse_ecp);
        RDB_drop_expr($3, _RDB_parse_ecp);
        if (exp == NULL) {
            RDB_destroy_expr_list(&$1, _RDB_parse_ecp);
            YYERROR;
        }

        RDB_expr_list_append(&$$, exp);
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

name_intro_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_name_intro_list
    ;

ne_name_intro_list: name_intro
    | ne_name_intro_list ',' name_intro {
        $$.firstp = $1.firstp;
        $1.lastp->nextp = $3.firstp;
        $$.lastp = $3.lastp;
    }
    ;

name_intro: expression TOK_AS TOK_ID {
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
        RDB_expression *exp = ($3);
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
            RDB_expression *texp = ($3.firstp);
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
            RDB_expression *texp = ($3.firstp);
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
            RDB_expression *texp = ($3.firstp);
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
            RDB_expression *texp = ($3.firstp);
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
            RDB_expression *texp = ($3.firstp);
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
            RDB_expression *texp = ($3.firstp);
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
                RDB_expression *exp = (argp);
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
    } */
    | TOK_TABLE_DEE {
        int ret;

        RDB_object tpl;
        RDB_expression *exp = table_dum_expr();
        if (exp == NULL) {
            YYERROR;
        }

        RDB_init_obj(&tpl);
        ret = RDB_insert(&exp->var.obj, &tpl, _RDB_parse_ecp, NULL);
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

ne_attribute_list: TOK_ID type {
        $$.firstp = $1;
        $$.lastp = $2;
        $$.firstp->nextp = $2;
        $2->nextp = NULL;
    }
    | ne_attribute_list ',' TOK_ID type {
        $$.firstp = $1.firstp;
        $$.lastp->nextp = $3;
        $$.lastp = $4;
        $3->nextp = $4;
        $4->nextp = NULL;
    }
    ;

type: TOK_ID
    | TOK_TUPLE '{' '}' {
        $$ = RDB_ro_op("TUPLE", _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
    }
    | TOK_TUPLE '{' ne_attribute_list '}' {
        $$ = RDB_ro_op("TUPLE", _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;

        $$->var.op.args = $3;
    }
    | TOK_RELATION '{' attribute_list '}' {
        $$ = RDB_ro_op("RELATION", _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;

        $$->var.op.args = $3;
    }
    | TOK_ARRAY type {
        $$ = RDB_ro_op("ARRAY", _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;

        RDB_add_arg($$, $2);
    }
    | TOK_SAME_TYPE_AS '(' expression ')' {
        $$ = RDB_ro_op("SAME_TYPE_AS", _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
        RDB_add_arg($$, $3);
	}
    | TOK_TUPLE TOK_SAME_HEADING_AS '(' expression ')' {
        $$ = RDB_ro_op("TUPLE_SAME_HEADING_AS", _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
        RDB_add_arg($$, $4);
	}
    | TOK_RELATION TOK_SAME_HEADING_AS '(' expression ')' {
        $$ = RDB_ro_op("RELATION_SAME_HEADING_AS", _RDB_parse_ecp);
		if ($$ == NULL)
		    YYERROR;
        RDB_add_arg($$, $4);
	}

expression_list: /* empty */ {
        $$.firstp = $$.lastp = NULL;
    }
    | ne_expression_list
    ;

%%
