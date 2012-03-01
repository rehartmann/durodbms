/*
 * $Id$
 *
 * Copyright (C) 2003-2011 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

%option nounput
%option noyywrap
%option yylineno
%option nounistd

%{
#define YYSTYPE RDB_parse_node *

#include <rel/rdb.h>
#include "parsenode.h"
#include "exparse.h"
#include <string.h>
#include <math.h>
#include <ctype.h>

#define COMMENT_BUF_SIZE 10

extern RDB_exec_context *_RDB_parse_ecp;

int _RDB_parse_interactive = 0;
int _RDB_parse_case_insensitive = 1;

YY_BUFFER_STATE _RDB_parse_buffer;

int _RDB_parse_start_token;

char *_RDB_parse_prompt;

RDB_object *whitecommp = NULL;

/*
 * Process backslash escapes on string.
 * The string must not end with a backslash.
 */
static void
process_escapes(char *txt)
{
    int i = 1, j = 1;

    for(;;) {
        if (yytext[i] == '\\') {
            switch (yytext[i + 1]) {
                case 'n':
                    yytext[j] = '\n';
                    i += 2;
                    j++;
                    continue;
                case 'r':
                    yytext[j] = '\r';
                    i += 2;
                    j++;
                    continue;
                case 't':
                    yytext[j] = '\t';
                    i += 2;
                    j++;
                    continue;
                default:
                    yytext[j] = yytext[i + 1];
                    i += 2;
                    j++;
                    continue;
            }
        }
        yytext[j] = yytext[i];
        if (yytext[i] == '\0')
            break;
        i++;
        j++;
    }
}

/*
 * Used in interactive mode to start with an empty buffer
 */
void
_RDB_parse_init_buf(FILE *f)
{
    if (_RDB_parse_interactive)
	    _RDB_parse_buffer = yy_scan_string("");
	else {
		_RDB_parse_buffer = yy_create_buffer(f, YY_BUF_SIZE);
		yy_switch_to_buffer(_RDB_parse_buffer);
	}
}

void
_RDB_parse_start_exp(void)
{
    _RDB_parse_start_token = TOK_START_EXP;
}

void
_RDB_parse_start_stmt(void)
{
    _RDB_parse_start_token = TOK_START_STMT;
}

%}

%%

%{

if (_RDB_parse_start_token != 0)
{
    int t = _RDB_parse_start_token;
    _RDB_parse_start_token = 0;
    return t;
}

yylloc.first_line = yylloc.last_line = yylineno;

%}

WHERE {
    yylval = RDB_new_parse_token(TOK_WHERE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WHERE;
}

UNION {
    yylval = RDB_new_parse_token(TOK_UNION, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UNION;
}

INTERSECT {
    yylval = RDB_new_parse_token(TOK_INTERSECT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_INTERSECT;
}

MINUS {
    yylval = RDB_new_parse_token(TOK_MINUS, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MINUS;
}

SEMIMINUS {
    yylval = RDB_new_parse_token(TOK_SEMIMINUS, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SEMIMINUS;
}

SEMIJOIN {
    yylval = RDB_new_parse_token(TOK_SEMIJOIN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SEMIJOIN;
}

JOIN {
    yylval = RDB_new_parse_token(TOK_JOIN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_JOIN;
}

WRAP {
    yylval = RDB_new_parse_token(TOK_WRAP, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WRAP;
}

UNWRAP {
    yylval = RDB_new_parse_token(TOK_UNWRAP, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UNWRAP;
}

GROUP {
    yylval = RDB_new_parse_token(TOK_GROUP, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_GROUP;
}

UNGROUP {
    yylval = RDB_new_parse_token(TOK_UNGROUP, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UNGROUP;
}

FROM {
    yylval = RDB_new_parse_token(TOK_FROM, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_FROM;
}

TUPLE {
    yylval = RDB_new_parse_token(TOK_TUPLE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TUPLE;
}

TUP {
    yylval = RDB_new_parse_token(TOK_TUPLE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TUPLE;
}

RELATION {
    yylval = RDB_new_parse_token(TOK_RELATION, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RELATION;
}

REL {
    yylval = RDB_new_parse_token(TOK_RELATION, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RELATION;
}

ARRAY {
    yylval = RDB_new_parse_token(TOK_ARRAY, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ARRAY;
}

BUT {
    yylval = RDB_new_parse_token(TOK_BUT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_BUT;
}

AS {
    yylval = RDB_new_parse_token(TOK_AS, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_AS;
}

EXTEND {
    yylval = RDB_new_parse_token(TOK_EXTEND, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_EXTEND;
}

RENAME {
    yylval = RDB_new_parse_token(TOK_RENAME, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RENAME;
}

SUMMARIZE {
    yylval = RDB_new_parse_token(TOK_SUMMARIZE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SUMMARIZE;
}

DIVIDEBY {
    yylval = RDB_new_parse_token(TOK_DIVIDEBY, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DIVIDEBY;
}

PER {
    yylval = RDB_new_parse_token(TOK_PER, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_PER;
}

ADD {
    yylval = RDB_new_parse_token(TOK_ADD, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ADD;
}

MATCHES {
    yylval = RDB_new_parse_token(TOK_MATCHES, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MATCHES;
}

IN {
    yylval = RDB_new_parse_token(TOK_IN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_IN;
}

SUBSET_OF {
    yylval = RDB_new_parse_token(TOK_SUBSET_OF, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SUBSET_OF;
}

OR {
    yylval = RDB_new_parse_token(TOK_OR, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_OR;
}

AND {
    yylval = RDB_new_parse_token(TOK_AND, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_AND;
}

XOR {
    yylval = RDB_new_parse_token(TOK_XOR, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_XOR;
}

NOT {
    yylval = RDB_new_parse_token(TOK_NOT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_NOT;
}

IF {
    yylval = RDB_new_parse_token(TOK_IF, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_IF;
}

THEN {
    yylval = RDB_new_parse_token(TOK_THEN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_THEN;
}

ELSE {
    yylval = RDB_new_parse_token(TOK_ELSE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ELSE;
}

END {
    yylval = RDB_new_parse_token(TOK_END, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_END;
}

CASE {
    yylval = RDB_new_parse_token(TOK_CASE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CASE;
}

WHEN {
    yylval = RDB_new_parse_token(TOK_WHEN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WHEN;
}

FOR {
    yylval = RDB_new_parse_token(TOK_FOR, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_FOR;
}

DO {
    yylval = RDB_new_parse_token(TOK_FOR, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_FOR;
}

TO {
    yylval = RDB_new_parse_token(TOK_TO, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TO;
}

WHILE {
    yylval = RDB_new_parse_token(TOK_WHILE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WHILE;
}

LEAVE {
    yylval = RDB_new_parse_token(TOK_LEAVE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LEAVE;
}

TRUE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_TRUE, _RDB_parse_ecp),
    		whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_BOOLEAN;
}

FALSE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_FALSE, _RDB_parse_ecp),
    		whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_BOOLEAN;
}

COUNT {
    yylval = RDB_new_parse_token(TOK_COUNT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_COUNT;
}

SUM {
    yylval = RDB_new_parse_token(TOK_SUM, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SUM;
}

AVG {
    yylval = RDB_new_parse_token(TOK_AVG, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_AVG;
}

MAX {
    yylval = RDB_new_parse_token(TOK_MAX, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MAX;
}

MIN {
    yylval = RDB_new_parse_token(TOK_MIN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MIN;
}

ALL {
    yylval = RDB_new_parse_token(TOK_ALL, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ALL;
}

ANY {
    yylval = RDB_new_parse_token(TOK_ANY, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ANY;
}

SAME_TYPE_AS {
    yylval = RDB_new_parse_token(TOK_SAME_TYPE_AS, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
	return TOK_SAME_TYPE_AS;
}

SAME_HEADING_AS {
    yylval = RDB_new_parse_token(TOK_SAME_HEADING_AS, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
	return TOK_SAME_HEADING_AS;
}

TABLE_DEE {
    yylval = RDB_new_parse_token(TOK_TABLE_DEE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DEE;
}

DEE {
    yylval = RDB_new_parse_token(TOK_TABLE_DEE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DEE;
}

TABLE_DUM {
    yylval = RDB_new_parse_token(TOK_TABLE_DUM, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DUM;
}

DUM {
    yylval = RDB_new_parse_token(TOK_TABLE_DUM, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DUM;
}

VAR {
    yylval = RDB_new_parse_token(TOK_VAR, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_VAR;
}

DROP {
    yylval = RDB_new_parse_token(TOK_DROP, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DROP;
}

REAL {
    yylval = RDB_new_parse_token(TOK_REAL, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_REAL;
}

PRIVATE {
    yylval = RDB_new_parse_token(TOK_PRIVATE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_PRIVATE;
}

BASE {
    yylval = RDB_new_parse_token(TOK_REAL, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_REAL;
}

VIRTUAL {
    yylval = RDB_new_parse_token(TOK_VIRTUAL, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_VIRTUAL;
}

KEY {
    yylval = RDB_new_parse_token(TOK_KEY, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_KEY;
}

INIT {
    yylval = RDB_new_parse_token(TOK_INIT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_INIT;
}

BEGIN {
    yylval = RDB_new_parse_token(TOK_BEGIN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_BEGIN;
}

TRANSACTION {
    yylval = RDB_new_parse_token(TOK_TX, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TX;
}

TX {
    yylval = RDB_new_parse_token(TOK_TX, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TX;
}

COMMIT {
    yylval = RDB_new_parse_token(TOK_COMMIT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_COMMIT;
}

ROLLBACK {
    yylval = RDB_new_parse_token(TOK_ROLLBACK, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ROLLBACK;
}

INSERT {
    yylval = RDB_new_parse_token(TOK_INSERT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_INSERT;
}

DELETE {
    yylval = RDB_new_parse_token(TOK_DELETE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DELETE;
}

UPDATE {
    yylval = RDB_new_parse_token(TOK_UPDATE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UPDATE;
}

CALL {
    yylval = RDB_new_parse_token(TOK_CALL, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CALL;
}

TYPE {
    yylval = RDB_new_parse_token(TOK_TYPE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
	return TOK_TYPE;
}

POSSREP {
    yylval = RDB_new_parse_token(TOK_POSSREP, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_POSSREP;
}

CONSTRAINT {
    yylval = RDB_new_parse_token(TOK_CONSTRAINT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CONSTRAINT;
}

OPERATOR {
    yylval = RDB_new_parse_token(TOK_OPERATOR, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_OPERATOR;
}

RETURNS {
    yylval = RDB_new_parse_token(TOK_RETURNS, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RETURNS;
}

UPDATES {
    yylval = RDB_new_parse_token(TOK_UPDATES, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UPDATES;
}

RETURN {
    yylval = RDB_new_parse_token(TOK_RETURN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RETURN;
}

LOAD {
    yylval = RDB_new_parse_token(TOK_LOAD, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LOAD;
}

ORDER {
    yylval = RDB_new_parse_token(TOK_ORDER, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ORDER;
}

ASC {
    yylval = RDB_new_parse_token(TOK_ASC, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ASC;
}

DESC {
    yylval = RDB_new_parse_token(TOK_DESC, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DESC;
}

WITH {
    yylval = RDB_new_parse_token(TOK_WITH, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WITH;
}

TRY {
    yylval = RDB_new_parse_token(TOK_TRY, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TRY;
}

CATCH {
    yylval = RDB_new_parse_token(TOK_CATCH, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CATCH;
}

RAISE {
    yylval = RDB_new_parse_token(TOK_RAISE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RAISE;
}

IMPLEMENT {
    yylval = RDB_new_parse_token(TOK_IMPLEMENT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_IMPLEMENT;
}

\|\| {
    yylval = RDB_new_parse_token(TOK_CONCAT, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CONCAT;
}

[a-zA-Z][a-zA-Z0-9_#]* {
    int i;

    if (_RDB_parse_case_insensitive) {
        for (i = 0; yytext[i] != '\0'; i++) {
            yytext[i] = tolower(yytext[i]);
        }
    }

    yylval = RDB_new_parse_expr(RDB_var_ref(yytext, _RDB_parse_ecp),
    		whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ID;
}

\'([^']|\\\\|\\n|\\r|\\t)*\' {
    /*
     * Unescaped string - remove trailing 's
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_new_parse_expr(RDB_string_to_expr(yytext + 1, _RDB_parse_ecp),
    		whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_STRING;
}

\"([^"]|\\\\|\\n|\\r|\\t|\\\")*\" {
    /*
     * Handle escaped string.
     * The trailing quote guarantees that there can be
     * no trailing backslash.
     */
    process_escapes(yytext);

    /*
     * Remove trailing "
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_new_parse_expr(RDB_string_to_expr(yytext + 1, _RDB_parse_ecp),
    		whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_STRING;
}

\' {
    char buf[40];

    if (_RDB_parse_interactive)
	    sprintf(buf, "unbalanced '");
	else
	    sprintf(buf, "unbalanced ' at line %d", yylineno);
    RDB_raise_syntax(buf, _RDB_parse_ecp);
    return TOK_INVALID;
}

\" {
    char buf[40];

    if (_RDB_parse_interactive)
	    sprintf(buf, "unbalanced \"");
	else 
	    sprintf(buf, "unbalanced \" at line %d", yylineno);
    RDB_raise_syntax(buf, _RDB_parse_ecp);
    return TOK_INVALID;
}

[0-9]+ {
    yylval = RDB_new_parse_expr(RDB_int_to_expr((RDB_int) atoi(yytext),
    		_RDB_parse_ecp), whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_INTEGER;
}

[0-9]+"."[0-9]+[Ee][0-9]+ {
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) atof(yytext),
    		_RDB_parse_ecp), whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_FLOAT;
}

[0-9]+"."[0-9]+	{
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) atof(yytext),
    		_RDB_parse_ecp), whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_FLOAT;
}

\<> {
    yylval = RDB_new_parse_token(TOK_NE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_NE;
}

!=  {
    yylval = RDB_new_parse_token(TOK_NE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_NE;
}

\<= {
    yylval = RDB_new_parse_token(TOK_LE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LE;
}

\>=   {
    yylval = RDB_new_parse_token(TOK_GE, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_GE;
}

:= {
    yylval = RDB_new_parse_token(TOK_ASSIGN, whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ASSIGN;
}

[[:space:]]	{
    yylloc.first_line = yylloc.last_line = yylineno;
    /* Add whitespace to *whitecommp */
    if (whitecommp == NULL) {
        whitecommp = RDB_alloc(sizeof(RDB_object), _RDB_parse_ecp);
        if (whitecommp == NULL) {
            return TOK_INVALID;
        }
        RDB_init_obj(whitecommp);
        RDB_string_to_obj(whitecommp, yytext, _RDB_parse_ecp);
    } else {
    	RDB_append_string(whitecommp, yytext, _RDB_parse_ecp);
    }
}

--[^\n]*\n {
    /* Add one-line comment to *whitecommp */
    if (whitecommp == NULL) {
        whitecommp = RDB_alloc(sizeof(RDB_object), _RDB_parse_ecp);
        if (whitecommp == NULL) {
            return TOK_INVALID;
        }
        RDB_init_obj(whitecommp);
        RDB_string_to_obj(whitecommp, yytext, _RDB_parse_ecp);
    } else {
    	RDB_append_string(whitecommp, yytext, _RDB_parse_ecp);
    }
}

^#![^\n]*\n	/* Ignore lines starting with #! for interpreter files */

"/*" {
    int c;
    char comment_buf[COMMENT_BUF_SIZE];
    int buf_idx = 0;

    if (whitecommp == NULL) {
        whitecommp = RDB_alloc(sizeof(RDB_object), _RDB_parse_ecp);
        if (whitecommp == NULL) {
            return TOK_INVALID;
        }
        RDB_init_obj(whitecommp);
        RDB_string_to_obj(whitecommp, "/*", _RDB_parse_ecp);
    } else {
	    RDB_append_string(whitecommp, "/*", _RDB_parse_ecp);
    }    

    for (;;)
    {
        while ((c = input()) != '*' && c != EOF) {
        	/* eat up text of comment */
            comment_buf[buf_idx++] = (char) c;
            if (buf_idx == COMMENT_BUF_SIZE - 1) {
                comment_buf[buf_idx] = '\0';
	            RDB_append_string(whitecommp, comment_buf, _RDB_parse_ecp);
                buf_idx = 0;
	        }
        }    

        if (c == '*')
        {
            do {
	            comment_buf[buf_idx++] = (char) c;
	            if (buf_idx == COMMENT_BUF_SIZE - 1) {
	                comment_buf[buf_idx] = '\0';
		            RDB_append_string(whitecommp, comment_buf, _RDB_parse_ecp);
		            buf_idx = 0;
		        }
            } while ((c = input()) == '*');
            if (c == '/') {
	            comment_buf[buf_idx++] = '/';
	            if (buf_idx == COMMENT_BUF_SIZE - 1) {
	                comment_buf[buf_idx] = '\0';
		            RDB_append_string(whitecommp, comment_buf, _RDB_parse_ecp);
		            buf_idx = 0;
		        }
                break;    /* found the end */
            }
            comment_buf[buf_idx++] = (char) c;
            if (buf_idx == COMMENT_BUF_SIZE - 1) {
	            comment_buf[buf_idx] = '\0';
		        RDB_append_string(whitecommp, comment_buf, _RDB_parse_ecp);
		        buf_idx = 0;
		    }
        }

        if (c == EOF)
        {
            char buf[40];
            if (_RDB_parse_interactive)
                sprintf(buf, "unterminated comment");
            else
                sprintf(buf, "unterminated comment at line %d", yylineno);
            RDB_raise_syntax(buf, _RDB_parse_ecp);
            return TOK_INVALID;
        }
    }
    comment_buf[buf_idx] = '\0';
    RDB_append_string(whitecommp, comment_buf, _RDB_parse_ecp);
}

[=><+\-*/{},().;\[\]:] {
    yylval = RDB_new_parse_token(yytext[0], whitecommp, _RDB_parse_ecp);
    whitecommp = NULL;
    return yytext[0];
}

. {
    char buf[40];

    if (_RDB_parse_interactive)
    	sprintf(buf, "invalid character %c", yytext[0]);
    else
	    sprintf(buf, "invalid character %c at line %d", yytext[0], yylineno);
    RDB_raise_syntax(buf, _RDB_parse_ecp);
    return TOK_INVALID;
}

<<EOF>> {
    /*
     * If in interactive mode, read next line from input
     */
    char strbuf[256];

    if (!_RDB_parse_interactive)
        yyterminate();

    fputs(_RDB_parse_prompt, stdout);
    if (fgets(strbuf, sizeof(strbuf), stdin) == NULL) {
        yy_delete_buffer(YY_CURRENT_BUFFER);
        yyterminate();
    }

    _RDB_parse_buffer = yy_scan_string(strbuf);
}

%%
