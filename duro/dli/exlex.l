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
#include <stdio.h>

#define COMMENT_BUF_SIZE 10

extern RDB_exec_context *RDB_parse_ecp;

static int RDB_parse_interactive = 0;
static int RDB_parse_case_insensitive = 1;
static RDB_readline_fn *RDB_parse_readline_fnp;

YY_BUFFER_STATE RDB_parse_buffer;

int RDB_parse_start_token;

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
RDB_parse_init_buf(FILE *f)
{
    if (RDB_parse_interactive)
	    RDB_parse_buffer = yy_scan_string("");
	else {
		RDB_parse_buffer = yy_create_buffer(f, YY_BUF_SIZE);
		yy_switch_to_buffer(RDB_parse_buffer);
	}
}

void
RDB_parse_start_exp(void)
{
    RDB_parse_start_token = TOK_START_EXP;
}

void
RDB_parse_start_stmt(void)
{
    RDB_parse_start_token = TOK_START_STMT;
}

%}

%%

%{

if (RDB_parse_start_token != 0)
{
    int t = RDB_parse_start_token;
    RDB_parse_start_token = 0;
    return t;
}

yylloc.first_line = yylloc.last_line = yylineno;

%}

WHERE {
    yylval = RDB_new_parse_token(TOK_WHERE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WHERE;
}

UNION {
    yylval = RDB_new_parse_token(TOK_UNION, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UNION;
}

INTERSECT {
    yylval = RDB_new_parse_token(TOK_INTERSECT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_INTERSECT;
}

MINUS {
    yylval = RDB_new_parse_token(TOK_MINUS, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MINUS;
}

SEMIMINUS {
    yylval = RDB_new_parse_token(TOK_SEMIMINUS, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SEMIMINUS;
}

SEMIJOIN {
    yylval = RDB_new_parse_token(TOK_SEMIJOIN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SEMIJOIN;
}

JOIN {
    yylval = RDB_new_parse_token(TOK_JOIN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_JOIN;
}

WRAP {
    yylval = RDB_new_parse_token(TOK_WRAP, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WRAP;
}

UNWRAP {
    yylval = RDB_new_parse_token(TOK_UNWRAP, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UNWRAP;
}

GROUP {
    yylval = RDB_new_parse_token(TOK_GROUP, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_GROUP;
}

UNGROUP {
    yylval = RDB_new_parse_token(TOK_UNGROUP, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UNGROUP;
}

FROM {
    yylval = RDB_new_parse_token(TOK_FROM, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_FROM;
}

TUPLE {
    yylval = RDB_new_parse_token(TOK_TUPLE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TUPLE;
}

TUP {
    yylval = RDB_new_parse_token(TOK_TUPLE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TUPLE;
}

RELATION {
    yylval = RDB_new_parse_token(TOK_RELATION, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RELATION;
}

REL {
    yylval = RDB_new_parse_token(TOK_RELATION, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RELATION;
}

ARRAY {
    yylval = RDB_new_parse_token(TOK_ARRAY, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ARRAY;
}

BUT {
    yylval = RDB_new_parse_token(TOK_BUT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_BUT;
}

AS {
    yylval = RDB_new_parse_token(TOK_AS, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_AS;
}

EXTEND {
    yylval = RDB_new_parse_token(TOK_EXTEND, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_EXTEND;
}

RENAME {
    yylval = RDB_new_parse_token(TOK_RENAME, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RENAME;
}

SUMMARIZE {
    yylval = RDB_new_parse_token(TOK_SUMMARIZE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SUMMARIZE;
}

DIVIDEBY {
    yylval = RDB_new_parse_token(TOK_DIVIDEBY, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DIVIDEBY;
}

PER {
    yylval = RDB_new_parse_token(TOK_PER, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_PER;
}

ADD {
    yylval = RDB_new_parse_token(TOK_ADD, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ADD;
}

MATCHES {
    yylval = RDB_new_parse_token(TOK_MATCHES, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MATCHES;
}

IN {
    yylval = RDB_new_parse_token(TOK_IN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_IN;
}

SUBSET_OF {
    yylval = RDB_new_parse_token(TOK_SUBSET_OF, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SUBSET_OF;
}

OR {
    yylval = RDB_new_parse_token(TOK_OR, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_OR;
}

AND {
    yylval = RDB_new_parse_token(TOK_AND, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_AND;
}

XOR {
    yylval = RDB_new_parse_token(TOK_XOR, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_XOR;
}

NOT {
    yylval = RDB_new_parse_token(TOK_NOT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_NOT;
}

IF {
    yylval = RDB_new_parse_token(TOK_IF, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_IF;
}

THEN {
    yylval = RDB_new_parse_token(TOK_THEN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_THEN;
}

ELSE {
    yylval = RDB_new_parse_token(TOK_ELSE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ELSE;
}

END {
    yylval = RDB_new_parse_token(TOK_END, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_END;
}

CASE {
    yylval = RDB_new_parse_token(TOK_CASE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CASE;
}

WHEN {
    yylval = RDB_new_parse_token(TOK_WHEN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WHEN;
}

FOR {
    yylval = RDB_new_parse_token(TOK_FOR, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_FOR;
}

FOREACH {
    yylval = RDB_new_parse_token(TOK_FOREACH, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_FOREACH;
}

DO {
    yylval = RDB_new_parse_token(TOK_FOR, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_FOR;
}

TO {
    yylval = RDB_new_parse_token(TOK_TO, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TO;
}

WHILE {
    yylval = RDB_new_parse_token(TOK_WHILE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WHILE;
}

LEAVE {
    yylval = RDB_new_parse_token(TOK_LEAVE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LEAVE;
}

TRUE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_TRUE, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_BOOLEAN;
}

FALSE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_FALSE, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_BOOLEAN;
}

COUNT {
    yylval = RDB_new_parse_token(TOK_COUNT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_COUNT;
}

SUM {
    yylval = RDB_new_parse_token(TOK_SUM, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_SUM;
}

AVG {
    yylval = RDB_new_parse_token(TOK_AVG, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_AVG;
}

MAX {
    yylval = RDB_new_parse_token(TOK_MAX, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MAX;
}

MIN {
    yylval = RDB_new_parse_token(TOK_MIN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_MIN;
}

ALL {
    yylval = RDB_new_parse_token(TOK_ALL, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ALL;
}

ANY {
    yylval = RDB_new_parse_token(TOK_ANY, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ANY;
}

SAME_TYPE_AS {
    yylval = RDB_new_parse_token(TOK_SAME_TYPE_AS, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
	return TOK_SAME_TYPE_AS;
}

SAME_HEADING_AS {
    yylval = RDB_new_parse_token(TOK_SAME_HEADING_AS, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
	return TOK_SAME_HEADING_AS;
}

TABLE_DEE {
    yylval = RDB_new_parse_token(TOK_TABLE_DEE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DEE;
}

DEE {
    yylval = RDB_new_parse_token(TOK_TABLE_DEE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DEE;
}

TABLE_DUM {
    yylval = RDB_new_parse_token(TOK_TABLE_DUM, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DUM;
}

DUM {
    yylval = RDB_new_parse_token(TOK_TABLE_DUM, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TABLE_DUM;
}

VAR {
    yylval = RDB_new_parse_token(TOK_VAR, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_VAR;
}

DROP {
    yylval = RDB_new_parse_token(TOK_DROP, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DROP;
}

REAL {
    yylval = RDB_new_parse_token(TOK_REAL, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_REAL;
}

PRIVATE {
    yylval = RDB_new_parse_token(TOK_PRIVATE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_PRIVATE;
}

BASE {
    yylval = RDB_new_parse_token(TOK_REAL, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_REAL;
}

VIRTUAL {
    yylval = RDB_new_parse_token(TOK_VIRTUAL, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_VIRTUAL;
}

KEY {
    yylval = RDB_new_parse_token(TOK_KEY, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_KEY;
}

DEFAULT {
    yylval = RDB_new_parse_token(TOK_DEFAULT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DEFAULT;
}

INIT {
    yylval = RDB_new_parse_token(TOK_INIT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_INIT;
}

BEGIN {
    yylval = RDB_new_parse_token(TOK_BEGIN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_BEGIN;
}

TRANSACTION {
    yylval = RDB_new_parse_token(TOK_TX, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TX;
}

TX {
    yylval = RDB_new_parse_token(TOK_TX, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TX;
}

COMMIT {
    yylval = RDB_new_parse_token(TOK_COMMIT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_COMMIT;
}

ROLLBACK {
    yylval = RDB_new_parse_token(TOK_ROLLBACK, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ROLLBACK;
}

INSERT {
    yylval = RDB_new_parse_token(TOK_INSERT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_INSERT;
}

DELETE {
    yylval = RDB_new_parse_token(TOK_DELETE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DELETE;
}

UPDATE {
    yylval = RDB_new_parse_token(TOK_UPDATE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UPDATE;
}

CALL {
    yylval = RDB_new_parse_token(TOK_CALL, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CALL;
}

TYPE {
    yylval = RDB_new_parse_token(TOK_TYPE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
	return TOK_TYPE;
}

POSSREP {
    yylval = RDB_new_parse_token(TOK_POSSREP, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_POSSREP;
}

CONSTRAINT {
    yylval = RDB_new_parse_token(TOK_CONSTRAINT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CONSTRAINT;
}

OPERATOR {
    yylval = RDB_new_parse_token(TOK_OPERATOR, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_OPERATOR;
}

RETURNS {
    yylval = RDB_new_parse_token(TOK_RETURNS, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RETURNS;
}

UPDATES {
    yylval = RDB_new_parse_token(TOK_UPDATES, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_UPDATES;
}

RETURN {
    yylval = RDB_new_parse_token(TOK_RETURN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RETURN;
}

LOAD {
    yylval = RDB_new_parse_token(TOK_LOAD, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LOAD;
}

ORDER {
    yylval = RDB_new_parse_token(TOK_ORDER, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ORDER;
}

ASC {
    yylval = RDB_new_parse_token(TOK_ASC, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ASC;
}

DESC {
    yylval = RDB_new_parse_token(TOK_DESC, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_DESC;
}

WITH {
    yylval = RDB_new_parse_token(TOK_WITH, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_WITH;
}

TRY {
    yylval = RDB_new_parse_token(TOK_TRY, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_TRY;
}

CATCH {
    yylval = RDB_new_parse_token(TOK_CATCH, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CATCH;
}

RAISE {
    yylval = RDB_new_parse_token(TOK_RAISE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_RAISE;
}

IMPLEMENT {
    yylval = RDB_new_parse_token(TOK_IMPLEMENT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_IMPLEMENT;
}

INDEX {
    yylval = RDB_new_parse_token(TOK_INDEX, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_INDEX;
}

ORDERED {
    yylval = RDB_new_parse_token(TOK_ORDERED, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ORDERED;
}

EXPLAIN {
    yylval = RDB_new_parse_token(TOK_EXPLAIN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_EXPLAIN;
} 

\|\| {
    yylval = RDB_new_parse_token(TOK_CONCAT, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_CONCAT;
}

[a-zA-Z][a-zA-Z0-9_#]* {
    int i;

    if (RDB_parse_case_insensitive) {
        for (i = 0; yytext[i] != '\0'; i++) {
            yytext[i] = tolower(yytext[i]);
        }
    }

    yylval = RDB_new_parse_expr(RDB_var_ref(yytext, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ID;
}

\'([^']|\\\\|\\n|\\r|\\t)*\' {
    /*
     * Unescaped string - remove trailing 's
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_new_parse_expr(RDB_string_to_expr(yytext + 1, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
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
    yylval = RDB_new_parse_expr(RDB_string_to_expr(yytext + 1, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_STRING;
}

\' {
    char buf[40];

    if (RDB_parse_interactive)
	    sprintf(buf, "unbalanced '");
	else
	    sprintf(buf, "unbalanced ' at line %d", yylineno);
    RDB_raise_syntax(buf, RDB_parse_ecp);
    return TOK_INVALID;
}

\" {
    char buf[40];

    if (RDB_parse_interactive)
	    sprintf(buf, "unbalanced \"");
	else 
	    sprintf(buf, "unbalanced \" at line %d", yylineno);
    RDB_raise_syntax(buf, RDB_parse_ecp);
    return TOK_INVALID;
}

[0-9]+ {
    yylval = RDB_new_parse_expr(RDB_int_to_expr((RDB_int) atoi(yytext),
    		RDB_parse_ecp), whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_INTEGER;
}

[0-9]+"."[0-9]+[Ee][0-9]+ {
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) atof(yytext),
    		RDB_parse_ecp), whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_FLOAT;
}

[0-9]+"."[0-9]+	{
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) atof(yytext),
    		RDB_parse_ecp), whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LIT_FLOAT;
}

\<> {
    yylval = RDB_new_parse_token(TOK_NE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_NE;
}

!=  {
    yylval = RDB_new_parse_token(TOK_NE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_NE;
}

\<= {
    yylval = RDB_new_parse_token(TOK_LE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_LE;
}

\>=   {
    yylval = RDB_new_parse_token(TOK_GE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_GE;
}

:= {
    yylval = RDB_new_parse_token(TOK_ASSIGN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return TOK_ASSIGN;
}

[[:space:]]	{
    yylloc.first_line = yylloc.last_line = yylineno;
    /* Add whitespace to *whitecommp */
    if (whitecommp == NULL) {
        whitecommp = RDB_alloc(sizeof(RDB_object), RDB_parse_ecp);
        if (whitecommp == NULL) {
            return TOK_INVALID;
        }
        RDB_init_obj(whitecommp);
        RDB_string_to_obj(whitecommp, yytext, RDB_parse_ecp);
    } else {
    	RDB_append_string(whitecommp, yytext, RDB_parse_ecp);
    }
}

--[^\n]*\n {
    /* Add one-line comment to *whitecommp */
    if (whitecommp == NULL) {
        whitecommp = RDB_alloc(sizeof(RDB_object), RDB_parse_ecp);
        if (whitecommp == NULL) {
            return TOK_INVALID;
        }
        RDB_init_obj(whitecommp);
        RDB_string_to_obj(whitecommp, yytext, RDB_parse_ecp);
    } else {
    	RDB_append_string(whitecommp, yytext, RDB_parse_ecp);
    }
}

^#![^\n]*\n	/* Ignore lines starting with #! for interpreter files */

"/*" {
    int c;
    char comment_buf[COMMENT_BUF_SIZE];
    int buf_idx = 0;

    if (whitecommp == NULL) {
        whitecommp = RDB_alloc(sizeof(RDB_object), RDB_parse_ecp);
        if (whitecommp == NULL) {
            return TOK_INVALID;
        }
        RDB_init_obj(whitecommp);
        RDB_string_to_obj(whitecommp, "/*", RDB_parse_ecp);
    } else {
	    RDB_append_string(whitecommp, "/*", RDB_parse_ecp);
    }    

    for (;;)
    {
        while ((c = input()) != '*' && c != EOF) {
        	/* eat up text of comment */
            comment_buf[buf_idx++] = (char) c;
            if (buf_idx == COMMENT_BUF_SIZE - 1) {
                comment_buf[buf_idx] = '\0';
	            RDB_append_string(whitecommp, comment_buf, RDB_parse_ecp);
                buf_idx = 0;
	        }
        }    

        if (c == '*')
        {
            do {
	            comment_buf[buf_idx++] = (char) c;
	            if (buf_idx == COMMENT_BUF_SIZE - 1) {
	                comment_buf[buf_idx] = '\0';
		            RDB_append_string(whitecommp, comment_buf, RDB_parse_ecp);
		            buf_idx = 0;
		        }
            } while ((c = input()) == '*');
            if (c == '/') {
	            comment_buf[buf_idx++] = '/';
	            if (buf_idx == COMMENT_BUF_SIZE - 1) {
	                comment_buf[buf_idx] = '\0';
		            RDB_append_string(whitecommp, comment_buf, RDB_parse_ecp);
		            buf_idx = 0;
		        }
                break;    /* found the end */
            }
            comment_buf[buf_idx++] = (char) c;
            if (buf_idx == COMMENT_BUF_SIZE - 1) {
	            comment_buf[buf_idx] = '\0';
		        RDB_append_string(whitecommp, comment_buf, RDB_parse_ecp);
		        buf_idx = 0;
		    }
        }

        if (c == EOF)
        {
            char buf[40];
            if (RDB_parse_interactive)
                sprintf(buf, "unterminated comment");
            else
                sprintf(buf, "unterminated comment at line %d", yylineno);
            RDB_raise_syntax(buf, RDB_parse_ecp);
            return TOK_INVALID;
        }
    }
    comment_buf[buf_idx] = '\0';
    RDB_append_string(whitecommp, comment_buf, RDB_parse_ecp);
}

[=><+\-*/{},().;\[\]:] {
    yylval = RDB_new_parse_token(yytext[0], whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return yytext[0];
}

. {
    char buf[40];

    if (RDB_parse_interactive)
    	sprintf(buf, "invalid character %c", yytext[0]);
    else
	    sprintf(buf, "invalid character %c at line %d", yytext[0], yylineno);
    RDB_raise_syntax(buf, RDB_parse_ecp);
    return TOK_INVALID;
}

<<EOF>> {
    /*
     * If in interactive mode, read next line from input
     */
    char *line;

    if (!RDB_parse_interactive)
        yyterminate();

    line = (*RDB_parse_readline_fnp) ();
    if (line == NULL) {
        yy_delete_buffer(YY_CURRENT_BUFFER);
        yyterminate();
    }

    RDB_parse_buffer = yy_scan_string(line); 
    free(line);
}

%%

void
RDB_parse_set_readline_fn(RDB_readline_fn *fnp) {
    RDB_parse_readline_fnp = fnp;
}

RDB_bool
RDB_parse_get_interactive(void) {
    return (RDB_bool) (RDB_parse_interactive != 0);
}

void
RDB_parse_set_interactive(RDB_bool ia) {
    RDB_parse_interactive = (int) ia;
}

void
RDB_parse_set_case_insensitive(RDB_bool is) {
    RDB_parse_case_insensitive = (int) is;
}
