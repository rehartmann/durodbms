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
static RDB_read_line_fn *RDB_parse_read_line_fnp;
static RDB_free_line_fn *RDB_parse_free_line_fnp;

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

static int create_token(int tok)
{
    yylval = RDB_new_parse_token(tok, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    return tok;
}

%}

%%

%{

if (RDB_parse_start_token != 0)
{
    int t = RDB_parse_start_token;
    RDB_parse_start_token = 0;
    yylval = NULL; /* Otherwise the %destructor would try to destroy it */
    return t;
}

yylloc.first_line = yylloc.last_line = yylineno;

%}

WHERE {
    return create_token(TOK_WHERE);
}

UNION {
    return create_token(TOK_UNION);
}

INTERSECT {
    return create_token(TOK_INTERSECT);
}

MINUS {
    return create_token(TOK_MINUS);
}

SEMIMINUS {
    return create_token(TOK_SEMIMINUS);
}

SEMIJOIN {
    return create_token(TOK_SEMIJOIN);
}

MATCHING {
    return create_token(TOK_MATCHING);
}

JOIN {
    return create_token(TOK_JOIN);
}

WRAP {
    return create_token(TOK_WRAP);
}

UNWRAP {
    return create_token(TOK_UNWRAP);
}

GROUP {
    return create_token(TOK_GROUP);
}

UNGROUP {
    return create_token(TOK_UNGROUP);
}

FROM {
    return create_token(TOK_FROM);
}

TUPLE {
    return create_token(TOK_TUPLE);
}

TUP {
    return create_token(TOK_TUPLE);
}

RELATION {
    return create_token(TOK_RELATION);
}

REL {
    return create_token(TOK_RELATION);
}

ARRAY {
    return create_token(TOK_ARRAY);
}

BUT {
    return create_token(TOK_BUT);
}

AS {
    return create_token(TOK_AS);
}

EXTEND {
    return create_token(TOK_EXTEND);
}

RENAME {
    return create_token(TOK_RENAME);
}

SUMMARIZE {
    return create_token(TOK_SUMMARIZE);
}

DIVIDEBY {
    return create_token(TOK_DIVIDEBY);
}

PER {
    return create_token(TOK_PER);
}

ADD {
    return create_token(TOK_ADD);
}

MATCHES {
    return create_token(TOK_MATCHES);
}

IN {
    return create_token(TOK_IN);
}

SUBSET_OF {
    return create_token(TOK_SUBSET_OF);
}

OR {
    return create_token(TOK_OR);
}

AND {
    return create_token(TOK_AND);
}

XOR {
    return create_token(TOK_XOR);
}

NOT {
    return create_token(TOK_NOT);
}

IF {
    return create_token(TOK_IF);
}

THEN {
    return create_token(TOK_THEN);
}

ELSE {
    return create_token(TOK_ELSE);
}

END {
    return create_token(TOK_END);
}

CASE {
    return create_token(TOK_CASE);
}

WHEN {
    return create_token(TOK_WHEN);
}

FOR {
    return create_token(TOK_FOR);
}

FOREACH {
    return create_token(TOK_FOREACH);
}

DO {
    return create_token(TOK_FOR);
}

TO {
    return create_token(TOK_TO);
}

WHILE {
    return create_token(TOK_WHILE);
}

LEAVE {
    return create_token(TOK_LEAVE);
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
    return create_token(TOK_COUNT);
}

SUM {
    return create_token(TOK_SUM);
}

AVG {
    return create_token(TOK_AVG);
}

MAX {
    return create_token(TOK_MAX);
}

MIN {
    return create_token(TOK_MIN);
}

ALL {
    return create_token(TOK_ALL);
}

ANY {
    return create_token(TOK_ANY);
}

SAME_TYPE_AS {
    return create_token(TOK_SAME_TYPE_AS);
}

SAME_HEADING_AS {
    return create_token(TOK_SAME_HEADING_AS);
}

TABLE_DEE {
    return create_token(TOK_TABLE_DEE);
}

DEE {
    return create_token(TOK_TABLE_DEE);
}

TABLE_DUM {
    return create_token(TOK_TABLE_DUM);
}

DUM {
    return create_token(TOK_TABLE_DUM);
}

VAR {
    return create_token(TOK_VAR);
}

DROP {
    return create_token(TOK_DROP);
}

REAL {
    return create_token(TOK_REAL);
}

PRIVATE {
    return create_token(TOK_PRIVATE);
}

BASE {
    return create_token(TOK_REAL);
}

VIRTUAL {
    return create_token(TOK_VIRTUAL);
}

KEY {
    return create_token(TOK_KEY);
}

DEFAULT {
    return create_token(TOK_DEFAULT);
}

INIT {
    return create_token(TOK_INIT);
}

BEGIN {
    return create_token(TOK_BEGIN);
}

TRANSACTION {
    return create_token(TOK_TX);
}

TX {
    return create_token(TOK_TX);
}

COMMIT {
    return create_token(TOK_COMMIT);
}

ROLLBACK {
    return create_token(TOK_ROLLBACK);
}

INSERT {
    return create_token(TOK_INSERT);
}

DELETE {
    return create_token(TOK_DELETE);
}

UPDATE {
    return create_token(TOK_UPDATE);
}

CALL {
    return create_token(TOK_CALL);
}

TYPE {
    return create_token(TOK_TYPE);
}

POSSREP {
    return create_token(TOK_POSSREP);
}

CONSTRAINT {
    return create_token(TOK_CONSTRAINT);
}

OPERATOR {
    return create_token(TOK_OPERATOR);
}

RETURNS {
    return create_token(TOK_RETURNS);
}

UPDATES {
    return create_token(TOK_UPDATES);
}

RETURN {
    return create_token(TOK_RETURN);
}

LOAD {
    return create_token(TOK_LOAD);
}

ORDER {
    return create_token(TOK_ORDER);
}

ASC {
    return create_token(TOK_ASC);
}

DESC {
    return create_token(TOK_DESC);
}

WITH {
    return create_token(TOK_WITH);
}

TRY {
    return create_token(TOK_TRY);
}

CATCH {
    return create_token(TOK_CATCH);
}

RAISE {
    return create_token(TOK_RAISE);
}

IMPLEMENT {
    return create_token(TOK_IMPLEMENT);
}

INDEX {
    return create_token(TOK_INDEX);
}

ORDERED {
    return create_token(TOK_ORDERED);
}

EXPLAIN {
    return create_token(TOK_EXPLAIN);
} 

\|\| {
    return create_token(TOK_CONCAT);
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

    line = (*RDB_parse_read_line_fnp) ();
    if (line == NULL) {
        yy_delete_buffer(YY_CURRENT_BUFFER);
        yyterminate();
    }

    RDB_parse_buffer = yy_scan_string(line); 
    (*RDB_parse_free_line_fnp) (line);
}

%%

void
RDB_parse_set_read_line_fn(RDB_read_line_fn *fnp)
{
    RDB_parse_read_line_fnp = fnp;
}

void
RDB_parse_set_free_line_fn(RDB_free_line_fn *fnp)
{
    RDB_parse_free_line_fnp = fnp;
}

RDB_bool
RDB_parse_get_interactive(void)
{
    return (RDB_bool) (RDB_parse_interactive != 0);
}

void
RDB_parse_set_interactive(RDB_bool ia)
{
    RDB_parse_interactive = (int) ia;
}

void
RDB_parse_set_case_insensitive(RDB_bool is)
{
    RDB_parse_case_insensitive = (int) is;
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

/*
 * Free the flex buffer
 */
void
RDB_parse_destroy_buf(void)
{
    yylex_destroy();
}
