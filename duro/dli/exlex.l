/*
 * $Id$
 *
 * Copyright (C) 2003-2011 Ren� Hartmann.
 * See the file COPYING for redistribution information.
 */

%option nounput
%option noyywrap
%option yylineno
%option nounistd

%{
#define YYSTYPE RDB_parse_node *

#include <rel/rdb.h>
#include "parse.h"
#include "exparse.h"
#include <string.h>
#include <math.h>
#include <ctype.h>

extern RDB_exec_context *_RDB_parse_ecp;

extern int _RDB_parse_interactive;

extern int _RDB_parse_case_insensitive;

YY_BUFFER_STATE _RDB_parse_buffer;

int _RDB_parse_start_token;

char *_RDB_parse_prompt;

/*
 * Process backslash escapes
 */
static void
process_escapes(char *txt)
{
    int i = 1, j = 1;

    for(;;) {
        if (yytext[i] == '\\') {
            switch (yytext[i + 1]) {
                case '\\':
                    yytext[j] = yytext[i + 1];
                    i += 2;
                    j++;
                    continue;
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
                default: ;
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
    yylval = RDB_new_parse_token(TOK_WHERE, _RDB_parse_ecp);
    return TOK_WHERE;
}

UNION {
    yylval = RDB_new_parse_token(TOK_UNION, _RDB_parse_ecp);
    return TOK_UNION;
}

INTERSECT {
    yylval = RDB_new_parse_token(TOK_INTERSECT, _RDB_parse_ecp);
    return TOK_INTERSECT;
}

MINUS {
    yylval = RDB_new_parse_token(TOK_MINUS, _RDB_parse_ecp);
    return TOK_MINUS;
}

SEMIMINUS {
    yylval = RDB_new_parse_token(TOK_SEMIMINUS, _RDB_parse_ecp);
    return TOK_SEMIMINUS;
}

SEMIJOIN {
    yylval = RDB_new_parse_token(TOK_SEMIJOIN, _RDB_parse_ecp);
    return TOK_SEMIJOIN;
}

JOIN {
    yylval = RDB_new_parse_token(TOK_JOIN, _RDB_parse_ecp);
    return TOK_JOIN;
}

WRAP {
    yylval = RDB_new_parse_token(TOK_WRAP, _RDB_parse_ecp);
    return TOK_WRAP;
}

UNWRAP {
    yylval = RDB_new_parse_token(TOK_UNWRAP, _RDB_parse_ecp);
    return TOK_UNWRAP;
}

GROUP {
    yylval = RDB_new_parse_token(TOK_GROUP, _RDB_parse_ecp);
    return TOK_GROUP;
}

UNGROUP {
    yylval = RDB_new_parse_token(TOK_UNGROUP, _RDB_parse_ecp);
    return TOK_UNGROUP;
}

FROM {
    yylval = RDB_new_parse_token(TOK_FROM, _RDB_parse_ecp);
    return TOK_FROM;
}

TUPLE {
    yylval = RDB_new_parse_token(TOK_TUPLE, _RDB_parse_ecp);
    return TOK_TUPLE;
}

RELATION {
    yylval = RDB_new_parse_token(TOK_RELATION, _RDB_parse_ecp);
    return TOK_RELATION;
}

ARRAY {
    yylval = RDB_new_parse_token(TOK_ARRAY, _RDB_parse_ecp);
    return TOK_ARRAY;
}

BUT {
    yylval = RDB_new_parse_token(TOK_BUT, _RDB_parse_ecp);
    return TOK_BUT;
}

AS {
    yylval = RDB_new_parse_token(TOK_AS, _RDB_parse_ecp);
    return TOK_AS;
}

EXTEND {
    yylval = RDB_new_parse_token(TOK_EXTEND, _RDB_parse_ecp);
    return TOK_EXTEND;
}

RENAME {
    yylval = RDB_new_parse_token(TOK_RENAME, _RDB_parse_ecp);
    return TOK_RENAME;
}

SUMMARIZE {
    yylval = RDB_new_parse_token(TOK_SUMMARIZE, _RDB_parse_ecp);
    return TOK_SUMMARIZE;
}

DIVIDEBY {
    yylval = RDB_new_parse_token(TOK_DIVIDEBY, _RDB_parse_ecp);
    return TOK_DIVIDEBY;
}

PER {
    yylval = RDB_new_parse_token(TOK_PER, _RDB_parse_ecp);
    return TOK_PER;
}

ADD {
    yylval = RDB_new_parse_token(TOK_ADD, _RDB_parse_ecp);
    return TOK_ADD;
}

MATCHES {
    yylval = RDB_new_parse_token(TOK_MATCHES, _RDB_parse_ecp);
    return TOK_MATCHES;
}

IN {
    yylval = RDB_new_parse_token(TOK_IN, _RDB_parse_ecp);
    return TOK_IN;
}

SUBSET_OF {
    yylval = RDB_new_parse_token(TOK_SUBSET_OF, _RDB_parse_ecp);
    return TOK_SUBSET_OF;
}

OR {
    yylval = RDB_new_parse_token(TOK_OR, _RDB_parse_ecp);
    return TOK_OR;
}

AND {
    yylval = RDB_new_parse_token(TOK_AND, _RDB_parse_ecp);
    return TOK_AND;
}

NOT {
    yylval = RDB_new_parse_token(TOK_NOT, _RDB_parse_ecp);
    return TOK_NOT;
}

IF {
    yylval = RDB_new_parse_token(TOK_IF, _RDB_parse_ecp);
    return TOK_IF;
}

THEN {
    yylval = RDB_new_parse_token(TOK_THEN, _RDB_parse_ecp);
    return TOK_THEN;
}

ELSE {
    yylval = RDB_new_parse_token(TOK_ELSE, _RDB_parse_ecp);
    return TOK_ELSE;
}

END {
    yylval = RDB_new_parse_token(TOK_END, _RDB_parse_ecp);
    return TOK_END;
}

CASE {
    yylval = RDB_new_parse_token(TOK_CASE, _RDB_parse_ecp);
    return TOK_CASE;
}

WHEN {
    yylval = RDB_new_parse_token(TOK_WHEN, _RDB_parse_ecp);
    return TOK_WHEN;
}

FOR {
    yylval = RDB_new_parse_token(TOK_FOR, _RDB_parse_ecp);
    return TOK_FOR;
}

DO {
    yylval = RDB_new_parse_token(TOK_FOR, _RDB_parse_ecp);
    return TOK_FOR;
}

TO {
    yylval = RDB_new_parse_token(TOK_TO, _RDB_parse_ecp);
    return TOK_TO;
}

WHILE {
    yylval = RDB_new_parse_token(TOK_WHILE, _RDB_parse_ecp);
    return TOK_WHILE;
}

LEAVE {
    yylval = RDB_new_parse_token(TOK_LEAVE, _RDB_parse_ecp);
    return TOK_LEAVE;
}

TRUE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_TRUE, _RDB_parse_ecp),
    		_RDB_parse_ecp);
    return TOK_LIT_BOOLEAN;
}

FALSE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_FALSE, _RDB_parse_ecp),
    		_RDB_parse_ecp);
    return TOK_LIT_BOOLEAN;
}

COUNT {
    yylval = RDB_new_parse_token(TOK_COUNT, _RDB_parse_ecp);
    return TOK_COUNT;
}

SUM {
    yylval = RDB_new_parse_token(TOK_SUM, _RDB_parse_ecp);
    return TOK_SUM;
}

AVG {
    yylval = RDB_new_parse_token(TOK_AVG, _RDB_parse_ecp);
    return TOK_AVG;
}

MAX {
    yylval = RDB_new_parse_token(TOK_MAX, _RDB_parse_ecp);
    return TOK_MAX;
}

MIN {
    yylval = RDB_new_parse_token(TOK_MIN, _RDB_parse_ecp);
    return TOK_MIN;
}

ALL {
    yylval = RDB_new_parse_token(TOK_ALL, _RDB_parse_ecp);
    return TOK_ALL;
}

ANY {
    yylval = RDB_new_parse_token(TOK_ANY, _RDB_parse_ecp);
    return TOK_ANY;
}

SAME_TYPE_AS {
    yylval = RDB_new_parse_token(TOK_SAME_TYPE_AS, _RDB_parse_ecp);
	return TOK_SAME_TYPE_AS;
}

SAME_HEADING_AS {
    yylval = RDB_new_parse_token(TOK_SAME_HEADING_AS, _RDB_parse_ecp);
	return TOK_SAME_HEADING_AS;
}

TABLE_DEE {
    yylval = RDB_new_parse_token(TOK_TABLE_DEE, _RDB_parse_ecp);
    return TOK_TABLE_DEE;
}

TABLE_DUM {
    yylval = RDB_new_parse_token(TOK_TABLE_DUM, _RDB_parse_ecp);
    return TOK_TABLE_DUM;
}

VAR {
    yylval = RDB_new_parse_token(TOK_VAR, _RDB_parse_ecp);
    return TOK_VAR;
}

DROP {
    yylval = RDB_new_parse_token(TOK_DROP, _RDB_parse_ecp);
    return TOK_DROP;
}

REAL {
    yylval = RDB_new_parse_token(TOK_REAL, _RDB_parse_ecp);
    return TOK_REAL;
}

PRIVATE {
    yylval = RDB_new_parse_token(TOK_PRIVATE, _RDB_parse_ecp);
    return TOK_PRIVATE;
}

BASE {
    yylval = RDB_new_parse_token(TOK_REAL, _RDB_parse_ecp);
    return TOK_REAL;
}

VIRTUAL {
    yylval = RDB_new_parse_token(TOK_VIRTUAL, _RDB_parse_ecp);
    return TOK_VIRTUAL;
}

KEY {
    yylval = RDB_new_parse_token(TOK_KEY, _RDB_parse_ecp);
    return TOK_KEY;
}

INIT {
    yylval = RDB_new_parse_token(TOK_INIT, _RDB_parse_ecp);
    return TOK_INIT;
}

BEGIN {
    yylval = RDB_new_parse_token(TOK_BEGIN, _RDB_parse_ecp);
    return TOK_BEGIN;
}

TRANSACTION {
    yylval = RDB_new_parse_token(TOK_TX, _RDB_parse_ecp);
    return TOK_TX;
}

TX {
    yylval = RDB_new_parse_token(TOK_TX, _RDB_parse_ecp);
    return TOK_TX;
}

COMMIT {
    yylval = RDB_new_parse_token(TOK_COMMIT, _RDB_parse_ecp);
    return TOK_COMMIT;
}

ROLLBACK {
    yylval = RDB_new_parse_token(TOK_ROLLBACK, _RDB_parse_ecp);
    return TOK_ROLLBACK;
}

INSERT {
    yylval = RDB_new_parse_token(TOK_INSERT, _RDB_parse_ecp);
    return TOK_INSERT;
}

DELETE {
    yylval = RDB_new_parse_token(TOK_DELETE, _RDB_parse_ecp);
    return TOK_DELETE;
}

UPDATE {
    yylval = RDB_new_parse_token(TOK_UPDATE, _RDB_parse_ecp);
    return TOK_UPDATE;
}

CALL {
    yylval = RDB_new_parse_token(TOK_CALL, _RDB_parse_ecp);
    return TOK_CALL;
}

TYPE {
    yylval = RDB_new_parse_token(TOK_TYPE, _RDB_parse_ecp);
	return TOK_TYPE;
}

POSSREP {
    yylval = RDB_new_parse_token(TOK_POSSREP, _RDB_parse_ecp);
    return TOK_POSSREP;
}

CONSTRAINT {
    yylval = RDB_new_parse_token(TOK_CONSTRAINT, _RDB_parse_ecp);
    return TOK_CONSTRAINT;
}

OPERATOR {
    yylval = RDB_new_parse_token(TOK_OPERATOR, _RDB_parse_ecp);
    return TOK_OPERATOR;
}

RETURNS {
    yylval = RDB_new_parse_token(TOK_RETURNS, _RDB_parse_ecp);
    return TOK_RETURNS;
}

UPDATES {
    yylval = RDB_new_parse_token(TOK_UPDATES, _RDB_parse_ecp);
    return TOK_UPDATES;
}

RETURN {
    yylval = RDB_new_parse_token(TOK_RETURN, _RDB_parse_ecp);
    return TOK_RETURN;
}

LOAD {
    yylval = RDB_new_parse_token(TOK_LOAD, _RDB_parse_ecp);
    return TOK_LOAD;
}

ORDER {
    yylval = RDB_new_parse_token(TOK_ORDER, _RDB_parse_ecp);
    return TOK_ORDER;
}

ASC {
    yylval = RDB_new_parse_token(TOK_ASC, _RDB_parse_ecp);
    return TOK_ASC;
}

DESC {
    yylval = RDB_new_parse_token(TOK_DESC, _RDB_parse_ecp);
    return TOK_DESC;
}

WITH {
    yylval = RDB_new_parse_token(TOK_WITH, _RDB_parse_ecp);
    return TOK_WITH;
}

TRY {
    yylval = RDB_new_parse_token(TOK_TRY, _RDB_parse_ecp);
    return TOK_TRY;
}

CATCH {
    yylval = RDB_new_parse_token(TOK_CATCH, _RDB_parse_ecp);
    return TOK_CATCH;
}

RAISE {
    yylval = RDB_new_parse_token(TOK_RAISE, _RDB_parse_ecp);
    return TOK_RAISE;
}

IMPLEMENT {
    yylval = RDB_new_parse_token(TOK_IMPLEMENT, _RDB_parse_ecp);
    return TOK_IMPLEMENT;
}

\|\| {
    yylval = RDB_new_parse_token(TOK_CONCAT, _RDB_parse_ecp);
    return TOK_CONCAT;
}

[a-zA-Z][a-zA-Z0-9_#]* {
    int i;

    if (_RDB_parse_case_insensitive) {
        for (i = 0; yytext[i] != '\0'; i++) {
            yytext[i] = toupper(yytext[i]);
        }
    }
    yylval = RDB_new_parse_expr(RDB_var_ref(yytext, _RDB_parse_ecp),
    		_RDB_parse_ecp);
    return TOK_ID;
}

\'([^']|\\\\|\\n|\\r|\\t)*\' {

    process_escapes(yytext);

    /*
     * Remove trailing 's
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_new_parse_expr(RDB_string_to_expr(yytext + 1, _RDB_parse_ecp),
    		_RDB_parse_ecp);
    return TOK_LIT_STRING;
}

\"([^"]|\\\\|\\n|\\r|\\t)*\" {

    process_escapes(yytext);

    /*
     * Remove trailing "s
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_new_parse_expr(RDB_string_to_expr(yytext + 1, _RDB_parse_ecp),
    		_RDB_parse_ecp);
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
    		_RDB_parse_ecp), _RDB_parse_ecp);
    return TOK_LIT_INTEGER;
}

[0-9]+"."[0-9]+[Ee][0-9]+ {
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) atof(yytext),
    		_RDB_parse_ecp), _RDB_parse_ecp);
    return TOK_LIT_FLOAT;
}

[0-9]+"."[0-9]+	{
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) atof(yytext),
    		_RDB_parse_ecp), _RDB_parse_ecp);
    return TOK_LIT_FLOAT;
}

\<> {
    yylval = RDB_new_parse_token(TOK_NE, _RDB_parse_ecp);
    return TOK_NE;
}

!=  {
    yylval = RDB_new_parse_token(TOK_NE, _RDB_parse_ecp);
    return TOK_NE;
}

\<= {
    yylval = RDB_new_parse_token(TOK_LE, _RDB_parse_ecp);
    return TOK_LE;
}

\>=   {
    yylval = RDB_new_parse_token(TOK_GE, _RDB_parse_ecp);
    return TOK_GE;
}

:= {
    yylval = RDB_new_parse_token(TOK_ASSIGN, _RDB_parse_ecp);
    return TOK_ASSIGN;
}

[[:space:]]	yylloc.first_line = yylloc.last_line = yylineno;

--[^\n]*\n	/* One-line comment */

^#![^\n]*\n	/* For interpreter files */

"/*" {
        int c;

        for ( ; ; )
        {
            while ((c = input()) != '*' && c != EOF)
                ;    /* eat up text of comment */

            if ( c == '*' )
            {
                while ((c = input()) == '*')
                    ;
                if (c == '/')
                    break;    /* found the end */
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
    }

[=><+\-*/{},().;\[\]:] {
    yylval = RDB_new_parse_token(yytext[0], _RDB_parse_ecp);

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
