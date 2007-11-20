/*
 * $Id$
 *
 * Copyright (C) 2003-2007 René Hartmann.
 * See the file COPYING for redistribution information.
 */

%option nounput
%option noyywrap
%option yylineno
%option nounistd

%{
#define YYSTYPE RDB_expression *

#include <rel/rdb.h>
#include "exparse.h"
#include <string.h>
#include <math.h>
#include <ctype.h>

extern RDB_exec_context *_RDB_parse_ecp;

extern int _RDB_parse_interactive;

extern int _RDB_parse_case_insensitive;

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
_RDB_parse_init_buf(void)
{
    yy_scan_string("");
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
    return TOK_WHERE;
}

UNION {
    return TOK_UNION;
}

INTERSECT {
    return TOK_INTERSECT;
}

MINUS {
    return TOK_MINUS;
}

SEMIMINUS {
    return TOK_SEMIMINUS;
}

SEMIJOIN {
    return TOK_SEMIJOIN;
}

JOIN {
    return TOK_JOIN;
}

WRAP {
    return TOK_WRAP;
}

UNWRAP {
    return TOK_UNWRAP;
}

GROUP {
    return TOK_GROUP;
}

UNGROUP {
    return TOK_UNGROUP;
}

FROM    {
    return TOK_FROM;
}

TUPLE   {
    return TOK_TUPLE;
}

RELATION {
    return TOK_RELATION;
}

ARRAY {
    return TOK_ARRAY;
}

BUT {
    return TOK_BUT;
}

AS      {
    return TOK_AS;
}

EXTEND  {
    return TOK_EXTEND;
}

RENAME  {
    return TOK_RENAME;
}

SUMMARIZE {
    return TOK_SUMMARIZE;
}

DIVIDEBY {
    return TOK_DIVIDEBY;
}

PER {
    return TOK_PER;
}

ADD {
    return TOK_ADD;
}

MATCHES {
    return TOK_MATCHES;
}

IN {
    return TOK_IN;
}

SUBSET_OF {
    return TOK_SUBSET_OF;
}

OR  {
    return TOK_OR;
}

AND {
    return TOK_AND;
}

NOT {
    return TOK_NOT;
}

IF {
    return TOK_IF;
}

THEN {
    return TOK_THEN;
}

ELSE {
    return TOK_ELSE;
}

END {
    return TOK_END;
}

FOR {
    return TOK_FOR;
}

DO {
    return TOK_FOR;
}

TO {
    return TOK_TO;
}

WHILE {
    return TOK_WHILE;
}

TRUE {
    yylval = RDB_bool_to_expr(RDB_TRUE, _RDB_parse_ecp);
    return TOK_LIT_BOOLEAN;
}

FALSE {
    yylval = RDB_bool_to_expr(RDB_FALSE, _RDB_parse_ecp);
    return TOK_LIT_BOOLEAN;
}

COUNT {
    return TOK_COUNT;
}

SUM {
    return TOK_SUM;
}

AVG {
    return TOK_AVG;
}

MAX {
    return TOK_MAX;
}

MIN {
    return TOK_MIN;
}

ALL {
    return TOK_ALL;
}

ANY {
    return TOK_ANY;
}

SAME_TYPE_AS {
	return TOK_SAME_TYPE_AS;
}

TABLE_DEE {
    return TOK_TABLE_DEE;
}

TABLE_DUM {
    return TOK_TABLE_DUM;
}

VAR {
    return TOK_VAR;
}

DROP {
    return TOK_DROP;
}

REAL {
    return TOK_REAL;
}

PRIVATE {
    return TOK_PRIVATE;
}

BASE {
    return TOK_REAL;
}

VIRTUAL {
    return TOK_VIRTUAL;
}

KEY {
    return TOK_KEY;
}

INIT {
    return TOK_INIT;
}

BEGIN {
    return TOK_BEGIN;
}

TRANSACTION {
    return TOK_TX;
}

TX {
    return TOK_TX;
}

COMMIT {
    return TOK_COMMIT;
}

ROLLBACK {
    return TOK_ROLLBACK;
}

INSERT {
    return TOK_INSERT;
}

DELETE {
    return TOK_DELETE;
}

UPDATE {
    return TOK_UPDATE;
}

CALL {
    return TOK_CALL;
}

TYPE {
	return TOK_TYPE;
}

POSSREP {
    return TOK_POSSREP;
}

CONSTRAINT {
    return TOK_CONSTRAINT;
}

OPERATOR {
    return TOK_OPERATOR;
}

RETURNS {
    return TOK_RETURNS;
}

UPDATES {
    return TOK_UPDATES;
}

RETURN {
    return TOK_RETURN;
}

LOAD {
    return TOK_LOAD;
}

ORDER {
    return TOK_ORDER;
}

ASC {
    return TOK_ASC;
}

DESC {
    return TOK_DESC;
}

\|\| {
    return TOK_CONCAT;
}

[a-zA-Z][a-zA-Z0-9_#]* {
    int i;

    if (_RDB_parse_case_insensitive) {
        for (i = 0; yytext[i] != '\0'; i++) {
            yytext[i] = toupper(yytext[i]);
        }
    }
    yylval = RDB_var_ref(yytext, _RDB_parse_ecp);
    return TOK_ID;
}

\"([^"]|\\\\|\\n|\\r|\\t)*\" {

    process_escapes(yytext);

    /*
     * Remove trailing "s
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_string_to_expr(yytext + 1, _RDB_parse_ecp);
    return TOK_LIT_STRING;
}

\'([^']|\\\\|\\n|\\r|\\t)*\' {

    process_escapes(yytext);

    /*
     * Remove trailing 's
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_string_to_expr(yytext + 1, _RDB_parse_ecp);
    return TOK_LIT_STRING;
}

-?[0-9]+ {
    yylval = RDB_int_to_expr((RDB_int) atoi(yytext), _RDB_parse_ecp);
    return TOK_LIT_INTEGER;
}

-?[0-9]+"."[0-9]+[Ee][0-9]+ {
    yylval = RDB_double_to_expr((RDB_double) atof(yytext), _RDB_parse_ecp);
    return TOK_LIT_FLOAT;
}

-?[0-9]+"."[0-9]+	{
    yylval = RDB_double_to_expr((RDB_double) atof(yytext), _RDB_parse_ecp);
    return TOK_LIT_FLOAT;
}

\<> return TOK_NE;

!= return TOK_NE;

\<= return TOK_LE;

\>= return TOK_GE;

:=	return TOK_ASSIGN;

[[:space:]]	yylloc.first_line = yylloc.last_line = yylineno;

[=><+\-*/{},().;\[\]] return yytext[0];

--[^\n]*\n	/* One-line comment */

. {
    char buf[40];

    sprintf(buf, "invalid character '%c' at line %d", yytext[0], yylineno);
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

    yy_switch_to_buffer(yy_scan_string(strbuf));
}

%%
