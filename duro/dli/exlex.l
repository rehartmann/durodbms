/* $Id$ */

%option nounput

%{
#define YYSTYPE RDB_expression *

#include <rel/rdb.h>
#include "exparse.h"
#include <string.h>
#include <math.h>

int
_RDB_parse_add_exp(RDB_expression *exp);

%}

%%

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

TRUE {
    yylval = RDB_bool_to_expr(RDB_TRUE);
    _RDB_parse_add_exp(yylval);
    return TOK_LIT_BOOLEAN;
}

FALSE {
    yylval = RDB_bool_to_expr(RDB_FALSE);
    _RDB_parse_add_exp(yylval);
    return TOK_LIT_BOOLEAN;
}

INTEGER {
    return TOK_INTEGER;
}

RATIONAL {
    return TOK_RATIONAL;
}

STRING {
    return TOK_STRING;
}

IS_EMPTY {
    return TOK_IS_EMPTY;
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

TABLE_DEE {
    return TOK_TABLE_DEE;
}

TABLE_DUM {
    return TOK_TABLE_DUM;
}

\|\| {
    return TOK_CONCAT;
}

[a-zA-Z][a-zA-Z0-9_#]* {
    yylval = RDB_expr_attr(yytext);
    _RDB_parse_add_exp(yylval);
    return TOK_ID;
}

\"[^"]*\" {
    /*
     * Remove trailing "s
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_string_to_expr(yytext + 1);
    _RDB_parse_add_exp(yylval);
    return TOK_LIT_STRING;
}

-?[0-9]+ {
    yylval = RDB_int_to_expr((RDB_int) atoi(yytext));
    _RDB_parse_add_exp(yylval);
    return TOK_LIT_INTEGER;
}

-?[0-9]+"."[0-9]+[Ee][0-9]+ {
    yylval = RDB_rational_to_expr((RDB_rational) atof(yytext));
    _RDB_parse_add_exp(yylval);
    return TOK_LIT_FLOAT;
}

-?[0-9]+"."[0-9]+	{
    yylval = RDB_rational_to_expr((RDB_rational) atof(yytext));
    _RDB_parse_add_exp(yylval);
    return TOK_LIT_FLOAT;
}

\<\> return TOK_NE;

\<\= return TOK_LE;

\>\= return TOK_GE;

[=><+\-*/{},().] return yytext[0];

[[:space:]]

.   {
    return INVALID;
}

%%
