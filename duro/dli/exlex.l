/* $Id$ */

%{
#define YYSTYPE RDB_expression *

#include <rel/rdb.h>
#include "exparse.h"
#include <string.h>
#include <math.h>

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

FROM    {
    return TOK_FROM;
}

TUPLE   {
    return TOK_TUPLE;
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
    yylval = RDB_bool_const(RDB_TRUE);
}

FALSE {
    yylval = RDB_bool_const(RDB_FALSE);
}

IS_EMPTY {
    return TOK_IS_EMPTY;
}

COUNT {
    return TOK_COUNT;
}

COUNTD {
    return TOK_COUNTD;
}

SUM {
    return TOK_SUM;
}

SUMD {
    return TOK_SUMD;
}

AVG {
    return TOK_AVG;
}

AVGD {
    return TOK_AVGD;
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

\|\| {
    return TOK_CONCAT;
}

[a-zA-Z][a-zA-Z0-9_#]* {
    yylval = RDB_expr_attr(yytext);
    return TOK_ID;
}

\".*\" {
    /*
     * Remove trailing "s
     */
    yytext[(strlen(yytext) - 1)] = '\0';
    yylval = RDB_string_const(yytext + 1);
    return TOK_STRING;
}

[0-9]+ {
    yylval = RDB_int_const((RDB_int) atoi(yytext));
    return TOK_INTEGER;
}

[0-9]+"."[0-9]+[Ee][0-9]+ {
    yylval = RDB_rational_const((RDB_rational) atof(yytext));
    return TOK_FLOAT;
}

[0-9]+"."[0-9]+	{
    yylval = RDB_rational_const((RDB_rational) atof(yytext));
    return TOK_DECIMAL;
}

[=><+\-*/{},().] return yytext[0];

[[:space:]]

.   {
    return INVALID;
}

%%
