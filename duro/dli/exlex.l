/* $Id */

%{
#define YYSTYPE RDB_expression *
#include <rel/rdb.h>
#include "exparse.h"
#include <string.h>
#include <math.h>

extern const char *expr_in;
#define YY_INPUT(buf,result,max_size) \
    { \
    int size = strlen(expr_in); \
    if (size > max_size) size = max_size; \
    if (size > 0) { \
        memcpy(buf, expr_in, size); \
        expr_in += size; \
    } \
    result = size; \
    }
%}

%%

WHERE   {
        return WHERE;
        }

UNION   {
        return UNION;
        }

INTERSECT {
        return INTERSECT;
        }

MINUS   {
        return MINUS;
        }

JOIN    {
        return JOIN;
        }

FROM    {
        return FROM;
        }

TUPLE   {
        return TUPLE;
        }

ALL[ \t]+BUT {
        return ALL_BUT;
        }

AS      {
        return AS;
        }

RENAME  {
        return RENAME;
        }

SUMMARIZE {
        return SUMMARIZE;
        }

RENAME  {
        return RENAME;
        }

SUMMARIZE {
        return SUMMARIZE;
        }

PER     {
        return PER;
        }

ADD     {
        return ADD;
        }

MATCHES {
        return MATCHES;
        }

TRUE    {
        yylval = RDB_bool_const(RDB_TRUE);
}

FALSE    {
        yylval = RDB_bool_const(RDB_FALSE);
}

[a-zA-Z][a-zA-Z0-9]* {
        printf("ID: %s\n", yytext);
	yylval = RDB_expr_attr(yytext);
        return ID;
        }

\".*\"	{
        /*
         * Remove trailing "s
         */
        printf("STRING: %s\n", yytext);
        yytext[(strlen(yytext) - 1)] = '\0';
        yylval = RDB_string_const(yytext + 1);
        return STRING;
        }

[0-9]+	{
        yylval = RDB_int_const((RDB_int) atoi(yytext));
        return INTEGER;
        }

[0-9]+"."[0-9]+[Ee][0-9]+ {
        yylval = RDB_rational_const((RDB_rational) atof(yytext));
        return FLOAT;
        }

[0-9]+"."[0-9]+	{
        yylval = RDB_rational_const((RDB_rational) atof(yytext));
        return DECIMAL;
        }

[=><+\-]   return (int)yytext[0];

%%
