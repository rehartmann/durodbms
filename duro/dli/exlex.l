/*
 * Scanner definition for Duro D/T.
 *
 * Copyright (C) 2003-2009, 2012-2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

%option nounput
%option noyywrap
%option yylineno
%option nounistd

%{
#define YYSTYPE RDB_parse_node *

#include <rel/rdb.h>
#include <rel/typeimpl.h>
#include "parsenode.h"
#include "exparse.h"
#include <ctype.h>
#include <math.h>

#ifdef _WIN32
#define isfinite _finite
#endif

enum {
    COMMENT_BUF_SIZE = 64
};

extern RDB_exec_context *RDB_parse_ecp;

static int RDB_parse_interactive = 0;
static int RDB_parse_case_insensitive = 1;
static RDB_read_line_fn *RDB_parse_read_line_fnp;
static RDB_free_line_fn *RDB_parse_free_line_fnp;

YY_BUFFER_STATE RDB_parse_buffer;
int RDB_parse_buffer_valid = 0;

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

/*
 * Create a token, consuming *whitecommp and
 * checking for error.
 */
#define RETURN_TOKEN(tok) \
yylval = RDB_new_parse_token(tok, whitecommp, RDB_parse_ecp); \
whitecommp = NULL; \
if (yylval == NULL) \
    yyterminate(); \
return tok

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
    RETURN_TOKEN(TOK_WHERE);
}

D_UNION {
    RETURN_TOKEN(TOK_D_UNION);
}

UNION {
    RETURN_TOKEN(TOK_UNION);
}

INTERSECT {
    RETURN_TOKEN(TOK_INTERSECT);
}

MINUS {
    RETURN_TOKEN(TOK_MINUS);
}

SEMIMINUS {
    RETURN_TOKEN(TOK_SEMIMINUS);
}

SEMIJOIN {
    RETURN_TOKEN(TOK_SEMIJOIN);
}

MATCHING {
    RETURN_TOKEN(TOK_MATCHING);
}

JOIN {
    RETURN_TOKEN(TOK_JOIN);
}

WRAP {
    RETURN_TOKEN(TOK_WRAP);
}

UNWRAP {
    RETURN_TOKEN(TOK_UNWRAP);
}

GROUP {
    RETURN_TOKEN(TOK_GROUP);
}

UNGROUP {
    RETURN_TOKEN(TOK_UNGROUP);
}

FROM {
    RETURN_TOKEN(TOK_FROM);
}

TUPLE {
    RETURN_TOKEN(TOK_TUPLE);
}

TUP {
    RETURN_TOKEN(TOK_TUPLE);
}

RELATION {
    RETURN_TOKEN(TOK_RELATION);
}

REL {
    RETURN_TOKEN(TOK_RELATION);
}

ARRAY {
    RETURN_TOKEN(TOK_ARRAY);
}

BUT {
    RETURN_TOKEN(TOK_BUT);
}

AS {
    RETURN_TOKEN(TOK_AS);
}

EXTEND {
    RETURN_TOKEN(TOK_EXTEND);
}

RENAME {
    RETURN_TOKEN(TOK_RENAME);
}

SUMMARIZE {
    RETURN_TOKEN(TOK_SUMMARIZE);
}

DIVIDEBY {
    RETURN_TOKEN(TOK_DIVIDEBY);
}

PER {
    RETURN_TOKEN(TOK_PER);
}

LIKE {
    RETURN_TOKEN(TOK_LIKE);
}

REGEX_LIKE {
    RETURN_TOKEN(TOK_REGEX_LIKE);
}

IN {
    RETURN_TOKEN(TOK_IN);
}

SUBSET_OF {
    RETURN_TOKEN(TOK_SUBSET_OF);
}

OR {
    RETURN_TOKEN(TOK_OR);
}

AND {
    RETURN_TOKEN(TOK_AND);
}

XOR {
    RETURN_TOKEN(TOK_XOR);
}

NOT {
    RETURN_TOKEN(TOK_NOT);
}

IF {
    RETURN_TOKEN(TOK_IF);
}

THEN {
    RETURN_TOKEN(TOK_THEN);
}

ELSE {
    RETURN_TOKEN(TOK_ELSE);
}

END {
    RETURN_TOKEN(TOK_END);
}

CASE {
    RETURN_TOKEN(TOK_CASE);
}

WHEN {
    RETURN_TOKEN(TOK_WHEN);
}

FOR {
    RETURN_TOKEN(TOK_FOR);
}

DO {
    RETURN_TOKEN(TOK_FOR);
}

TO {
    RETURN_TOKEN(TOK_TO);
}

WHILE {
    RETURN_TOKEN(TOK_WHILE);
}

LEAVE {
    RETURN_TOKEN(TOK_LEAVE);
}

TRUE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_TRUE, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_LIT_BOOLEAN;
}

FALSE {
    yylval = RDB_new_parse_expr(RDB_bool_to_expr(RDB_FALSE, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_LIT_BOOLEAN;
}

COUNT {
    RETURN_TOKEN(TOK_COUNT);
}

SUM {
    RETURN_TOKEN(TOK_SUM);
}

AVG {
    RETURN_TOKEN(TOK_AVG);
}

MAX {
    RETURN_TOKEN(TOK_MAX);
}

MIN {
    RETURN_TOKEN(TOK_MIN);
}

ALL {
    RETURN_TOKEN(TOK_ALL);
}

ANY {
    RETURN_TOKEN(TOK_ANY);
}

SAME_TYPE_AS {
    RETURN_TOKEN(TOK_SAME_TYPE_AS);
}

SAME_HEADING_AS {
    RETURN_TOKEN(TOK_SAME_HEADING_AS);
}

TABLE_DEE {
    RETURN_TOKEN(TOK_TABLE_DEE);
}

DEE {
    RETURN_TOKEN(TOK_TABLE_DEE);
}

TABLE_DUM {
    RETURN_TOKEN(TOK_TABLE_DUM);
}

DUM {
    RETURN_TOKEN(TOK_TABLE_DUM);
}

VAR {
    RETURN_TOKEN(TOK_VAR);
}

DROP {
    RETURN_TOKEN(TOK_DROP);
}

CONST {
    RETURN_TOKEN(TOK_CONST);
}

REAL {
    RETURN_TOKEN(TOK_REAL);
}

PRIVATE {
    RETURN_TOKEN(TOK_PRIVATE);
}

PUBLIC {
    RETURN_TOKEN(TOK_PUBLIC);
}

BASE {
    RETURN_TOKEN(TOK_REAL);
}

VIRTUAL {
    RETURN_TOKEN(TOK_VIRTUAL);
}

KEY {
    RETURN_TOKEN(TOK_KEY);
}

DEFAULT {
    RETURN_TOKEN(TOK_DEFAULT);
}

INIT {
    RETURN_TOKEN(TOK_INIT);
}

BEGIN {
    RETURN_TOKEN(TOK_BEGIN);
}

TRANSACTION {
    RETURN_TOKEN(TOK_TX);
}

TX {
    RETURN_TOKEN(TOK_TX);
}

COMMIT {
    RETURN_TOKEN(TOK_COMMIT);
}

ROLLBACK {
    RETURN_TOKEN(TOK_ROLLBACK);
}

INSERT {
    RETURN_TOKEN(TOK_INSERT);
}

D_INSERT {
    RETURN_TOKEN(TOK_D_INSERT);
}

DELETE {
    RETURN_TOKEN(TOK_DELETE);
}

I_DELETE {
    RETURN_TOKEN(TOK_I_DELETE);
}

UPDATE {
    RETURN_TOKEN(TOK_UPDATE);
}

CALL {
    RETURN_TOKEN(TOK_CALL);
}

TYPE {
    RETURN_TOKEN(TOK_TYPE);
}

IS {
    RETURN_TOKEN(TOK_IS);
}

POSSREP {
    RETURN_TOKEN(TOK_POSSREP);
}

CONSTRAINT {
    RETURN_TOKEN(TOK_CONSTRAINT);
}

OPERATOR {
    RETURN_TOKEN(TOK_OPERATOR);
}

EXTERN {
    RETURN_TOKEN(TOK_EXTERN);
}

RETURNS {
    RETURN_TOKEN(TOK_RETURNS);
}

UPDATES {
    RETURN_TOKEN(TOK_UPDATES);
}

RETURN {
    RETURN_TOKEN(TOK_RETURN);
}

LOAD {
    RETURN_TOKEN(TOK_LOAD);
}

ORDER {
    RETURN_TOKEN(TOK_ORDER);
}

ASC {
    RETURN_TOKEN(TOK_ASC);
}

DESC {
    RETURN_TOKEN(TOK_DESC);
}

WITH {
    RETURN_TOKEN(TOK_WITH);
}

TRY {
    RETURN_TOKEN(TOK_TRY);
}

CATCH {
    RETURN_TOKEN(TOK_CATCH);
}

RAISE {
    RETURN_TOKEN(TOK_RAISE);
}

IMPLEMENT {
    RETURN_TOKEN(TOK_IMPLEMENT);
}

INDEX {
    RETURN_TOKEN(TOK_INDEX);
}

ORDERED {
    RETURN_TOKEN(TOK_ORDERED);
}

EXPLAIN {
    RETURN_TOKEN(TOK_EXPLAIN);
} 

MAP {
    RETURN_TOKEN(TOK_MAP);
} 

PACKAGE {
    RETURN_TOKEN(TOK_PACKAGE);
}

\|\| {
    RETURN_TOKEN(TOK_CONCAT);
}

[a-zA-Z][a-zA-Z0-9_#]*\'? {
    int i;

    if (RDB_parse_case_insensitive) {
        for (i = 0; yytext[i] != '\0'; i++) {
            yytext[i] = tolower(yytext[i]);
        }
    }

    yylval = RDB_new_parse_expr(RDB_var_ref(yytext, RDB_parse_ecp),
    		whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
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
    if (yylval == NULL)
        yyterminate();
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
    if (yylval == NULL)
        yyterminate();
    return TOK_LIT_STRING;
}

X\'([a-fA-F0-9][a-fA-F0-9])*\' {
    int i;
    char buf[3];
    char *datap;
    size_t len = (strlen(yytext) - 3) / 2;
    RDB_expression *exp = RDB_obj_to_expr(NULL, RDB_parse_ecp);
    if (exp == NULL) {
        yylval = NULL;
        yyterminate();
    }

    if (RDB_irep_to_obj(RDB_expr_obj(exp), &RDB_BINARY, NULL,
            len, RDB_parse_ecp) != RDB_OK) {
        yylval = NULL;
        yyterminate(); 
    }

    datap = RDB_obj_irep(RDB_expr_obj(exp), NULL);
    buf[2] = '\0';
    for (i = 0; i < len; i++) {
        buf[0] = yytext[2 + i * 2];
        buf[1] = yytext[3 + i * 2];
        datap[i] = (char) strtol(buf, NULL, 16);
    }
    yylval = RDB_new_parse_expr(exp, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_LIT_BINARY;    
}

\' {
    char buf[40];

    if (RDB_parse_interactive)
	    sprintf(buf, "unbalanced '");
	else
	    sprintf(buf, "unbalanced ' at line %d", yylineno);
    RDB_raise_syntax(buf, RDB_parse_ecp);
    yylval = NULL;
    return TOK_INVALID;
}

\" {
    char buf[40];

    if (RDB_parse_interactive)
	    sprintf(buf, "unbalanced \"");
	else 
	    sprintf(buf, "unbalanced \" at line %d", yylineno);
    RDB_raise_syntax(buf, RDB_parse_ecp);
    yylval = NULL;
    return TOK_INVALID;
}

[0-9]+ {
    long lv = strtol(yytext, NULL, 10);
    if (lv > RDB_INT_MAX || (errno == ERANGE && lv == LONG_MAX)) {
	    RDB_raise_type_constraint_violation("integer number too large",
	            RDB_parse_ecp); 
        yylval = NULL;
        return TOK_INVALID;
	}
    yylval = RDB_new_parse_expr(RDB_int_to_expr((RDB_int) lv,
    		RDB_parse_ecp), whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_LIT_INTEGER;
}

[0-9]+"."[0-9]+[Ee][0-9]+ {
    double v = strtod(yytext, NULL);
    if (!isfinite(v)) {
	    RDB_raise_type_constraint_violation("floating point number too large",
	            RDB_parse_ecp); 
        yylval = NULL;
        return TOK_INVALID;
	}
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) v,
    		RDB_parse_ecp), whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_LIT_FLOAT;
}

[0-9]+"."[0-9]+	{
    double v = strtod(yytext, NULL);
    if (!isfinite(v)) {
	    RDB_raise_type_constraint_violation("floating point number too large",
	            RDB_parse_ecp); 
        yylval = NULL;
        return TOK_INVALID;
	}
    yylval = RDB_new_parse_expr(RDB_float_to_expr((RDB_float) v,
    		RDB_parse_ecp), whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_LIT_FLOAT;
}

\<> {
    yylval = RDB_new_parse_token(TOK_NE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_NE;
}

!=  {
    yylval = RDB_new_parse_token(TOK_NE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_NE;
}

\<= {
    yylval = RDB_new_parse_token(TOK_LE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_LE;
}

\>=   {
    yylval = RDB_new_parse_token(TOK_GE, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return TOK_GE;
}

:= {
    yylval = RDB_new_parse_token(TOK_ASSIGN, whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
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
            if (RDB_parse_interactive) {
                yy_delete_buffer(YY_CURRENT_BUFFER);
                RDB_parse_buffer = yy_scan_string("");
                RDB_parse_buffer_valid = 1;
                sprintf(buf, "unterminated comment");
            } else {
                sprintf(buf, "unterminated comment at line %d", yylineno);
            }
            RDB_raise_syntax(buf, RDB_parse_ecp);
            yylval = NULL;
            return TOK_INVALID;
        }
    }
    comment_buf[buf_idx] = '\0';
    RDB_append_string(whitecommp, comment_buf, RDB_parse_ecp);
}

[=><+\-*/{},().;\[\]:%] {
    yylval = RDB_new_parse_token(yytext[0], whitecommp, RDB_parse_ecp);
    whitecommp = NULL;
    if (yylval == NULL)
        yyterminate();
    return yytext[0];
}

. {
    char buf[40];

    if (RDB_parse_interactive)
    	sprintf(buf, "invalid character %c", yytext[0]);
    else
	    sprintf(buf, "invalid character %c at line %d", yytext[0], yylineno);
    RDB_raise_syntax(buf, RDB_parse_ecp);
    yylval = NULL;
    return TOK_INVALID;
}

<<EOF>> {
    /*
     * If in interactive mode, read next line from input
     */
    char *line;

    if (!RDB_parse_interactive)
        yyterminate();

    yy_delete_buffer(YY_CURRENT_BUFFER);

    if (whitecommp == NULL) {
        whitecommp = RDB_alloc(sizeof (RDB_object), RDB_parse_ecp);
        if (whitecommp == NULL)
            yyterminate();
        RDB_init_obj(whitecommp);
        if (RDB_string_to_obj(whitecommp, "\n", RDB_parse_ecp) != RDB_OK)
            yyterminate();
    } else {
        if (RDB_append_char(whitecommp, '\n', RDB_parse_ecp) != RDB_OK)
            yyterminate();
    }

    line = (*RDB_parse_read_line_fnp) ();
    if (line == NULL) {
        /* Provide valid input in case yylex() is called again */
        RDB_parse_buffer = yy_scan_string("");
        RDB_parse_buffer_valid = 1;

        yyterminate();
    }

    RDB_parse_buffer = yy_scan_string(line);
    RDB_parse_buffer_valid = 1; 
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
    RDB_parse_buffer_valid = 1;
}

void
RDB_parse_flush_buf(void)
{
    yy_flush_buffer(YY_CURRENT_BUFFER);
}

/*
 * Free the flex buffer
 */
void
RDB_parse_destroy_buf(void)
{
    yylex_destroy();
    RDB_parse_buffer_valid = 0;
}
