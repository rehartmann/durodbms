/*
 * The dreisam front controller. Processes FastCGI requests
 * and dispatches them to the action and view operators.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include <fcgiapp.h>

#include <dli/iinterp.h>
#include <rel/rdb.h>

#include "getaction.h"
#include "viewop.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

/* Maximum POST request size is 1MB */
#define POST_SIZE_MAX  ((size_t) (1024 * 1024))

static FCGX_Stream *fcgi_out;
static FCGX_Stream *fcgi_err;
static RDB_bool headers_sent;

static void
log_err(Duro_interp *interpp, RDB_exec_context *ecp, FCGX_Stream *err)
{
    RDB_object msgobj;
    RDB_object *errp = RDB_get_err(ecp);

    RDB_init_obj(&msgobj);

    FCGX_PutS(RDB_type_name(RDB_obj_type(errp)), err);

    if (RDB_obj_property(errp, "msg", &msgobj, NULL, ecp, NULL) == RDB_OK) {
        FCGX_FPrintF(err, ": %s", RDB_obj_string(&msgobj));
    }
    if (interpp->err_opname != NULL) {
        FCGX_FPrintF(err, " in %s", interpp->err_opname);
    }
    if (interpp->err_line != -1) {
        FCGX_FPrintF(err, " at line %d", interpp->err_line);
    }
    FCGX_PutS("\n", err);

    RDB_destroy_obj(&msgobj, ecp);
}

static int
send_headers(void)
{
    int ret = 0;
    if (!headers_sent) {
        ret = FCGX_PutS("Status: 200\r\n"
                  "Content-Type: text/html\r\n"
                  "\r\n",
                  fcgi_out);
        headers_sent = RDB_TRUE;
    }
    return ret;
}

static int
op_net_put_line(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    send_headers();
    if (FCGX_PutS(RDB_obj_string(argv[0]), fcgi_out) == -1)
        goto error;
    if (FCGX_PutS("\r\n", fcgi_out) == -1)
        goto error;

    return RDB_OK;

error:
    RDB_errcode_to_error(errno, ecp);
    return RDB_ERROR;
}

static int
op_net_put(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    send_headers();
    if (FCGX_PutS(RDB_obj_string(argv[0]), fcgi_out) == -1) {
        RDB_errcode_to_error(errno, ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
op_net_put_err_line(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    send_headers();
    if (FCGX_PutS(RDB_obj_string(argv[0]), fcgi_err) == -1)
        goto error;
    if (FCGX_PutS("\r\n", fcgi_err) == -1)
        goto error;

    return RDB_OK;

error:
    RDB_errcode_to_error(errno, ecp);
    return RDB_ERROR;
}

static int
op_net_put_err(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    send_headers();
    if (FCGX_PutS(RDB_obj_string(argv[0]), fcgi_err) == -1) {
        RDB_errcode_to_error(errno, ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
send_error_response(int status, const char *msg, FCGX_Stream *out)
{
    int cnt;
    if (headers_sent)
        return 0;
    cnt = FCGX_FPrintF(out,
              "Status: %d\r\n"
              "Content-type: text/html\r\n"
              "\r\n"
              "<html>\n\r"
              "<head>\n\r"
              "<title>%s</title>\r\n"
              "<p>%s\r\n"
              "<html>\n\r",
              status, msg, msg);
    if (cnt > 0)
        headers_sent = RDB_TRUE;
    return cnt;
}

/*
 * Reads POST request data and converts it to model data.
 *
 * If RDB_ERROR has been returned and *client_error is TRUE,
 * a response has been sent and no error value has been stored in *ecp.
 */
static int
post_req_to_model(RDB_object *modelp, RDB_exec_context *ecp,
        FCGX_ParamArray envp, FCGX_Stream *in, FCGX_Stream *out,
        FCGX_Stream *err, RDB_bool *client_error)
{
    int ret;
    size_t len;
    char *content;
    char *content_length = FCGX_GetParam("CONTENT_LENGTH", envp);
    if (content_length == NULL) {
        send_error_response(411, "Content length required", out);
        *client_error = RDB_TRUE;
        return RDB_ERROR;
    }
    len = (size_t) atol(content_length);

    /* Check size limit */
    if (len > POST_SIZE_MAX) {
        send_error_response(413, "Content length limit exceeded", out);
        *client_error = RDB_TRUE;
        return RDB_ERROR;
    }

    /* Read content */
    content = RDB_alloc(len + 1, ecp);
    if (content == NULL) {
        send_error_response(507, "Insufficient memory", out);
        *client_error = RDB_FALSE;
        return RDB_ERROR;
    }
    if (FCGX_GetStr(content, len, in) != len) {
        send_error_response(400, "Request length does not match content length", out);
        RDB_free(content);
        *client_error = RDB_TRUE;
        return RDB_ERROR;
    }
    content[len] = '\0';

    /* Convert content to tuple */
    ret = RDB_net_form_to_tuple(modelp, content, ecp);
    RDB_free(content);
    *client_error = RDB_FALSE;
    return ret;
}

static int
process_request(Duro_interp *interpp, RDB_exec_context *ecp,
        FCGX_ParamArray envp, FCGX_Stream *in, FCGX_Stream *out, FCGX_Stream *err)
{
    int ret;
    RDB_object viewopname;
    RDB_object model;
    RDB_object *argv[2];
    RDB_operator *controller_op;
    const char *path_info;
    const char *request_method;
    RDB_operator *view_op;

    RDB_init_obj(&viewopname);
    RDB_init_obj(&model);

    path_info = FCGX_GetParam("PATH_INFO", envp);
    if (path_info == NULL)
        path_info = "";

    /*
     * The initial view name is taken from path_info, with the leading
     * slash removed, and preceded by 't.' so it will match the view operator
     * generated from a template.
     */
    RDB_string_to_obj(&viewopname, "t.", ecp);
    if (path_info[0] == '/') {
        if (RDB_append_string(&viewopname, path_info + 1, ecp) != RDB_OK)
            goto error;
    } else {
        if (RDB_append_string(&viewopname, path_info, ecp) != RDB_OK)
            goto error;
    }

    controller_op = Dr_get_action_op(path_info, interpp, ecp);
    if (controller_op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            FCGX_FPrintF(out,
                    "Status: 404\r\n"
                    "Content-type: text/html\r\n"
                    "\r\n"
                    "<html>\n\r"
                    "<head>\n\r"
                    "<title>No action found</title>\r\n"
                    "<body>\n\r"
                    "<p>No action found for path %s\r\n"
                    "</html>\n\r",
                    path_info);
            return RDB_OK;
        }
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            log_err(interpp, ecp, err);
            FCGX_FPrintF(out,
                    "Status: 404\r\n"
                    "Content-type: text/html\r\n"
                    "\r\n"
                    "<html>\n\r"
                    "<head>\n\r"
                    "<title>No action operator found</title>\r\n"
                    "<body>\n\r"
                    "<p>No action operator found for path %s\r\n"
                    "<html>\n\r",
                    path_info);
            return RDB_OK;
        }
        goto error;
    }

    if (RDB_set_init_value(&model, RDB_get_parameter(controller_op, 0)->typ,
            interpp->envp, ecp) != RDB_OK) {
        FCGX_PutS("Initializing the model failed\n", err);
        goto error;
    }

    request_method = FCGX_GetParam("REQUEST_METHOD", envp);
    if (request_method == NULL)
        request_method = "";

    if (strcmp(request_method, "GET") == 0) {
        char *query_string = FCGX_GetParam("QUERY_STRING", envp);
        if (RDB_net_form_to_tuple(&model, query_string, ecp) != RDB_OK)
            goto error;
    } else if (strcmp(request_method, "POST") == 0) {
        RDB_bool client_error;
        if (post_req_to_model(&model, ecp, envp, in, out, err, &client_error)
                != RDB_OK) {
            if (client_error)
                return RDB_OK;
            goto error;
        }
    } else {
        FCGX_FPrintF(out,
                  "Status: 405\r\n"
                  "Content-type: text/html\r\n"
                  "Allow: GET, POST\r\n"
                  "\r\n"
                  "<html>\n\r"
                  "<head>\n\r"
                  "<title>Request method not supported</title>\r\n"
                  "<body>\n\r"
                  "<p>Request method not supported\r\n"
                  "<html>\n\r",
                  path_info);
        return RDB_OK;
    }

    fcgi_out = out;
    fcgi_err = err;
    headers_sent = RDB_FALSE;

    argv[0] = &model;
    argv[1] = &viewopname;
    if (RDB_call_update_op(controller_op, 2, argv, ecp, NULL)
            != RDB_OK) {
        FCGX_PutS("Invoking controller operator failed\n", err);
        goto error;
    }

    view_op = Dr_provide_view_op(&viewopname, interpp, ecp, err);
    if (view_op == NULL)
        goto error;

    argv[0] = &model;
    if (RDB_call_update_op(view_op, 1, argv, ecp, NULL) != RDB_OK) {
        FCGX_FPrintF(err, "Invoking view operator %s failed\n",
                RDB_obj_string(&viewopname));
        log_err(interpp, ecp, err);
        send_error_response(500, "processing the request failed", out);

        return RDB_OK;
    }

    RDB_destroy_obj(&viewopname, ecp);
    RDB_destroy_obj(&model, ecp);
    return RDB_OK;

error:
    ret = RDB_ERROR;
    /*
     * If there is a syntax error in the template,
     * or the template was not found
     * return RDB_OK so the process won't exit
     */
    if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_SYNTAX_ERROR
            || RDB_obj_type(RDB_get_err(ecp)) == &RDB_RESOURCE_NOT_FOUND_ERROR)
        ret = RDB_OK;

    log_err(interpp, ecp, err);
    FCGX_FFlush(err);

    if (!headers_sent) {
        FCGX_FPrintF(out,
                "Status: 500\r\n"
                "Content-type: text/html\r\n"
                "\r\n"
                "<html>\n\r"
                "<head>\n\r"
                "<title>Request processing failed</title>\r\n"
                "<body>\n\r"
                "<p>An error occurred while processing the request, path=%s\r\n"
                "<html>\n\r",
                path_info);
    }
    FCGX_FFlush(out);

    RDB_destroy_obj(&viewopname, ecp);
    RDB_destroy_obj(&model, ecp);
    return ret;
}

int main(void)
{
    int ret;
    FCGX_Stream *in, *out, *err;
    FCGX_ParamArray envp;
    RDB_exec_context ec;
    Duro_interp interp;
    RDB_parameter param;

    RDB_init_exec_context(&ec);

    if (RDB_init_builtin(&ec) != RDB_OK) {
        fputs("Unable to initialize built-ins\n", stderr);
        goto error;
    }

    if (Duro_init_interp(&interp, &ec, NULL, "") != RDB_OK) {
        fputs("Unable to initialize interpreter\n", stderr);
        goto error;
    }

    param.typ = &RDB_STRING;
    param.update = RDB_FALSE;
    if (RDB_put_upd_op(&interp.sys_upd_op_map, "net.put_line", 1, &param,
            &op_net_put_line, &ec) != RDB_OK)
       goto error;
    if (RDB_put_upd_op(&interp.sys_upd_op_map, "net.put_err_line", 1, &param,
            &op_net_put_err_line, &ec) != RDB_OK)
       goto error;

    if (RDB_put_upd_op(&interp.sys_upd_op_map, "net.put", 1, &param,
            &op_net_put, &ec) != RDB_OK)
       goto error;
    if (RDB_put_upd_op(&interp.sys_upd_op_map, "net.put_err", 1, &param,
            &op_net_put_err, &ec) != RDB_OK)
       goto error;

    if (Duro_dt_execute_str("var path_info string; var dbenv string;", &interp, &ec) != RDB_OK) {
        fputs("Declaration of path_info failed\n", stderr);
        goto error;
    }

    if (Duro_dt_execute_path("dreisam/config.td", &interp, &ec) != RDB_OK) {
        fputs("Execution of config file failed\n", stderr);
        goto error;
    }

    if (Duro_dt_execute_str("connect(dbenv);", &interp, &ec) != RDB_OK) {
        fputs("Connecting to database environment failed\n", stderr);
        goto error;
    }

    while ((ret = FCGX_Accept(&in, &out, &err, &envp)) >= 0) {
        if (process_request(&interp, &ec, envp, in, out, err) != RDB_OK) {
            FCGX_Finish();
            goto error;
        }
    }

    Duro_destroy_interp(&interp);

    RDB_destroy_exec_context(&ec);

	return EXIT_SUCCESS;

error:
    Duro_destroy_interp(&interp);

    RDB_destroy_exec_context(&ec);

    return EXIT_FAILURE;
}
