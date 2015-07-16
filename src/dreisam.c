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
#include "sreason.h"
#include "json.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

/* Maximum POST request size is 1MB */
#define POST_SIZE_MAX ((size_t) (1024 * 1024))

/* Return codes */
enum {
    DR_ERR_INIT_BUILTINS = 25,
    DR_ERR_INIT_INTERP = 26,
    DR_ERR_INIT_OP = 27,
    DR_ERR_DECL = 28,
    DR_ERR_CONFIG = 29,
    DR_ERR_CONNECT = 30,
    DR_ERR_REQUEST = 31,
    DR_ERR_GET_OP = 32
};

/* Header output states */
enum {
    DR_HEADERS_NOT_SENT = 0,
    DR_HEADERS_SENDING = 1,
    DR_HEADERS_SENT = 2
};

static FCGX_Stream *fcgi_out;
static FCGX_Stream *fcgi_err;
static FCGX_ParamArray fcgi_envp;

static RDB_operator *send_headers_op;
static RDB_object *headers_state;
static RDB_object *request;
static RDB_object *response;
static const char *request_method;

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
send_headers(RDB_exec_context *ecp)
{
    if (RDB_obj_int(headers_state) == 0) {
        RDB_object *send_headers_argv[0];

        send_headers_argv[0] = response;
        if (RDB_call_update_op(send_headers_op, 1, send_headers_argv, ecp, NULL)
                != RDB_OK) {
            FCGX_PutS("Sending headers failed\n", fcgi_err);
            return RDB_ERROR;
        }
    }
    return RDB_OK;
}

static int
op_http_put_line(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    if (send_headers(ecp) != RDB_OK)
        return RDB_ERROR;

    /* Don't send response body if the request method is HEAD */
    if (strcmp(request_method, "HEAD") == 0
            && RDB_obj_int(headers_state) == DR_HEADERS_SENT) {
        return RDB_OK;
    }

    if (FCGX_PutS(RDB_obj_string(argv[1]), fcgi_out) == -1)
        goto error;
    if (FCGX_PutS("\n", fcgi_out) == -1)
        goto error;

    return RDB_OK;

error:
    RDB_errcode_to_error(errno, ecp);
    return RDB_ERROR;
}

static int
op_http_put(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    send_headers(ecp);

    if (strcmp(request_method, "HEAD") == 0
            && RDB_obj_int(headers_state) == DR_HEADERS_SENT) {
        return RDB_OK;
    }

    if (FCGX_PutS(RDB_obj_string(argv[1]), fcgi_out) == -1) {
        RDB_errcode_to_error(errno, ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
op_http_put_err_line(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    send_headers(ecp);

    if (FCGX_PutS(RDB_obj_string(argv[1]), fcgi_err) == -1)
        goto error;
    if (FCGX_PutS("\n", fcgi_err) == -1)
        goto error;

    return RDB_OK;

error:
    RDB_errcode_to_error(errno, ecp);
    return RDB_ERROR;
}

static int
op_http_put_err(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    send_headers(ecp);

    if (FCGX_PutS(RDB_obj_string(argv[1]), fcgi_err) == -1) {
        RDB_errcode_to_error(errno, ecp);
        return RDB_ERROR;
    }

    return RDB_OK;
}

static int
op_net_to_json(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return Dr_obj_to_json(retvalp, argv[0], ecp);
}

static int
op_http_status_reason(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    return RDB_string_to_obj(retvalp, Dr_sc_reason(RDB_obj_int(argv[0])), ecp);
}

static int
op_http_get_request_header(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp, RDB_object *retvalp)
{
    const char *value = FCGX_GetParam(RDB_obj_string(argv[1]), fcgi_envp);
    if (value == NULL) {
        RDB_raise_not_found(RDB_obj_string(argv[1]), ecp);
        return RDB_ERROR;
    }
    return RDB_string_to_obj(retvalp, value, ecp);
}

/*
 * Send error response if it has not already been sent.
 * *ecp is only used to look up the interpreter and is not modified even if RDB_ERROR is returned.
 */
static int
send_error_response(int status, const char *msg, FCGX_Stream *out, RDB_exec_context *ecp)
{
    int cnt;
    Duro_interp *interpp = RDB_ec_property(ecp, "INTERP");

    if (interpp == NULL)
        return RDB_ERROR;

    if (RDB_obj_int(headers_state) != DR_HEADERS_NOT_SENT)
        return 0;
    cnt = FCGX_FPrintF(out,
              "Status: %d %s\n"
              "Content-Type: text/html; charset=utf-8\n"
              "\n"
              "<html>\n"
              "<head>\n"
              "<title>%s</title>\n"
              "<p>%s\n"
              "<html>\n",
              status, Dr_sc_reason(status), msg, msg);
    if (cnt > 0)
        RDB_int_to_obj(headers_state, DR_HEADERS_SENT);
    return cnt;
}

/*
 * Reads POST request data and converts it to model data.
 *
 * If RDB_ERROR has been returned and *client_error is RDB_TRUE,
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
        send_error_response(411, "Content length required", out, ecp);
        *client_error = RDB_TRUE;
        return RDB_ERROR;
    }
    len = (size_t) atol(content_length);

    /* Check size limit */
    if (len > POST_SIZE_MAX) {
        send_error_response(413, "Content length limit exceeded", out, ecp);
        *client_error = RDB_TRUE;
        return RDB_ERROR;
    }

    /* Read content */
    content = RDB_alloc(len + 1, ecp);
    if (content == NULL) {
        send_error_response(507, "Insufficient memory", out, ecp);
        *client_error = RDB_FALSE;
        return RDB_ERROR;
    }
    if (FCGX_GetStr(content, len, in) != len) {
        send_error_response(400, "Request length does not match content length",
                out, ecp);
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
        FCGX_ParamArray envp, FCGX_Stream *in, FCGX_Stream *out,
        FCGX_Stream *err)
{
    static const char *ERROR_REQUEST = "An error occurred while processing the request";

    int ret;
    int i;
    RDB_object viewname;
    RDB_object model;
    int argc;
    RDB_object *argv[3];
    RDB_operator *action_op;
    const char *path_info;
    RDB_operator *view_op;
    RDB_object method;

    RDB_init_obj(&viewname);
    RDB_init_obj(&model);
    RDB_init_obj(&method);

    fcgi_out = out;
    fcgi_err = err;
    fcgi_envp = envp;

    if (Duro_dt_execute_str("dreisam_resp := http.http_response("
                    "'200 OK',"
                    "array (tup { name 'Content-Type', value 'text/html; charset=utf-8' } ));"
            "dreisam_response_headers_state := 0;",
            interpp, ecp) != RDB_OK) {
        goto error;
    }

    path_info = FCGX_GetParam("PATH_INFO", envp);
    if (path_info == NULL)
        path_info = "";

    request_method = FCGX_GetParam("REQUEST_METHOD", envp);
    if (request_method == NULL)
        request_method = "";

    FCGX_FPrintF(err, "path_info: %s, method: %s\n", request_method, path_info);

    if (RDB_string_to_obj(&method, request_method, ecp) != RDB_OK)
        goto error;

    if (RDB_obj_set_property(request, "method", &method, interpp->envp,
            ecp, NULL) != RDB_OK) {
        goto error;
    }

    action_op = Dr_get_action_op(interpp, RDB_obj_type(response), ecp);
    if (action_op == NULL) {
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_NOT_FOUND_ERROR) {
            FCGX_FPrintF(out,
                    "Status: 404 Not found\n"
                    "Content-Type: text/html; charset=utf-8\n"
                    "\n"
                    "<html>\n"
                    "<head>\n"
                    "<title>No action found</title>\n"
                    "<body>\n"
                    "<p>No action found for path %s\n"
                    "</html>\n",
                    path_info);
            return RDB_OK;
        }
        if (RDB_obj_type(RDB_get_err(ecp)) == &RDB_OPERATOR_NOT_FOUND_ERROR) {
            log_err(interpp, ecp, err);
            FCGX_FPrintF(out,
                    "Status: 404 Not found\n"
                    "Content-Type: text/html; charset=utf-8\n"
                    "\n"
                    "<html>\n"
                    "<head>\n"
                    "<title>No action operator found</title>\n"
                    "<body>\n"
                    "<p>No action operator found for path %s\n"
                    "<html>\n",
                    path_info);
            return RDB_OK;
        }
        goto error;
    }

    /* Get view from path info without leading slash */
    if (path_info[0] == '/') {
        if (RDB_string_to_obj(&viewname, path_info + 1, ecp) != RDB_OK)
            goto error;
    } else {
        if (RDB_string_to_obj(&viewname, path_info, ecp) != RDB_OK)
            goto error;
    }

    if (RDB_set_init_value(&model, RDB_get_parameter(action_op, 0)->typ,
            interpp->envp, ecp) != RDB_OK) {
        FCGX_PutS("Initializing the model failed\n", err);
        goto error;
    }

    if (strcmp(request_method, "GET") == 0
            || strcmp(request_method, "HEAD") == 0
            || strcmp(request_method, "DELETE") == 0) {
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
                  "Status: 405 Method not allowed\n"
                  "Content-Type: text/html; charset=utf-8\n"
                  "Allow: GET, HEAD, POST, DELETE\n"
                  "\n"
                  "<html>\n"
                  "<head>\n"
                  "<title>Request method not supported</title>\n"
                  "<body>\n"
                  "<p>Request method not supported\n"
                  "<html>\n",
                  path_info);
        return RDB_OK;
    }

    argc = RDB_operator_param_count(action_op);
    argv[0] = &model;
    argv[1] = &viewname;
    for (i = 2; i < argc; i++) {
        RDB_type *paramtyp = RDB_get_parameter(action_op, i)->typ;
        if (paramtyp == RDB_obj_type(request)) {
            argv[i] = request;
        } else if (paramtyp == RDB_obj_type(response)) {
            argv[i] = response;
        } else {
            FCGX_FPrintF(err, "Action operator %s has invalid signature\n", RDB_operator_name(action_op));
            FCGX_FPrintF(out,
                    "Status: 404 Not found\n"
                    "Content-Type: text/html; charset=utf-8\n"
                    "\n"
                    "<html>\n"
                    "<head>\n"
                    "<title>No valid action operator found</title>\n"
                    "<body>\n"
                    "<p>No valid action operator found for path %s\n"
                    "<html>\n",
                    path_info);
            return RDB_OK;
        }
    }
    if (RDB_call_update_op(action_op, argc, argv, ecp, NULL)
            != RDB_OK) {
        FCGX_PutS("Invoking action operator failed\n", err);
        log_err(interpp, ecp, err);

        if (Duro_dt_tx(interpp) != NULL) {
            FCGX_PutS("Found transaction after action invocation, rolling back\n", err);
            Duro_rollback_all(interpp, ecp);
        }

        send_error_response(500, ERROR_REQUEST, out, ecp);
        RDB_destroy_obj(&viewname, ecp);
        RDB_destroy_obj(&model, ecp);
        return RDB_OK;
    }

    if (Duro_dt_tx(interpp) != NULL) {
        FCGX_PutS("Found transaction after action invocation, rolling back\n", err);
        Duro_rollback_all(interpp, ecp);
    }

    if (*RDB_obj_string(&viewname) != '\0') {
        view_op = Dr_provide_view_op(RDB_obj_string(&viewname),
                interpp, RDB_obj_type(response), ecp, err);
        if (view_op == NULL) {
            RDB_type *errtyp = RDB_obj_type(RDB_get_err(ecp));
            if (errtyp != NULL) {
                /*
                 * Special treatment of syntax_error and net.template_error
                 */
                const char *typename;

                if (errtyp == &RDB_SYNTAX_ERROR) {
                    log_err(interpp, ecp, err);

                    send_error_response(500, ERROR_REQUEST, out, ecp);

                    RDB_destroy_obj(&viewname, ecp);
                    RDB_destroy_obj(&model, ecp);
                    RDB_destroy_obj(&method, ecp);
                    return RDB_OK;
                }
                typename = RDB_type_name(errtyp);
                if (typename != NULL
                        && strcmp(typename, "net.template_error") == 0) {
                    interpp->err_line = -1; /* Line number is meaningless here */
                    log_err(interpp, ecp, err);

                    send_error_response(500, ERROR_REQUEST, out, ecp);

                    RDB_destroy_obj(&viewname, ecp);
                    RDB_destroy_obj(&model, ecp);
                    RDB_destroy_obj(&method, ecp);
                    return RDB_OK;
                }
                goto error;
            }
        }

        argv[0] = response;
        argv[1] = &model;
        if (RDB_call_update_op(view_op, 2, argv, ecp, NULL) != RDB_OK) {
            FCGX_FPrintF(err, "Invoking view operator %s failed\n",
                    RDB_obj_string(&viewname));
            log_err(interpp, ecp, err);
            send_error_response(500, ERROR_REQUEST, out, ecp);
        }
    } else {
        /* No view - send headers if they haven't already been sent */
        send_headers(ecp);
    }

    RDB_destroy_obj(&viewname, ecp);
    RDB_destroy_obj(&model, ecp);
    RDB_destroy_obj(&method, ecp);
    return RDB_OK;

error:
    ret = RDB_ERROR;

    log_err(interpp, ecp, err);

    send_error_response(500, ERROR_REQUEST, out, ecp);

    RDB_destroy_obj(&viewname, ecp);
    RDB_destroy_obj(&model, ecp);
    RDB_destroy_obj(&method, ecp);
    return ret;
}

static int
create_fcgi_ops(Duro_interp *interpp, RDB_type *reqtyp, RDB_type *resptyp, RDB_exec_context *ecp)
{
    int ret;
    RDB_parameter paramv[2];
    RDB_type *paramtypv[2];

    paramv[0].typ = resptyp;
    paramv[0].update = RDB_TRUE;
    paramv[1].typ = &RDB_STRING;
    paramv[1].update = RDB_FALSE;
    if (RDB_put_upd_op(&interpp->sys_upd_op_map, "http.put_line", 2, paramv,
            &op_http_put_line, ecp) != RDB_OK) {
        ret = DR_ERR_INIT_OP;
        goto error;
    }
    if (RDB_put_upd_op(&interpp->sys_upd_op_map, "http.put_err_line", 2, paramv,
            &op_http_put_err_line, ecp) != RDB_OK) {
        ret = DR_ERR_INIT_OP;
        goto error;
    }

    if (RDB_put_upd_op(&interpp->sys_upd_op_map, "http.put", 2, paramv,
            &op_http_put, ecp) != RDB_OK) {
        ret = DR_ERR_INIT_OP;
        goto error;
    }
    if (RDB_put_upd_op(&interpp->sys_upd_op_map, "http.put_err", 2, paramv,
            &op_http_put_err, ecp) != RDB_OK) {
        ret = DR_ERR_INIT_OP;
        goto error;
    }

    paramtypv[0] = &RDB_INTEGER;
    if (RDB_put_global_ro_op("http.status_reason", 1, paramtypv,
            &RDB_STRING, op_http_status_reason, ecp) != RDB_OK) {
        goto error;
    }

    paramtypv[0] = reqtyp;
    paramtypv[1] = &RDB_STRING;
    if (RDB_put_global_ro_op("http.get_request_header", 2, paramtypv,
            &RDB_STRING, op_http_get_request_header, ecp) != RDB_OK) {
        goto error;
    }

    paramtypv[0] = NULL;
    if (RDB_put_global_ro_op("net.to_json", 1, paramtypv,
            &RDB_STRING, &op_net_to_json, ecp) != RDB_OK) {
        ret = DR_ERR_INIT_OP;
        goto error;
    }
    return RDB_OK;

error:
    return ret;
}

int
main(void)
{
    int ret;
    FCGX_Stream *in, *out, *err;
    FCGX_ParamArray envp;
    RDB_exec_context ec;
    Duro_interp interp;
    RDB_type *send_headers_argtv[0];

    RDB_init_exec_context(&ec);

    if (RDB_init_builtin(&ec) != RDB_OK) {
        ret = DR_ERR_INIT_BUILTINS;
        goto error;
    }

    if (Duro_init_interp(&interp, &ec, NULL, "") != RDB_OK) {
        ret = DR_ERR_INIT_INTERP;
        goto error;
    }

    if (Duro_dt_execute_str("var dbenv string;"
            "var dreisam_response_headers_state int;",
            &interp, &ec) != RDB_OK) {
        ret = DR_ERR_DECL;
        goto error;
    }

    headers_state = Duro_lookup_var("dreisam_response_headers_state", &interp, &ec);
    if (headers_state == NULL) {
        ret = DR_ERR_DECL;
        goto error;
    }

    if (Duro_dt_execute_path("dreisam/config.td", &interp, &ec) != RDB_OK) {
        ret = DR_ERR_CONFIG;
        goto error;
    }

    if (Duro_dt_execute_str("connect(dbenv);", &interp, &ec) != RDB_OK) {
        ret = DR_ERR_CONNECT;
        goto error;
    }

    if (Duro_dt_execute_str("begin tx;", &interp, &ec) != RDB_OK) {
        ret = DR_ERR_GET_OP;
        goto error;
    }

    if (Duro_dt_execute_str("var dreisam_req http.http_request;"
            "var dreisam_resp http.http_response;",
            &interp, &ec) != RDB_OK) {
        ret = DR_ERR_GET_OP;
        goto error;
    }

    request = Duro_lookup_var("dreisam_req", &interp, &ec);
    if (request == NULL) {
        ret = DR_ERR_GET_OP;
        goto error;
    }

    response = Duro_lookup_var("dreisam_resp", &interp, &ec);
    if (response == NULL) {
        ret = DR_ERR_GET_OP;
        goto error;
    }

    ret = create_fcgi_ops(&interp, RDB_obj_type(request), RDB_obj_type(response), &ec);
    if (ret != RDB_OK)
        goto error;

    send_headers_argtv[0] = RDB_obj_type(response);
    send_headers_op = RDB_get_update_op("http.send_headers",
            1, send_headers_argtv, NULL, &ec, Duro_dt_tx(&interp));
    if (send_headers_op == NULL) {
        ret = DR_ERR_GET_OP;
        goto error;
    }

    if (Duro_dt_execute_str("commit;", &interp, &ec) != RDB_OK) {
        ret = DR_ERR_GET_OP;
        goto error;
    }

    while (FCGX_Accept(&in, &out, &err, &envp) >= 0) {
        if (process_request(&interp, &ec, envp, in, out, err) != RDB_OK) {
            FCGX_Finish();
            ret = DR_ERR_REQUEST;
            goto error;
        }
    }

    Duro_destroy_interp(&interp);

    RDB_destroy_exec_context(&ec);

	return EXIT_SUCCESS;

error:
    Duro_destroy_interp(&interp);

    RDB_destroy_exec_context(&ec);

    return ret;
}
