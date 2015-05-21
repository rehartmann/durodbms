/*
 * Functions for converting a template to a view operator.
 *
 * Copyright (C) 2015 Rene Hartmann.
 * See the file COPYING for redistribution information.
 */

#include "viewop.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

static int
template_path(const char *viewnamep, RDB_object *tnameobjp,
        RDB_exec_context *ecp)
{
    if (RDB_string_to_obj(tnameobjp, "dreisam/views/", ecp) != RDB_OK)
        return RDB_ERROR;
    if (RDB_append_string(tnameobjp, viewnamep, ecp) != RDB_OK)
        return RDB_ERROR;
    return RDB_append_string(tnameobjp, ".thtml", ecp);
}

/*
 * Create view operator whose name is given by viewopname.
 * viewopname is passed without the module name.
 */
static int
create_view_op(const char *viewopname, Duro_interp *interpp,
        RDB_exec_context *ecp, FCGX_Stream *err)
{
    RDB_object *argv[3];
    RDB_object viewopnameobj;
    RDB_object tfilename;
    RDB_object opstrm;
    FILE *viewopfile;

    RDB_init_obj(&viewopnameobj);
    RDB_init_obj(&tfilename);
    RDB_init_obj(&opstrm);

    if (RDB_string_to_obj(&viewopnameobj, viewopname, ecp) != RDB_OK)
        goto error;
    if (RDB_call_ro_op_by_name("io.tmpfile", 0, NULL, ecp, Duro_dt_tx(interpp),
            &opstrm) != RDB_OK) {
        FCGX_PutS("Creating temporary file failed\n", err);
        goto error;
    }
    if (template_path(viewopname, &tfilename, ecp) != RDB_OK) {
        Duro_io_close(&opstrm, ecp);
        goto error;
    }

    argv[0] = &tfilename;
    argv[1] = &viewopnameobj;
    argv[2] = &opstrm;

    /*
     * Convert template to view operator
     */

    /* Call conversion operator */
    if (RDB_call_update_op_by_name("net.template_to_op", 3, argv,
               ecp, Duro_dt_tx(interpp)) != RDB_OK) {
        FCGX_FPrintF(err, "Processing template %s failed\n",
                RDB_obj_string(&tfilename));
        Duro_io_close(&opstrm, ecp);
        goto error;
    }

    viewopfile = Duro_io_iostream_file(&opstrm, ecp);
    if (viewopfile == NULL) {
        Duro_io_close(&opstrm, ecp);
        goto error;
    }
    rewind(viewopfile);

    /* Execute operator creation */
    if (Duro_dt_execute(viewopfile, interpp, ecp) != RDB_OK) {
        FCGX_FPrintF(err, "Error defining operator %s at line %d\n",
                viewopname, interpp->err_line);
        Duro_io_close(&opstrm, ecp);
        goto error;
    }

    if (Duro_io_close(&opstrm, ecp) != RDB_OK) {
        goto error;
    }

    RDB_destroy_obj(&viewopnameobj, ecp);
    RDB_destroy_obj(&tfilename, ecp);
    RDB_destroy_obj(&opstrm, ecp);

    return RDB_OK;

error:
    RDB_destroy_obj(&viewopnameobj, ecp);
    RDB_destroy_obj(&tfilename, ecp);
    RDB_destroy_obj(&opstrm, ecp);

    return RDB_ERROR;
}

/* Check if the template has been modified */
static int
template_is_newer(RDB_operator *view_op, Duro_interp *interpp,
        RDB_bool *resp, RDB_exec_context *ecp)
{
    struct stat statbuf;
    struct tm *modtm;
    RDB_object *argv[2];
    RDB_object tpath;
    RDB_object modtime;
    RDB_object cmpresult;

    RDB_init_obj(&tpath);
    RDB_init_obj(&modtime);
    RDB_init_obj(&cmpresult);

    /*
     * Get template modificaton time
     */

    if (template_path(RDB_operator_name(view_op) + MOD_PREFIX_LEN, &tpath, ecp) != RDB_OK)
        goto error;

    if (stat(RDB_obj_string(&tpath), &statbuf) == -1) {
        RDB_errcode_to_error(errno, ecp);
        goto error;
    }

    modtm = gmtime(&statbuf.st_mtime);
    if (modtm == NULL) {
        RDB_raise_system("gmtime() failed", ecp);
        goto error;
    }

    RDB_tm_to_obj(&modtime, modtm);

    /*
     * Check if template is newer than the view operator
     */

    argv[0] = &modtime;
    argv[1] = RDB_operator_creation_time(view_op);
    if (RDB_call_ro_op_by_name(">", 2, argv, ecp, Duro_dt_tx(interpp),
            &cmpresult) != RDB_OK) {
        goto error;
    }
    *resp = RDB_obj_bool(&cmpresult);

    RDB_destroy_obj(&tpath, ecp);
    RDB_destroy_obj(&modtime, ecp);
    RDB_destroy_obj(&cmpresult, ecp);

    return RDB_OK;

error:
    RDB_destroy_obj(&tpath, ecp);
    RDB_destroy_obj(&modtime, ecp);
    RDB_destroy_obj(&cmpresult, ecp);

    return RDB_ERROR;
}

/*
 * Creates the view operator given by *viewopname if necessary
 * The operator name must start with ".net"
 */
RDB_operator *
Dr_provide_view_op(RDB_object *viewopnamep, Duro_interp *interpp,
        RDB_exec_context *ecp, FCGX_Stream *err)
{
    RDB_type *viewargtyp = NULL;
    /* Try to get the view operator without a tx */
    RDB_operator *view_op = RDB_get_update_op(RDB_obj_string(viewopnamep), 1,
            &viewargtyp, interpp->envp, ecp, NULL);

    if (view_op == NULL) {
        /* Retry with tx */
        if (Duro_dt_execute_str("begin tx;", interpp, ecp) != RDB_OK)
            goto error;
        view_op = RDB_get_update_op(RDB_obj_string(viewopnamep), 1,
                    &viewargtyp, interpp->envp, ecp, &interpp->txnp->tx);
    }

    /* If the operator name does not start with "t.", don't (re-)create it */
    if (strncmp(RDB_obj_string(viewopnamep), "t.", MOD_PREFIX_LEN) != 0) {
        if (interpp->txnp != NULL) {
            if (Duro_dt_execute_str("commit;", interpp, ecp) != RDB_OK)
                return NULL;
        }
        return view_op;
    }

    if (view_op == NULL) {
        FCGX_FPrintF(err, "View operator not found, creating...\n");

        if (create_view_op(RDB_obj_string(viewopnamep) + MOD_PREFIX_LEN,
                interpp, ecp, err) != RDB_OK) {
            goto error;
        }
    } else {
        RDB_bool isnewer;

        if (template_is_newer(view_op, interpp, &isnewer, ecp) != RDB_OK)
            goto error;
        if (isnewer) {
            FCGX_PutS("template is newer, recreating view\n", err);

            if (Duro_dt_tx(interpp) == NULL) {
                if (Duro_dt_execute_str("begin tx;", interpp, ecp) != RDB_OK)
                    goto error;
            }

            if (RDB_drop_op(RDB_obj_string(viewopnamep), ecp, Duro_dt_tx(interpp)) != RDB_OK) {
                goto error;
            }

            if (create_view_op(RDB_obj_string(viewopnamep) + MOD_PREFIX_LEN,
                    interpp, ecp, err) != RDB_OK) {
                goto error;
            }
            view_op = RDB_get_update_op(RDB_obj_string(viewopnamep), 1,
                    &viewargtyp, interpp->envp, ecp, &interpp->txnp->tx);
        }
    }
    if (Duro_dt_tx(interpp) != NULL) {
        if (Duro_dt_execute_str("commit;", interpp, ecp) != RDB_OK)
            return NULL;
    }
    return view_op;

error:
    if (interpp->txnp != NULL) {
        /* Preserve the error in *ecp */
        RDB_exec_context ec;
        RDB_init_exec_context(&ec);
        Duro_dt_execute_str("commit;", interpp, &ec);
        RDB_destroy_exec_context(&ec);
    }
    return NULL;
}
