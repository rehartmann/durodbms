/*
 * jduro.c
 *
 * Functions implementing JDuro's native methods.
 *
 *  Created on: 23.02.2014
 *      Author: Rene Hartmann
 */

#include "jduro.h"
#include <string.h>

int
JDuro_throw_exception_from_error(JNIEnv *env, JDuro_session *sessionp, const char *reason,
        RDB_exec_context *ecp)
{
    jobject errobj;
    jstring msgobj;
    jobject exception;
    jmethodID constructorID;

    constructorID = (*env)->GetMethodID(env, sessionp->dExceptionClass, "<init>",
            "(Ljava/lang/String;Ljava/lang/Object;)V");
    if (constructorID == NULL)
        return -1;

    errobj = JDuro_duro_obj_to_jobj(env, RDB_get_err(ecp), RDB_FALSE, sessionp);
    if (errobj == NULL)
        return -1;

    msgobj = (*env)->NewStringUTF(env, reason);
    if (msgobj == NULL)
        return -1;

    exception = (*env)->NewObject(env, sessionp->dExceptionClass,
            constructorID, msgobj, errobj);
    if (exception == NULL)
        return -1;

    return (*env)->Throw(env, exception);
}

static void
del_session(JNIEnv *env, JDuro_session *sessionp)
{
    if (sessionp->booleanClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->booleanClass);
    if (sessionp->tupleClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->tupleClass);
    if (sessionp->integerClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->integerClass);
    if (sessionp->stringClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->stringClass);
    if (sessionp->doubleClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->doubleClass);
    if (sessionp->byteArrayClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->byteArrayClass);
    if (sessionp->hashSetClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->hashSetClass);
    if (sessionp->updatableArrayClass != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->updatableArrayClass);
    if (sessionp->sessionObj != NULL)
        (*env)->DeleteGlobalRef(env, sessionp->sessionObj);
    Duro_destroy_interp(&sessionp->interp);
    free(sessionp);
}

static int
type_to_sig(RDB_object *objp, const RDB_type *typ, RDB_bool update,
        RDB_exec_context *ecp) {
    if (RDB_type_is_scalar(typ)) {
        if (typ == &RDB_STRING) {
            if (RDB_string_to_obj(objp,
                    update ? "Ljava/lang/StringBuilder;" : "Ljava/lang/String;",
                    ecp) != RDB_OK)
                return RDB_ERROR;
        } else if (typ == &RDB_INTEGER) {
            if (RDB_string_to_obj(objp,
                    update ? "Lnet/sf/duro/UpdatableInteger;" : "Ljava/lang/Integer;",
                    ecp) != RDB_OK)
                return RDB_ERROR;
        } else if (typ == &RDB_BOOLEAN) {
            if (RDB_string_to_obj(objp,
                    update ? "Lnet/sf/duro/UpdatableBoolean;" : "Ljava/lang/Boolean;",
                    ecp) != RDB_OK)
                return RDB_ERROR;
        } else if (typ == &RDB_FLOAT) {
            if (RDB_string_to_obj(objp,
                    update ? "Lnet/sf/duro/UpdatableDouble;" : "Ljava/lang/Double;",
                    ecp) != RDB_OK)
                return RDB_ERROR;
        } else if (typ == &RDB_BINARY) {
            if (RDB_string_to_obj(objp,
                    update ? "Lnet/sf/duro/ByteArray;" : "[B", ecp) != RDB_OK)
                return RDB_ERROR;
        } else {
            if (RDB_string_to_obj(objp, "Lnet/sf/duro/PossrepObject;", ecp) != RDB_OK)
                return RDB_ERROR;
        }
    } else if (RDB_type_is_tuple(typ)) {
        if (RDB_string_to_obj(objp, "Lnet/sf/duro/Tuple;", ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (RDB_type_is_relation(typ)) {
        if (RDB_string_to_obj(objp, "Ljava/util/Set;", ecp) != RDB_OK)
            return RDB_ERROR;
    } else if (RDB_type_is_array(typ)) {
        if (update) {
            if (RDB_string_to_obj(objp, "Ljava/util/ArrayList;", ecp) != RDB_OK)
                return RDB_ERROR;
        } else {
            RDB_object subtypestrobj;

            if (RDB_string_to_obj(objp, "[", ecp) != RDB_OK)
                return RDB_ERROR;

            RDB_init_obj(&subtypestrobj);
            if (type_to_sig(&subtypestrobj, RDB_base_type(typ), RDB_FALSE, ecp) != RDB_OK) {
                RDB_destroy_obj(&subtypestrobj, ecp);
                return RDB_ERROR;
            }
            if (RDB_append_string(objp, RDB_obj_string(&subtypestrobj), ecp) != RDB_OK) {
                RDB_destroy_obj(&subtypestrobj, ecp);
                return RDB_ERROR;
            }

            RDB_destroy_obj(&subtypestrobj, ecp);
        }
    }
    return RDB_OK;
}

/*
 * Create the signature of the Java method implementing operator *op
 * and store it in *sigobjp.
 */
static int
java_signature(RDB_operator *op, RDB_object *sigobjp, RDB_exec_context *ecp) {
    RDB_object jtypesigobj;
    RDB_type *rtyp;
    RDB_parameter *paramp;
    int i;

    RDB_init_obj(&jtypesigobj);

    if (RDB_string_to_obj(sigobjp, "(", ecp) != RDB_OK)
        goto error;

    rtyp = RDB_return_type(op);

    for (i = 0; (paramp = RDB_get_parameter(op, i)) != NULL; i++) {
        if (type_to_sig(&jtypesigobj, paramp->typ,
                (RDB_bool) (rtyp == NULL && paramp->update), ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(sigobjp, RDB_obj_string(&jtypesigobj), ecp)
                != RDB_OK)
            goto error;
    }
    RDB_append_string(sigobjp, ")", ecp);

    if (rtyp == NULL) {
        if (RDB_append_string(sigobjp, "V", ecp) != RDB_OK)
            goto error;
    } else {
        if (type_to_sig(&jtypesigobj, rtyp, RDB_FALSE, ecp) != RDB_OK)
            goto error;
        if (RDB_append_string(sigobjp, RDB_obj_string(&jtypesigobj), ecp) != RDB_OK)
            goto error;
    }

    RDB_destroy_obj(&jtypesigobj, ecp);
    return RDB_OK;

error:
    RDB_destroy_obj(&jtypesigobj, ecp);
    return RDB_ERROR;
}

/*
 * Replace the last '.' with a null character and return a pointer to the
 * following character.
 * Replace other '.'. by '/'.
 */
static char *
separate_method(char *qmethod) {
    char *chp;
    char *methodName = strrchr(qmethod, '.');
    if (methodName == NULL) {
        return NULL;
    }

    /* Separate method and class name */
    *methodName = '\0';
    methodName++;

    /* Replace '.' by '/' */
    for (chp = qmethod; *chp != '\0'; chp++) {
        if (*chp == '.')
            *chp = '/';
    }
    return methodName;
}

static void
duro_error_from_exception(JDuro_session *sessionp, jthrowable jex,
        RDB_exec_context *ecp)
{
    jobject errjobj;
    RDB_object *errobjp;
    jfieldID errorFieldID = (*sessionp->env)->GetFieldID(sessionp->env,
            sessionp->dExceptionClass, "error", "Ljava/lang/Object;");
    if (errorFieldID == NULL) {
        RDB_raise_internal("cannot find error field ID in DException", ecp);
    } else {
        errjobj = (*sessionp->env)->GetObjectField(sessionp->env, jex,
                errorFieldID);
        if (errjobj == NULL) {
            RDB_raise_internal("cannot find error field ID in DException", ecp);
        } else {
            errobjp = RDB_raise_err(ecp);

            if (JDuro_jobj_to_duro_obj(sessionp->env, errjobj, errobjp,
                    sessionp, ecp) != 0) {
                RDB_raise_internal("cannot convert DException to error", ecp);
            }
        }

    }
}

int
JDuro_invoke_ro_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp,
        RDB_object *retvalp)
{
    jclass clazz;
    jmethodID methodID;
    char *nameBuf;
    char *methodName;
    jobject result;
    RDB_object signature;
    int i;
    jthrowable jex;
    jvalue *jargv = NULL;
    JDuro_session *sessionp = RDB_ec_property(ecp, "JDuro_Session");
    if (sessionp == NULL) {
        RDB_raise_internal("JDuro_invoke_ro_op(): session not available", ecp);
        return RDB_ERROR;
    }

    nameBuf = RDB_dup_str(RDB_operator_source(op));
    if (nameBuf == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&signature);

    methodName = separate_method(nameBuf);
    if (methodName == NULL) {
        RDB_raise_operator_not_found(nameBuf, ecp);
        goto error;
    }

    clazz = (*sessionp->env)->FindClass(sessionp->env, nameBuf);
    if (clazz == NULL) {
        RDB_raise_resource_not_found(nameBuf, ecp);
        goto error;
    }

    if (java_signature(op, &signature, ecp) != RDB_OK)
        goto error;

    methodID = (*sessionp->env)->GetStaticMethodID(sessionp->env, clazz,
            methodName, RDB_obj_string(&signature));
    if (methodID == NULL) {
        RDB_raise_resource_not_found(methodName, ecp);
        goto error;
    }

    jargv = RDB_alloc(sizeof(jvalue) * argc, ecp);
    if (jargv == NULL)
        goto error;

    for (i = 0; i < argc; i++) {
        jargv[i].l = JDuro_duro_obj_to_jobj(sessionp->env, argv[i], RDB_FALSE,
                sessionp);
        if (jargv[i].l == NULL)
            goto error;
    }

    result = (*sessionp->env)->CallStaticObjectMethodA(sessionp->env, clazz,
            methodID, jargv);
    jex = (*sessionp->env)->ExceptionOccurred(sessionp->env);
    if (jex != NULL) {
        /* If a DException has been thrown, convert it to a Duro error */
        if (!(*sessionp->env)->IsInstanceOf(sessionp->env, jex,
                sessionp->dExceptionClass)) {
            RDB_raise_system("exception occurred during Java method call", ecp);
            goto error;
        }
        duro_error_from_exception(sessionp, jex, ecp);
        goto error;
    }

    /* Convert result */
    if (JDuro_jobj_to_duro_obj(sessionp->env, result, retvalp, sessionp, ecp)
            != 0)
        goto error;

    free(nameBuf);
    RDB_free(jargv);
    RDB_destroy_obj(&signature, ecp);
    return RDB_OK;

error:
    free(nameBuf);
    RDB_free(jargv);
    RDB_destroy_obj(&signature, ecp);
    return RDB_ERROR;
}

int
JDuro_invoke_update_op(int argc, RDB_object *argv[], RDB_operator *op,
        RDB_exec_context *ecp, RDB_transaction *txp)
{
    jclass clazz;
    jmethodID methodID;
    char *nameBuf;
    char *methodName;
    RDB_object signature;
    int i;
    jthrowable jex;
    jvalue *jargv = NULL;
    JDuro_session *sessionp = RDB_ec_property(ecp, "JDuro_Session");
    if (sessionp == NULL) {
        RDB_raise_internal("JDuro_invoke_ro_op(): session not available", ecp);
        return RDB_ERROR;
    }

    nameBuf = RDB_dup_str(RDB_operator_source(op));
    if (nameBuf == NULL) {
        RDB_raise_no_memory(ecp);
        return RDB_ERROR;
    }

    RDB_init_obj(&signature);

    methodName = separate_method(nameBuf);
    if (methodName == NULL) {
        RDB_raise_operator_not_found(nameBuf, ecp);
        goto error;
    }

    clazz = (*sessionp->env)->FindClass(sessionp->env, nameBuf);
    if (clazz == NULL) {
        RDB_raise_resource_not_found("class not found", ecp);
        goto error;
    }

    if (java_signature(op, &signature, ecp) != RDB_OK)
        goto error;

    methodID = (*sessionp->env)->GetStaticMethodID(sessionp->env, clazz,
            methodName, RDB_obj_string(&signature));
    if (methodID == NULL) {
        RDB_raise_resource_not_found(methodName, ecp);
        goto error;
    }

    jargv = RDB_alloc(sizeof(jvalue) * argc, ecp);
    if (jargv == NULL)
        goto error;

    for (i = 0; i < argc; i++) {
        jargv[i].l = JDuro_duro_obj_to_jobj(sessionp->env, argv[i],
                RDB_get_parameter(op, i)->update, sessionp);
        if (jargv[i].l == NULL)
            goto error;
    }

    (*sessionp->env)->CallStaticVoidMethodA(sessionp->env, clazz,
            methodID, jargv);
    jex = (*sessionp->env)->ExceptionOccurred(sessionp->env);
    if (jex != NULL) {
        /* If a DException has been thrown, convert it to a Duro error */
        if (!(*sessionp->env)->IsInstanceOf(sessionp->env, jex,
                sessionp->dExceptionClass)) {
            RDB_raise_system("exception occurred during Java method call", ecp);
            goto error;
        }
        duro_error_from_exception(sessionp, jex, ecp);
        goto error;
    }

    /* Convert updated arguments back */
    for (i = 0; i < argc; i++) {
        if (RDB_get_parameter(op, i)->update) {
            if (JDuro_jobj_to_duro_obj(sessionp->env, jargv[i].l,
                    argv[i], sessionp, &sessionp->ec) != 0) {
                JDuro_throw_exception_from_error(sessionp->env, sessionp,
                        "conversion of updated argument failed", &sessionp->ec);
                goto error;
            }
        }
    }

    free(nameBuf);
    RDB_free(jargv);
    RDB_destroy_obj(&signature, ecp);
    return RDB_OK;

error:
    free(nameBuf);
    RDB_free(jargv);
    RDB_destroy_obj(&signature, ecp);
    return RDB_ERROR;
}

static Duro_uop_info op_info = {
   "libjduro",
   "JDuro_invoke_ro_op",
   "JDuro_invoke_update_op"
};

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_initInterp(JNIEnv *env, jobject obj)
{
    jclass clazz;
    jfieldID interpFieldID;
    JDuro_session *sessionp;
    jclass dExClass = (*env)->FindClass(env, "net/sf/duro/DException");
    if (dExClass == NULL)
        return;

    sessionp = malloc(sizeof (JDuro_session));
    if (sessionp == NULL) {
        jclass clazz = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
        (*env)->ThrowNew(env, clazz, "");
        return;
    }

    sessionp->sessionObj = (*env)->NewGlobalRef(env, obj);
    if (sessionp->sessionObj == NULL)
        return;

    RDB_init_exec_context(&sessionp->ec);

    if (RDB_init_builtin(&sessionp->ec) != RDB_OK) {
        /*
         * Don't use JDuro_throw_exception_from_error() because that requires
         * an interpreter
         */
        (*env)->ThrowNew(env, dExClass,
                RDB_type_name(RDB_obj_type(RDB_get_err(&sessionp->ec))));
        free(sessionp);
        return;
    }
    if (Duro_init_interp(&sessionp->interp, &sessionp->ec, NULL, "") != RDB_OK) {
        (*env)->ThrowNew(env, dExClass,
                RDB_type_name(RDB_obj_type(RDB_get_err(&sessionp->ec))));
        free(sessionp);
        return;
    }

    sessionp->env = env;

    /*
     * Set session so it can be found when an operator implemented in Java
     * is called
     */
    RDB_ec_set_property(&sessionp->ec, "JDuro_Session", sessionp);

    if (Duro_dt_put_creop_info(&sessionp->interp, "Java", &op_info, &sessionp->ec)
            != RDB_OK) {
        (*env)->ThrowNew(env, dExClass,
                RDB_type_name(RDB_obj_type(RDB_get_err(&sessionp->ec))));
        free(sessionp);
        return;
    }

    /*
     * Get frequently used classes
     */

    sessionp->booleanClass = NULL;
    sessionp->updatableBooleanClass = NULL;
    sessionp->integerClass = NULL;
    sessionp->updatableIntegerClass = NULL;
    sessionp->stringClass = NULL;
    sessionp->updatableStringClass = NULL;
    sessionp->doubleClass = NULL;
    sessionp->updatableDoubleClass = NULL;
    sessionp->tupleClass = NULL;
    sessionp->byteArrayClass = NULL;
    sessionp->updatableByteArrayClass = NULL;
    sessionp->hashSetClass = NULL;
    sessionp->updatableArrayClass = NULL;

    sessionp->dExceptionClass = (jclass) (*env)->NewGlobalRef(env, dExClass);
    if (sessionp->dExceptionClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "java/lang/Boolean");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->booleanClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->booleanClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/UpdatableBoolean");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->updatableBooleanClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->updatableBooleanClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "java/lang/Integer");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->integerClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->integerClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/UpdatableInteger");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->updatableIntegerClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->updatableIntegerClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "java/lang/String");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->stringClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->stringClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "java/lang/StringBuilder");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->updatableStringClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->updatableStringClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "java/lang/Double");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->doubleClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->doubleClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/UpdatableDouble");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->updatableDoubleClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->updatableDoubleClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/Tuple");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->tupleClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->tupleClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "[B");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->byteArrayClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->byteArrayClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/ByteArray");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->updatableByteArrayClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->updatableByteArrayClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "java/util/HashSet");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->hashSetClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->hashSetClass == NULL) {
        goto error;
    }

    clazz = (*env)->FindClass(env, "java/util/ArrayList");
    if (clazz == NULL) {
        goto error;
    }
    sessionp->updatableArrayClass = (jclass) (*env)->NewGlobalRef(env, clazz);
    if (sessionp->updatableArrayClass == NULL) {
        goto error;
    }

    sessionp->updatableStringConstructorID = (*env)->GetMethodID(env,
            sessionp->updatableStringClass,
            "<init>", "(Ljava/lang/String;)V");
    if (sessionp->updatableStringConstructorID == NULL) {
        goto error;
    }

    sessionp->booleanConstructorID = (*env)->GetMethodID(env, sessionp->booleanClass,
            "<init>", "(Z)V");
    if (sessionp->booleanConstructorID == NULL)
        goto error;

    sessionp->updatableBooleanConstructorID = (*env)->GetMethodID(env, sessionp->booleanClass,
                "<init>", "(Z)V");
    if (sessionp->updatableBooleanConstructorID == NULL)
        goto error;

    sessionp->integerConstructorID = (*env)->GetMethodID(env, sessionp->integerClass,
            "<init>", "(I)V");
    if (sessionp->integerConstructorID == NULL)
        goto error;

    sessionp->updatableIntegerConstructorID = (*env)->GetMethodID(env,
            sessionp->updatableIntegerClass,
            "<init>", "(I)V");
    if (sessionp->updatableIntegerConstructorID == NULL)
        goto error;

    sessionp->doubleConstructorID = (*env)->GetMethodID(env, sessionp->doubleClass,
            "<init>", "(D)V");
    if (sessionp->doubleConstructorID == NULL)
        goto error;

    sessionp->updatableDoubleConstructorID = (*env)->GetMethodID(env,
            sessionp->updatableDoubleClass,
            "<init>", "(D)V");
    if (sessionp->updatableDoubleConstructorID == NULL)
        goto error;

    sessionp->tupleConstructorID = (*env)->GetMethodID(env, sessionp->tupleClass,
            "<init>", "()V");
    if (sessionp->tupleConstructorID == NULL)
        goto error;

    sessionp->updatableByteArrayConstructorID = (*env)->GetMethodID(env,
            sessionp->updatableByteArrayClass,
            "<init>", "([B)V");
    if (sessionp->updatableByteArrayConstructorID == NULL)
        goto error;

    sessionp->hashSetConstructorID = (*env)->GetMethodID(env, sessionp->hashSetClass,
            "<init>", "()V");
    if (sessionp->hashSetConstructorID == NULL)
        goto error;

    sessionp->updatableArrayConstructorID = (*env)->GetMethodID(env,
            sessionp->updatableArrayClass,
            "<init>", "(I)V");
    if (sessionp->updatableArrayConstructorID == NULL)
        goto error;

    clazz = (*env)->GetObjectClass(env, obj);
    interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    (*env)->SetLongField(env, obj, interpFieldID, (intptr_t) sessionp);
    return;

error:
    del_session(env, sessionp);
}

JDuro_session *
JDuro_jobj_session(JNIEnv *env, jobject obj)
{
    jlong interpField;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    if (interpFieldID == NULL)
        return NULL;
    interpField = (*env)->GetLongField(env, obj, interpFieldID);
    if (interpField == (jlong) 0) {
        return NULL;
    }
    return (JDuro_session *) (intptr_t) interpField;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_destroyInterp(JNIEnv *env, jobject obj)
{
    JDuro_session *sessionp;
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    jlong interpField = (*env)->GetLongField(env, obj, interpFieldID);

    if (interpField == (jlong) 0)
        return;

    sessionp = (JDuro_session *) (intptr_t) interpField;

    (*env)->SetLongField(env, obj, interpFieldID, (jlong) 0);
    RDB_destroy_exec_context(&sessionp->ec);
    del_session(env, sessionp);
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_executeI(JNIEnv *env, jobject jobj,
        jstring statements)
{
    int ret;
    jclass clazz = (*env)->GetObjectClass(env, jobj);
    jfieldID interpFieldID = (*env)->GetFieldID(env, clazz, "interp", "J");
    JDuro_session *sessionp = (JDuro_session *) (intptr_t)
            (*env)->GetLongField(env, jobj, interpFieldID);
    const char *str = (*env)->GetStringUTFChars(env, statements, 0);
    if (str == NULL)
        return;

    ret = Duro_dt_execute_str(str, &sessionp->interp, &sessionp->ec);
    (*env)->ReleaseStringUTFChars(env, statements, str);
    if (ret != RDB_OK) {
        JDuro_throw_exception_from_error(env, sessionp,
                    "execution of statements failed", &sessionp->ec);
    }
}

static jobject
tuple_to_jobj(JNIEnv *env, const RDB_object *tup, JDuro_session *sessionp)
{
    int i;
    jmethodID setAttributeID;
    jobject jobj;
    int n;
    char **namev;

    setAttributeID = (*env)->GetMethodID(env, sessionp->tupleClass,
            "setAttribute", "(Ljava/lang/String;Ljava/lang/Object;)V");
    if (setAttributeID == NULL)
        return NULL;
    jobj = (*env)->NewObject(env, sessionp->tupleClass,
            sessionp->tupleConstructorID);
    n = RDB_tuple_size(tup);
    namev = malloc(sizeof(char *) * n);
    if (namev == NULL)
        return NULL;
    RDB_tuple_attr_names(tup, namev);

    for (i = 0; i < n; i++) {
        RDB_object *attrp = RDB_tuple_get(tup, namev[i]);
        jstring jstr = (*env)->NewStringUTF(env, namev[i]);
        (*env)->CallVoidMethod(env, jobj, setAttributeID,
                jstr, JDuro_duro_obj_to_jobj(env, attrp, RDB_FALSE, sessionp));
    }
    free(namev);
    return jobj;
}

static jobject
table_to_jobj(JNIEnv *env, const RDB_object *tbp, JDuro_session *sessionp)
{
    RDB_object tpl;
    RDB_qresult *qrp;
    jobject elem;
    jmethodID addID;
    jobject jtable;
    addID = (*env)->GetMethodID( env, sessionp->hashSetClass, "add",
            "(Ljava/lang/Object;)Z");
    if (addID == NULL)
        return NULL;
    jtable = (*env)->NewObject(env, sessionp->hashSetClass,
            sessionp->hashSetConstructorID);
    if (jtable == NULL)
        return NULL;

    qrp = RDB_table_iterator((RDB_object *) tbp, 0, NULL, &sessionp->ec, NULL);
    if (qrp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp,
                "cannot create table iterator", &sessionp->ec);
        return NULL;
    }
    RDB_init_obj(&tpl);
    while (RDB_next_tuple(qrp, &tpl, &sessionp->ec, NULL) == RDB_OK) {
        elem = JDuro_duro_obj_to_jobj(env, &tpl, RDB_FALSE, sessionp);
        if (elem == NULL)
            goto error;
        (*env)->CallObjectMethod(env, jtable, addID, elem);
    }

    RDB_del_table_iterator(qrp, &sessionp->ec, NULL);
    RDB_destroy_obj(&tpl, &sessionp->ec);
    return jtable;

error:
    RDB_del_table_iterator(qrp, &sessionp->ec, NULL);
    RDB_destroy_obj(&tpl, &sessionp->ec);
    return NULL;
}

static jobject
array_to_jobj(JNIEnv *env, const RDB_object *arrp,
        JDuro_session *sessionp)
{
    int i;
    jobject objval;
    jobjectArray jobjarr;
    jclass clazz;
    RDB_int size = RDB_array_length(arrp, &sessionp->ec);

    RDB_type *elemtyp = RDB_base_type(RDB_obj_type(arrp));
    if (elemtyp == &RDB_INTEGER) {
        clazz = sessionp->integerClass;
    } else if (elemtyp == &RDB_FLOAT) {
        clazz = sessionp->doubleClass;
    } else if (elemtyp == &RDB_BOOLEAN) {
        clazz = sessionp->booleanClass;
    } else if (elemtyp == &RDB_STRING) {
        clazz = sessionp->stringClass;
    } else if (RDB_type_is_tuple(elemtyp)) {
        clazz = sessionp->tupleClass;
    } else if (RDB_type_is_relation(elemtyp)) {
        clazz = sessionp->hashSetClass;
    } else if (elemtyp == &RDB_BINARY) {
        clazz = sessionp->byteArrayClass;
    } else {
        clazz = (*env)->FindClass(env, "net/sf/duro/DuroPossrepObject");
    }

    jobjarr = (*env)->NewObjectArray(env, (jsize) size, clazz, NULL);
    if (jobjarr == NULL)
        return NULL;
    for (i = 0; i < size; i++) {
         objval = JDuro_duro_obj_to_jobj(env, RDB_array_get(arrp,
                 (RDB_int) i, &sessionp->ec), RDB_FALSE, sessionp);
         if (objval == NULL)
             return NULL;
         (*env)->SetObjectArrayElement(env, jobjarr, (jsize) i, objval);
    }

    return jobjarr;
}

static jobject
array_to_upd_jobj(JNIEnv *env, const RDB_object *arrp,
        JDuro_session *sessionp)
{
    int i;
    jobject arraylist;
    RDB_int len = RDB_array_length(arrp, &sessionp->ec);
    jmethodID addMethodID = (*env)->GetMethodID(env, sessionp->updatableArrayClass,
            "add", "(Ljava/lang/Object;)Z");
    if (addMethodID == NULL)
        return NULL;

    /* Create ArrayList instance */
    arraylist = (*env)->NewObject(env, sessionp->updatableArrayClass,
            sessionp->updatableArrayConstructorID, (jint) len);
    if (arraylist == NULL)
        return NULL;

    /* Add elements */
    for (i = 0; i < len; i++) {
        jobject elem = JDuro_duro_obj_to_jobj(env, RDB_array_get(arrp,
                (RDB_int) i, &sessionp->ec), RDB_FALSE, sessionp);
        if (elem == NULL)
            return elem;

        (*env)->CallBooleanMethod(env, arraylist, addMethodID, elem);
        if ((*sessionp->env)->ExceptionOccurred(sessionp->env) != NULL)
            return NULL;
    }

    return arraylist;
}

/*
 * Convert *objp to a Java object.
 */
jobject
JDuro_duro_obj_to_jobj(JNIEnv *env, const RDB_object *objp, RDB_bool updatable,
        JDuro_session *sessionp)
{
    jmethodID constructorID;
    jclass clazz;
    RDB_object *cobjp;
    jobject jobj = NULL;
    RDB_type *typ = RDB_obj_type(objp);

    if (RDB_is_tuple(objp))
        return tuple_to_jobj(env, objp, sessionp);
    if (typ == NULL) {
        (*env)->ThrowNew(env,
                         (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                         "missing type");
        return NULL;
    }
    if (typ == &RDB_STRING) {
        jobj = (jobject) (*env)->NewStringUTF(env, RDB_obj_string(objp));
        if (jobj == NULL)
            return NULL;
        if (updatable) {
            jobj = (*env)->NewObject(env, sessionp->updatableStringClass,
                    sessionp->updatableStringConstructorID, jobj);
        }
        return jobj;
    }
    if (typ == &RDB_INTEGER) {
        return (*env)->NewObject(env,
                updatable ? sessionp->updatableIntegerClass : sessionp->integerClass,
                updatable ? sessionp->updatableIntegerConstructorID : sessionp->integerConstructorID,
                (jint) RDB_obj_int(objp));
    }
    if (typ == &RDB_FLOAT) {
        return (*env)->NewObject(env,
                updatable ? sessionp->updatableDoubleClass : sessionp->doubleClass,
                updatable ? sessionp->updatableDoubleConstructorID : sessionp->doubleConstructorID,
                (jdouble) RDB_obj_float(objp));
    }
    if (typ == &RDB_BOOLEAN) {
        return (*env)->NewObject(env,
                updatable ? sessionp->updatableBooleanClass : sessionp->booleanClass,
                updatable ? sessionp->updatableBooleanConstructorID : sessionp->booleanConstructorID,
                (jboolean) RDB_obj_bool(objp));
    }
    if (typ == &RDB_BINARY) {
        void *bp;
        jsize len = (jsize) RDB_binary_length(objp);
        jobj = (*env)->NewByteArray(env, len);
        if (jobj == NULL)
            return NULL;
        if (RDB_binary_get(objp, 0, (size_t) len, &sessionp->ec, &bp, NULL) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "getting byte array failed", &sessionp->ec);
            return NULL;
        }

        (*env)->SetByteArrayRegion(env, jobj, 0, len, bp);
        if (updatable) {
            jobj = (*env)->NewObject(env, sessionp->updatableByteArrayClass,
                        sessionp->updatableByteArrayConstructorID, jobj);
        }
        return jobj;
    }
    if (RDB_type_is_array(typ)) {
        return updatable ? array_to_upd_jobj(env, objp, sessionp)
                : array_to_jobj(env, objp, sessionp);
    }
    if (RDB_type_is_relation(typ)) {
        return table_to_jobj(env, objp, sessionp);
    }

    /*
     * Create a copy of *objp which is managed by the Java object
     */
    cobjp = RDB_new_obj(&sessionp->ec);
    if (cobjp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp,
                "creating RDB_object failed", &sessionp->ec);
        goto error;
    }

    if (RDB_copy_obj(cobjp, objp, &sessionp->ec) == RDB_ERROR) {
        JDuro_throw_exception_from_error(env, sessionp,
                "copying RDB_object failed", &sessionp->ec);
        goto error;
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/DuroPossrepObject");
    if (clazz == NULL)
        goto error;
    constructorID = (*env)->GetMethodID(env, clazz, "<init>",
            "(JLnet/sf/duro/DuroDSession;)V");
    if (constructorID == NULL) {
        goto error;
    }

    /* objp will be managed by the Java object */

    jobj = (*env)->NewObject(env, clazz, constructorID,
            (jlong) (intptr_t) cobjp, sessionp->sessionObj);
    if (jobj == NULL)
        goto error;
    return jobj;

error:
    if (cobjp != NULL)
        RDB_free_obj(cobjp, &sessionp->ec);
    return NULL;
}

JNIEXPORT jobject
JNICALL Java_net_sf_duro_DuroDSession_evaluateI(JNIEnv *env, jobject obj,
        jstring expression)
{
    RDB_object result;
    jobject jresult;
    RDB_expression *exp;
    const char *str;
    JDuro_session *sessionp = JDuro_jobj_session(env, obj);
    if (sessionp == NULL)
        return NULL;
    str = (*env)->GetStringUTFChars(env, expression, 0);

    exp = Duro_dt_parse_expr_str(str, &sessionp->interp, &sessionp->ec);
    (*env)->ReleaseStringUTFChars(env, expression, str);
    if (exp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp,
                "evaluating expression failed", &sessionp->ec);
        return NULL;
    }

    RDB_init_obj(&result);
    if (Duro_evaluate_retry(exp, &sessionp->interp, &sessionp->ec,
            &result) != RDB_OK) {
        JDuro_throw_exception_from_error(env, sessionp,
                "expression evaluation failed", &sessionp->ec);
        goto error;
    }

    /* If the expression type is missing, try to get it */
    if (RDB_obj_type(&result) == NULL
            && !RDB_is_tuple(&result)) {
        RDB_type *typ = Duro_expr_type_retry(exp, &sessionp->interp, &sessionp->ec);
        if (typ == NULL) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "getting expression type failed", &sessionp->ec);
            goto error;
        }
        RDB_obj_set_typeinfo(&result, typ);
    }
    jresult = JDuro_duro_obj_to_jobj(env, &result, RDB_FALSE, sessionp);
    RDB_destroy_obj(&result, &sessionp->ec);
    RDB_del_expr(exp, &sessionp->ec);
    return jresult;

error:
    RDB_destroy_obj(&result, &sessionp->ec);
    RDB_del_expr(exp, &sessionp->ec);
    return NULL;
}

static int
jtuple_to_obj(JNIEnv *env, RDB_object *dstp, jobject obj, JDuro_session *sessionp,
        RDB_exec_context *ecp)
{
    jstring attrnameobj;
    jobject attrvalobj;
    const char *attrname;
    RDB_object attrval;
    jmethodID getAttributeMethodID;
    jobject keyset;
    RDB_type *dsttyp = RDB_obj_type(dstp);
    jclass tupleclazz = (*env)->GetObjectClass(env, obj);
    jmethodID methodID = (*env)->GetMethodID(env, tupleclazz, "attributeNames",
            "()Ljava/util/Set;");
    if (methodID == NULL)
        return -1;
    keyset = (*env)->CallObjectMethod(env, obj, methodID);

    getAttributeMethodID = (*env)->GetMethodID(env, tupleclazz, "getAttribute",
            "(Ljava/lang/String;)Ljava/lang/Object;");
    if (getAttributeMethodID == NULL)
        return -1;

    if (dsttyp != NULL) {
        int i;
        RDB_attr *attrv;
        int attrc;
        RDB_object *attrvalp;
        jmethodID sizeMethodID = (*env)->GetMethodID(env, tupleclazz, "attributeNames",
                "()Ljava/util/Set;");

        if (!RDB_type_is_tuple(dsttyp)) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be tuple");
            return -1;
        }

        /*
         * Copy all attributes
         */

        /* Get attributes from type */
        attrv = RDB_type_attrs(dsttyp, &attrc);

        /* Compare size */
        sizeMethodID = (*env)->GetMethodID(env, tupleclazz, "size", "()I");
        if (sizeMethodID == NULL)
            return -1;
        if (attrc != (int) (*env)->CallIntMethod(env, obj, sizeMethodID)) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "tuple size differs from destination");
            return -1;
        }

        for (i = 0; i < attrc; i++) {
            /* Get Duro tuple attribute */
            attrvalp = RDB_tuple_get(dstp, attrv[i].name);
            if (attrvalp == NULL) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "attribute not present in destination");
                return -1;
            }

            /* Get attribute from Java tuple */
            attrnameobj = (*env)->NewStringUTF(env, attrv[i].name);
            if (attrnameobj == NULL) {
                return -1;
            }
            attrvalobj = (*env)->CallObjectMethod(env, obj, getAttributeMethodID,
                    attrnameobj);
            if (attrvalobj == NULL) {
                if ((*env)->ExceptionOccurred(env) == NULL) {
                    (*env)->ThrowNew(env,
                            (*env)->FindClass(env,
                                    "java/lang/IllegalArgumentException"),
                                    "missing attribute");
                }
                return -1;
            }

            /* Convert attribute value */
            RDB_init_obj(&attrval);
            if (JDuro_jobj_to_duro_obj(env, attrvalobj, &attrval, sessionp, ecp) != 0) {
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }

            /* Copy attribute to tuple */
            if (RDB_copy_obj(attrvalp, &attrval, &sessionp->ec) != RDB_OK) {
                JDuro_throw_exception_from_error(env, sessionp, "setting tuple attribute failed", ecp);
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }
            RDB_destroy_obj(&attrval, ecp);
        }
    } else {
        jobject iter;
        jclass iterclazz;
        jclass setclazz;
        jmethodID hasNextMethodID;
        jmethodID nextMethodID;
        setclazz = (*env)->GetObjectClass(env, keyset);
        methodID = (*env)->GetMethodID(env, setclazz, "iterator",
                "()Ljava/util/Iterator;");
        iter = (*env)->CallObjectMethod(env, keyset, methodID);
        if (iter == NULL)
            return -1;
        iterclazz = (*env)->GetObjectClass(env, iter);
        hasNextMethodID = (*env)->GetMethodID(env, iterclazz, "hasNext",
                "()Z");
        nextMethodID = (*env)->GetMethodID(env, iterclazz, "next",
                "()Ljava/lang/Object;");

        for(;;) {
            /* Invoke hashNext() to check if there are any attributes left to read */
            if (!(*env)->CallBooleanMethod(env, iter, hasNextMethodID))
                break;

            /* Get attribute name and value */
            attrnameobj = (*env)->CallObjectMethod(env, iter, nextMethodID);
            attrvalobj = (*env)->CallObjectMethod(env, obj, getAttributeMethodID,
                    attrnameobj);
            attrname = (*env)->GetStringUTFChars(env, attrnameobj, 0);

            RDB_init_obj(&attrval);
            if (JDuro_jobj_to_duro_obj(env, attrvalobj, &attrval, sessionp, ecp) != 0) {
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }
            if (RDB_tuple_set(dstp, attrname, &attrval, ecp) != RDB_OK) {
                JDuro_throw_exception_from_error(env, sessionp,
                        "setting tuple attribute failed", ecp);
                RDB_destroy_obj(&attrval, ecp);
                return -1;
            }
            RDB_destroy_obj(&attrval, ecp);

            (*env)->ReleaseStringUTFChars(env, attrnameobj, attrname);
        }
    }
    return 0;
}

static int
jobj_to_table(JNIEnv *env, jobject obj, RDB_object *dstp,
        JDuro_session *sessionp, RDB_exec_context *ecp)
{
    RDB_object tpl;
    jobject iter;
    jobject elem;
    jclass iterclazz, setclazz;
    jmethodID methodID, hasNextMethodID, nextMethodID;

    if (RDB_obj_type(dstp) == NULL
            || !RDB_type_is_relation(RDB_obj_type(dstp))) {
        RDB_raise_type_mismatch("not a table", &sessionp->ec);
        JDuro_throw_exception_from_error(env, sessionp,
                "destination is not a table", &sessionp->ec);
        return -1;
    }

    setclazz = (*env)->GetObjectClass(env, obj);
    methodID = (*env)->GetMethodID(env, setclazz, "iterator",
            "()Ljava/util/Iterator;");
    iter = (*env)->CallObjectMethod(env, obj, methodID);
    if (iter == NULL)
        return -1;
    iterclazz = (*env)->GetObjectClass(env, iter);
    hasNextMethodID = (*env)->GetMethodID(env, iterclazz, "hasNext",
            "()Z");
    nextMethodID = (*env)->GetMethodID(env, iterclazz, "next",
            "()Ljava/lang/Object;");

    /*
     * Clear table
     */
    if (RDB_delete(dstp, NULL, &sessionp->ec, Duro_dt_tx(&sessionp->interp))
            == (RDB_int) RDB_ERROR) {
        JDuro_throw_exception_from_error(env, sessionp, "delete failed", &sessionp->ec);
        return -1;
    }

    RDB_init_obj(&tpl);
    for(;;) {
        /* Invoke hashNext() to check if there are any attributes left to read */
        if (!(*env)->CallBooleanMethod(env, iter, hasNextMethodID))
            break;

        /* Get next set element */
        elem = (*env)->CallObjectMethod(env, iter, nextMethodID);
        if (elem == NULL)
            goto error;

        /* Convert it to a Duro tuple */
        if (JDuro_jobj_to_duro_obj(env, elem, &tpl, sessionp, &sessionp->ec) == -1)
            goto error;

        /* Insert tuple into table */
        if (RDB_insert(dstp, &tpl, &sessionp->ec,
                Duro_dt_tx(&sessionp->interp)) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "insert failed", &sessionp->ec);
            goto error;
        }
    }
    RDB_destroy_obj(&tpl, &sessionp->ec);
    return 0;

error:
    RDB_destroy_obj(&tpl, &sessionp->ec);
    return -1;
}

/*
 * Convert a Java object to a RDB_object.
 * *dstp must be either newly initialized or carry type information.
 * If the Java object is a java.util.Set, *dstp must be an empty table.
 *
 * If conversion fails, a Java exception is thrown and -1 is returned.
 */
int
JDuro_jobj_to_duro_obj(JNIEnv *env, jobject obj, RDB_object *dstp,
        JDuro_session *sessionp, RDB_exec_context *ecp)
{
    jsize len;
    jmethodID methodID;
    jclass clazz;
    const char *strval;
    RDB_type *typ = RDB_obj_type(dstp);
    if ((*env)->IsInstanceOf(env, obj, sessionp->stringClass)) {
        strval = (*env)->GetStringUTFChars(env, obj, 0);

        if (typ != NULL && typ != &RDB_STRING) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be string");
            return -1;
        }

        if (RDB_string_to_obj(dstp, strval, ecp) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "getting string data failed", ecp);
            return -1;
        }
        return 0;
    }
    if ((*env)->IsInstanceOf(env, obj, sessionp->updatableStringClass)) {
        methodID = (*env)->GetMethodID(env, sessionp->updatableStringClass,
                "toString", "()Ljava/lang/String;");
        obj = (*env)->CallObjectMethod(env, obj, methodID);
        if (obj == NULL)
            return -1;

        strval = (*env)->GetStringUTFChars(env, obj, 0);

        if (typ != NULL && typ != &RDB_STRING) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be string");
            return -1;
        }

        if (RDB_string_to_obj(dstp, strval, ecp) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "getting string data failed", ecp);
            return -1;
        }
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->integerClass)) {
        if (typ != NULL && typ != &RDB_INTEGER) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be integer");
            return -1;
        }
        methodID = (*env)->GetMethodID(env, sessionp->integerClass,
                "intValue", "()I");
        RDB_int_to_obj(dstp, (RDB_int) (*env)->CallIntMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->updatableIntegerClass)) {
        if (typ != NULL && typ != &RDB_INTEGER) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be integer");
            return -1;
        }
        methodID = (*env)->GetMethodID(env, sessionp->integerClass,
                "intValue", "()I");
        RDB_int_to_obj(dstp, (RDB_int) (*env)->CallIntMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->doubleClass)) {
        if (typ != NULL && typ != &RDB_FLOAT) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be float");
            return -1;
        }

        methodID = (*env)->GetMethodID(env, sessionp->doubleClass, "doubleValue",
                "()D");
        RDB_float_to_obj(dstp, (RDB_float) (*env)->CallDoubleMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->updatableDoubleClass)) {
        if (typ != NULL && typ != &RDB_FLOAT) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be float");
            return -1;
        }

        methodID = (*env)->GetMethodID(env, sessionp->updatableDoubleClass,
                "doubleValue", "()D");
        RDB_float_to_obj(dstp, (RDB_float) (*env)->CallDoubleMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->booleanClass)) {
        if (typ != NULL && typ != &RDB_BOOLEAN) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be boolean");
            return -1;
        }

        methodID = (*env)->GetMethodID(env, sessionp->booleanClass,
                "booleanValue", "()Z");
        RDB_bool_to_obj(dstp, (RDB_float) (*env)->CallBooleanMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->updatableBooleanClass)) {
        if (typ != NULL && typ != &RDB_BOOLEAN) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be boolean");
            return -1;
        }

        methodID = (*env)->GetMethodID(env, sessionp->updatableBooleanClass,
                "booleanValue", "()Z");
        RDB_bool_to_obj(dstp, (RDB_float) (*env)->CallBooleanMethod(env, obj, methodID));
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->byteArrayClass)) {
        jbyte *bp;

        if (typ != NULL && typ != &RDB_BINARY) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be binary");
            return -1;
        }

        len = (*env)->GetArrayLength(env, obj);
        bp = (*env)->GetByteArrayElements(env, obj, NULL);
        if (bp == NULL)
            return -1;
        if (RDB_binary_set (dstp, 0, bp, (size_t) len, &sessionp->ec) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "setting binary data failed",
                    &sessionp->ec);
            return -1;
        }
        (*env)->ReleaseByteArrayElements(env, obj, bp, JNI_ABORT);
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->updatableByteArrayClass)) {
        jobject jobj;

        if (typ != NULL && typ != &RDB_BINARY) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be binary");
            return -1;
        }

        methodID = (*env)->GetMethodID(env, sessionp->updatableByteArrayClass,
                "getBytes", "()[B");

        jobj = (*env)->CallObjectMethod(env, obj, methodID);
        return JDuro_jobj_to_duro_obj(env, jobj, dstp, sessionp, ecp);
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->updatableArrayClass)) {
        int i;
        RDB_int len;
        jobject elem;
        RDB_object *elemp;
        jmethodID sizeMethodID = (*env)->GetMethodID(env,
                sessionp->updatableArrayClass, "size", "()I");
        jmethodID getMethodID = (*env)->GetMethodID(env,
                sessionp->updatableArrayClass, "get", "(I)Ljava/lang/Object;");

        len = (RDB_int) (*env)->CallIntMethod(env, obj, sizeMethodID);

        if (RDB_set_array_length(dstp, len, ecp) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp,
                    "conversion to DuroDBMS array failed", &sessionp->ec);
            return -1;
        }

        for (i = 0; i < (int) len; i++) {
            elem = (*env)->CallObjectMethod(env, obj, getMethodID, (jint) i);
            if ((*sessionp->env)->ExceptionOccurred(sessionp->env) != NULL)
                return -1;
            elemp = RDB_array_get(dstp, (RDB_int) i, ecp);
            if (JDuro_jobj_to_duro_obj(env, elem, elemp, sessionp, ecp) != 0)
                return -1;
        }

        return 0;
    }

    /* Other array types */
    clazz = (*env)->FindClass(env, "[Ljava/lang/Object;");
    if (clazz == NULL)
        return -1;
    if ((*env)->IsAssignableFrom(env, (*env)->GetObjectClass(env, obj), clazz)) {
        /* Convert array of objects */
        int i;
        RDB_object *dstelemp;
        RDB_type *basetyp = NULL;
        len = (*env)->GetArrayLength(env, obj);

        if (typ != NULL && !RDB_type_is_array(typ)) {
            (*env)->ThrowNew(env,
                    (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                    "destination must be array");
            return -1;
        }

        if (typ != NULL)
            basetyp = RDB_base_type(typ);

        if (basetyp != NULL) {
            /* Check base type against Java array type */
            clazz = (*env)->FindClass(env, "[Ljava/lang/String;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_STRING) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be string array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[Ljava/lang/Integer;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_INTEGER) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be integer array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[Ljava/lang/Double;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_FLOAT) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be float array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[Ljava/lang/Boolean;");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_BOOLEAN) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be boolean array");
                return -1;
            }

            clazz = (*env)->FindClass(env, "[[B");
            if (clazz == NULL)
                return -1;
            if ((*env)->IsInstanceOf(env, obj, clazz) && basetyp != &RDB_BINARY) {
                (*env)->ThrowNew(env,
                        (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                        "destination must be array of binary");
                return -1;
            }
        }

        if (RDB_set_array_length(dstp, (RDB_int) len, &sessionp->ec) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "setting binary data failed",
                    &sessionp->ec);
            return -1;
        }
        for (i = 0; i < len; i++) {
            dstelemp = RDB_array_get(dstp, (RDB_int) i, &sessionp->ec);

            if (JDuro_jobj_to_duro_obj(env, (*env)->GetObjectArrayElement(env,
                                       obj, (jsize) i),
                                 dstelemp, sessionp, &sessionp->ec) != 0) {
                return -1;
            }
        }
        return 0;
    }

    if ((*env)->IsInstanceOf(env, obj, sessionp->tupleClass)) {
        return jtuple_to_obj(env, dstp, obj, sessionp, ecp);
    }

    clazz = (*env)->FindClass(env, "net/sf/duro/DuroPossrepObject");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        RDB_object *objp;
        jfieldID refFieldID = (*env)->GetFieldID(env, clazz, "ref", "J");
        if (refFieldID == NULL)
            return -1;

        objp = (RDB_object *) (intptr_t)
                (*env)->GetLongField(env, obj, refFieldID);
        if (objp == NULL)
            return -1;

        if (RDB_copy_obj(dstp, objp, ecp) != RDB_OK) {
            JDuro_throw_exception_from_error(env, sessionp, "copy failed", ecp);
            return -1;
        }
        return 0;
    }

    clazz = (*env)->FindClass(env, "java/util/Set");
    if ((*env)->IsInstanceOf(env, obj, clazz)) {
        return jobj_to_table(env, obj, dstp, sessionp, &sessionp->ec);
    }

    clazz = (*env)->FindClass(env, "java/lang/IllegalArgumentException");
    (*env)->ThrowNew(env, clazz, "unsupported class");
    return -1;
}

JNIEXPORT void
JNICALL Java_net_sf_duro_DuroDSession_setVarI(JNIEnv *env, jobject obj,
        jstring name, jobject value)
{
    const char *namestr;
    RDB_object *dstp;
    JDuro_session *sessionp = JDuro_jobj_session(env, obj);
    if (sessionp == NULL)
        return;
    namestr = (*env)->GetStringUTFChars(env, name, 0);
    if (namestr == NULL)
        return;

    dstp = Duro_lookup_var(namestr, &sessionp->interp, &sessionp->ec);
    if (dstp == NULL) {
        JDuro_throw_exception_from_error(env, sessionp, "variable lookup failed", &sessionp->ec);
        goto cleanup;
    }

    if (JDuro_jobj_to_duro_obj(env, value, dstp, sessionp, &sessionp->ec) != 0) {
        goto cleanup;
    }

cleanup:
    (*env)->ReleaseStringUTFChars(env, name, namestr);
}
