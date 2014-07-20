/*
 * jduro.h
 *
 *  Created on: 02.07.2014
 *      Author: Rene Hartmann
 */

#ifndef JDURO_H_
#define JDURO_H_

#include <jni.h>
#include <dli/iinterp.h>

typedef struct {
    /* The interpreter */
    Duro_interp interp;

    /* The Java environment */
    JNIEnv *env;

    /* Reference to the DuroDSession object */
    jobject sessionObj;

    /* References to classes used often */
    jclass booleanClass;
    jclass integerClass;
    jclass stringClass;
    jclass doubleClass;
    jclass tupleClass;
    jclass byteArrayClass;
    jclass hashSetClass;
} JDuro_session;

JDuro_session *
JDuro_jobj_session(JNIEnv *, jobject);

int
JDuro_throw_exception_from_error(JNIEnv *, JDuro_session *, const char *,
        RDB_exec_context *);

jobject
JDuro_duro_obj_to_jobj(JNIEnv *, const RDB_object *, JDuro_session *);

int
JDuro_jobj_to_duro_obj(JNIEnv *, jobject, RDB_object *,
        JDuro_session *, RDB_exec_context *);

extern RDB_exec_context JDuro_ec;

#endif /* JDURO_H_ */
