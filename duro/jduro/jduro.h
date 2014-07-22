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
    jclass updatableBooleanClass;
    jclass integerClass;
    jclass updatableIntegerClass;
    jclass stringClass;
    jclass updatableStringClass;
    jclass doubleClass;
    jclass updatableDoubleClass;
    jclass tupleClass;
    jclass byteArrayClass;
    jclass hashSetClass;

    jmethodID booleanConstructorID;
    jmethodID updatableBooleanConstructorID;
    jmethodID integerConstructorID;
    jmethodID updatableIntegerConstructorID;
    jmethodID updatableStringConstructorID;
    jmethodID doubleConstructorID;
    jmethodID updatableDoubleConstructorID;
    jmethodID tupleConstructorID;
    jmethodID hashSetConstructorID;
} JDuro_session;

JDuro_session *
JDuro_jobj_session(JNIEnv *, jobject);

int
JDuro_throw_exception_from_error(JNIEnv *, JDuro_session *, const char *,
        RDB_exec_context *);

jobject
JDuro_duro_obj_to_jobj(JNIEnv *, const RDB_object *, RDB_bool, JDuro_session *);

int
JDuro_jobj_to_duro_obj(JNIEnv *, jobject, RDB_object *,
        JDuro_session *, RDB_exec_context *);

extern RDB_exec_context JDuro_ec;

#endif /* JDURO_H_ */
