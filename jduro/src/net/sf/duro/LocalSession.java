package net.sf.duro;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

public class LocalSession extends DSession {
    static {
        System.loadLibrary("jduro");
    }
    
    private long interp = 0L; // Contains the pointer to the Duro_interp structure

    native private void initInterp();

    native private void destroyInterp();

    native private void executeI(String s)
            throws ClassNotFoundException, NoSuchMethodException;

    native private Object evaluateI(String expr)
            throws ClassNotFoundException, NoSuchMethodException;

    native private void setVarI(String name, Object v);

    LocalSession() {
        synchronized (LocalSession.class) {
            initInterp();
        }
    }

    /**
     * Closes the session.
     * 
     * @throws DException
     *             If a Duro error occurs
     */
    public void close() {
        synchronized (DSession.class) {
            destroyInterp();
        }
    }

    public void execute(String code) {
        try {
            synchronized (DSession.class) {
                executeI(code);
            }
        } catch (ClassNotFoundException|NoSuchMethodException e) {
            throw new DException(e);
        }
    }

    public Object evaluate(String expr) {
        try {
            synchronized (DSession.class) {
                return evaluateI(expr);
            }
        } catch (ClassNotFoundException|NoSuchMethodException e) {
            throw new DException(e);
        }
    }

    public void evaluate(String expr, Object dest) {
        PossrepObject src;
        try {
            src = (PossrepObject) evaluate(expr);
        } catch (ClassCastException ex) {
            throw new IllegalArgumentException("Expression type does not have possreps");
        }
        Possrep pr = src.getType().getPossreps()[0];
        for (int i = 0; i < pr.getComponents().length; i++) {
            String propname = pr.getComponent(i).getName();
            Object prop = src.getProperty(propname);
            String setterName = "set" + propname.substring(0, 1).toUpperCase()
                    + propname.substring(1);
            Class<?> parameterType;
            if (prop instanceof Integer) {
                parameterType = int.class;
            } else if (prop instanceof Double) {
                parameterType = double.class;
            } else if (prop instanceof Boolean) {
                parameterType = boolean.class;
            } else {
                parameterType = prop.getClass();
            }
            try {
                dest.getClass().getMethod(setterName, parameterType).invoke(dest, prop);
            } catch (IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                throw new DException(e);
            }
        }
        src.dispose();
    }

    public <T> T evaluate(String expr, Class<T> destClass) {
        Object result;
        if (destClass.isInterface()) {
            PossrepObject po = (PossrepObject) evaluate(expr);
            result = Proxy.newProxyInstance(destClass.getClassLoader(),
                    new Class<?>[] { destClass },
                    new PossrepInvocationHandler(po));
        } else {
            try {
                result = destClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new DException(e);
            }
            evaluate(expr, result);
        }
        return (T) result;
    }
    
    /**
     * Assigns a value to a variable.
     * 
     * @param name
     *            The name of the variable
     * @param v
     *            The value
     * @throws DException
     *             If a Duro error occurs.
     * @throws java.lang.IllegalArgumentException
     *             If v does not match the type of the variable.
     */
    public void setVar(String name, Object v) {
        synchronized (DSession.class) {
            setVarI(name, v);
        }
    }
}
