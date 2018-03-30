package net.sf.duro;

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
        synchronized (LocalSession.class) {
            destroyInterp();
        }
    }

    public void execute(String code) {
        try {
            synchronized (LocalSession.class) {
                executeI(code);
            }
        } catch (ClassNotFoundException|NoSuchMethodException e) {
            throw new DException(e);
        }
    }

    public Object evaluate(String expr) {
        try {
            synchronized (LocalSession.class) {
                return evaluateI(expr);
            }
        } catch (ClassNotFoundException|NoSuchMethodException e) {
            throw new DException(e);
        }
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
        synchronized (LocalSession.class) {
            setVarI(name, v);
        }
    }
}
