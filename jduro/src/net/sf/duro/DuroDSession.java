package net.sf.duro;

public class DuroDSession implements DSession {
    private long interp = 0L; // Contains the pointer to the Duro_interp structure

    static { 
        System.loadLibrary("jduro"); 
    }

    private DuroDSession() {}

    native private void initInterp() throws DException;

    native private void destroyInterp() throws DException;

    native private void executeI(String s) throws DException;

    native private Object evaluateI(String expr) throws DException;

    native private Object setVarI(String name, Object v) throws DException;

    static public DSession createSession() throws DException {
	DuroDSession instance = new DuroDSession();
	synchronized(DuroDSession.class) {
	    instance.initInterp();
	}
	return instance;
    }

    public void close() throws DException {
	synchronized(DuroDSession.class) {
	    destroyInterp();
	}
    }

    public void execute(String s) throws DException {
	synchronized(DuroDSession.class) {
	    executeI(s);
	}
    }

    public Object evaluate(String expr) throws DException {
	synchronized(DuroDSession.class) {
	    return evaluateI(expr);
	}
    }

    public Object setVar(String name, Object v) throws DException
    {
	synchronized(DuroDSession.class) {
	    return setVarI(name, v);
	}
    }
}
