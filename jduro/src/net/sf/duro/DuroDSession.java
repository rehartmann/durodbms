package net.sf.duro;

/**
 * This class implements the DSession interface.
 * @author Rene Hartmann
 *
 */
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

    native private void setVarI(String name, Object v) throws DException;

    /**
     * Creates a DSession.
     * @return the DSession created.
     * @throws DException	if a Duro error occurs
     */
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

    public void setVar(String name, Object v) throws DException
    {
	synchronized(DuroDSession.class) {
	    setVarI(name, v);
	}
    }
}
