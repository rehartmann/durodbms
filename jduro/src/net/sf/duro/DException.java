package net.sf.duro;

/**
 * Signals that a Duro exception has occurred.
 */
public class DException extends Exception {

    private static final long serialVersionUID = -8161811884817880524L;

    private Object error;

    public DException() {}

    public DException(Object error) {
	this.error = error;
    }

    public DException(String message, Object error) {
	super(message);
	this.error = error;
    }

    public Object getError() {
	return error;
    }

    public String toString() {
	StringBuffer buf = new StringBuffer(super.toString());
	if (error != null && error instanceof PossrepObject) {
	    String typename =  ((PossrepObject)error).getTypeName();
	    if (typename != null) {
		buf.append(", ");
		buf.append(typename);
	    }
	    try {
		String msg = (String) ((PossrepObject)error).getProperty("msg");
		if (msg != null) {
	            buf.append(": ");
		    buf.append(msg);
                }
	    } catch (DException ex) {
		// Ignore invalid_argument_error
		Object error = ex.getError();
		if (!(error instanceof PossrepObject)
			|| !((PossrepObject) error).getTypeName()
			    .equals("invalid_argument_error")) {
		    buf.append(": [error reading msg property]");
		}
	    }
	}
	return buf.toString();
    }
}
