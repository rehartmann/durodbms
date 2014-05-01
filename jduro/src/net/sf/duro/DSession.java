package net.sf.duro;

public interface DSession extends AutoCloseable {

    /**
     * Executes Duro D/T code.
     * @param code		The code to execute
     * @throws DException	If a Duro error occurs
     */
    public void execute(String code) throws DException;

    /**
     * Evaluates a Duro D/T expression.
     * 
     * Duro types map to Java classes as follows:
     * 
     * <dl>
     * <dt><code>boolean</code> <dd><code>java.lang.Boolean</code>
     * <dt><code>string</code> <dd><code>java.lang.String</code>
     * <dt><code>integer</code> <dd><code>java.lang.Integer</code>
     * <dt><code>float</code> <dd><code>java.lang.Double</code>
     * <dt><code>binary</code> <dd><code>byte[]</code>
     * <dt><code>tuple</code> { ... } <dd>net.sf.duro.Tuple</dd>
     * <dt>types with declared possible representaions
     * <dd><code>net.sf.duro.PossrepObject</code>
     * </dl>
     * 
     * Duro arrays map to Java arrays.
     * For example, <code>array integer</code> is converted to <code>java.lang.Integer[]</code>.
     * 
     * @param	expr		The code to execute
     * @throws	DException	If a Duro error occurs
     */
    public Object evaluate(String expr) throws DException;

    /**
     * Assigns a value to a variable.
     * @param name	The name of the variable
     * @param v		The value
     * @throws DException	If a Duro error occurs.
     * @throws java.lang.IllegalArgumentException	If v does not match the
     * 		type of the variable
     */
    public void setVar(String name, Object v) throws DException;

    /**
     * Closes the session.
     * @throws DException	If a Duro error occurs
     */
    public void close() throws DException;
}
