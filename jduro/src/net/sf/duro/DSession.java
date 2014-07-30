package net.sf.duro;

/**
 * The main runtime interface between a Java application and DuroDBMS.
 *
 * @author Rene Hartmann
 *
 */
public interface DSession extends AutoCloseable {

    /**
     * Executes Duro D/T code.
     * @param code		The code to execute
     * @throws DException	If a Duro error occurs
     * @throws	IllegalStateException	If the code could not be executed
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
     * <dt><code>tuple { ... }</code> <dd>{@link net.sf.duro.Tuple}</dd>
     * <dt><code>relation { ... }</code> <dd><code>java.util.Set</code>
     * <dt><code>array</code>
     * <dd>Duro arrays map to Java arrays.
     * For example, <code>array integer</code> is converted to
     * <code>java.lang.Integer[]</code>.</dd>
     * <dt>types with declared possible representations
     * <dd><code>{@link net.sf.duro.PossrepObject}</code>
     * </dl>
     * 
     * 
     * 
     * @param	expr		The code to execute
     * @throws	DException	If a Duro error occurs
     * @throws	IllegalStateException	If the code could not be executed
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
