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
     * 
     * <p>User-defined operators implemented in Java can be created by
     * using the OPERATOR statment with the EXTERN keyword.
     * 
     * <p>Creating a user-defined read-only operator implemented by a Java method:
     * 
     * <p><code>OPERATOR &lt;opname&gt;(&lt;parameter_list&gt;) RETURNS &lt;type&gt;<br />
     * EXTERN 'Java' '&lt;classname&gt;.&lt;methodname&gt;';<br />
     * END OPERATOR;</code> 
     *
     * <p>The method must be static. Parameter types and return type map to
     * Java types as described in DSession.evaluate().
     * 
     * <p>Creating an user-defined update operator implemented by a Java method:
     * 
     * <p><code>OPERATOR &lt;opname&gt;(&lt;parameter_list&gt;) UPDATES { &lt;update_param_list&gt; }<br />
     * EXTERN 'Java' '&lt;classname&gt;.&lt;methodname&gt;';<br />
     * END OPERATOR;</code> 
     *
     * <p>The method must be static. Non-update parameter types map to
     * Java types as described in {@link net.sf.duro.DSession#evaluate(String) evaluate}.
     * Update parameter types map to Java classes as follows:
     * 
     * <dl>
     * <dt><code>boolean</code> <dd>{@link net.sf.duro.UpdatableBoolean}</dd>
     * <dt><code>string</code> <dd>java.lang.StringBuilder</dd>
     * <dt><code>integer</code> <dd>{@link net.sf.duro.UpdatableInteger}</dd>
     * <dt><code>float</code> <dd>{@link net.sf.duro.UpdatableDouble}</dd>
     * <dt><code>binary</code> <dd>{@link net.sf.duro.ByteArray}</dd>
     * <dt><code>tuple { ... }</code> <dd>{@link net.sf.duro.Tuple}</dd>
     * <dt><code>relation { ... }</code> <dd>java.util.Set</dd>
     * <dt><code>array</code> <dd>java.util.ArrayList</dd>
     * <dt>types with declared possible representations
     * <dd>{@link net.sf.duro.PossrepObject}</dd>
     * </dl>
     * 
     * @param code		The code to execute
     * @throws DException	If a Duro error occurs.
     * @throws	IllegalStateException	If the code could not be executed.
     * 
     */
    public void execute(String code) throws DException;

    /**
     * Evaluates a Duro D/T expression.
     * 
     * <p>Duro types map to Java classes as follows:
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
     * @param	expr		The expression to evaluate
     * @throws	DException	If a Duro error occurs.
     * @throws	IllegalStateException	If the expression could not be evaluated.
     * 
     */
    public Object evaluate(String expr) throws DException;

    /**
     * Assigns a value to a variable.
     * @param name	The name of the variable
     * @param v		The value
     * @throws DException	If a Duro error occurs.
     * @throws java.lang.IllegalArgumentException	If v does not match the
     * 		type of the variable.
     */
    public void setVar(String name, Object v) throws DException;

    /**
     * Closes the session.
     * @throws DException	If a Duro error occurs
     */
    public void close() throws DException;
}
