package net.sf.duro;

import java.net.URL;

import net.sf.duro.rest.RESTSession;

/**
 * The central class used by Java applications to access DuroDBMS.
 *
 * @author Rene Hartmann
 *
 */
public abstract class DSession {

    protected DSession() { }
    
    /**
     * Creates a local DSession.
     * 
     * @return the DSession created.
     * @throws DException
     *             if a Duro error occurs
     */
    static public DSession createSession() {
        DSession instance = new LocalSession();
        return instance;
    }

    /**
     * Creates a remote DSession.
     * 
     * The URL must be as follows: <protocol>:[port]//database
     * 
     * @return the DSession created.
     * @throws DException
     */
    static public DSession createSession(URL url) {
        return new RESTSession(url);
    }

    /**
     * Closes the session.
     * 
     * @throws DException
     *             If a Duro error occurs
     */
    public void close() {
        // Do nothing
    }

    /**
     * Executes Duro D/T code.
     * 
     * <p>
     * User-defined operators implemented in Java can be created by using the
     * OPERATOR statement with the EXTERN keyword.
     * 
     * <p>
     * Creating a user-defined read-only operator implemented by a Java method:
     * 
     * <p>
     * <code>OPERATOR &lt;opname&gt;(&lt;parameter_list&gt;) RETURNS &lt;type&gt;<br />
     * EXTERN 'Java' '&lt;classname&gt;.&lt;methodname&gt;';<br />
     * END OPERATOR;</code>
     *
     * <p>
     * The method must be static. Parameter types and return type map to Java
     * types as described in {@link net.sf.duro.DSession#evaluate(String)
     * evaluate}.
     * 
     * <p>
     * Creating a user-defined update operator implemented by a Java method:
     * 
     * <p>
     * <code>OPERATOR &lt;opname&gt;(&lt;parameter_list&gt;) UPDATES { &lt;update_param_list&gt; }<br />
     * EXTERN 'Java' '&lt;classname&gt;.&lt;methodname&gt;';<br />
     * END OPERATOR;</code>
     *
     * <p>
     * The method must be static. Non-update parameter types map to Java types
     * as described in {@link net.sf.duro.DSession#evaluate(String) evaluate}.
     * Update parameter types map to Java classes as follows:
     * 
     * <dl>
     * <dt><code>boolean</code>
     * <dd>{@link net.sf.duro.UpdatableBoolean}</dd>
     * <dt><code>string</code>
     * <dd>java.lang.StringBuilder</dd>
     * <dt><code>integer</code>
     * <dd>{@link net.sf.duro.UpdatableInteger}</dd>
     * <dt><code>float</code>
     * <dd>{@link net.sf.duro.UpdatableDouble}</dd>
     * <dt><code>binary</code>
     * <dd>{@link net.sf.duro.ByteArray}</dd>
     * <dt><code>tuple { ... }</code>
     * <dd>{@link net.sf.duro.Tuple}</dd>
     * <dt><code>relation { ... }</code>
     * <dd>java.util.Set</dd>
     * <dt><code>array</code>
     * <dd>java.util.ArrayList</dd>
     * <dt>types with declared possible representations
     * <dd>{@link net.sf.duro.PossrepObject}</dd>
     * </dl>
     * 
     * @param code
     *            The code to execute
     * @throws DException
     *             If the code could not be executed.
     * 
     */
    public abstract void execute(String code);

    /**
     * Evaluates a Duro D/T expression.
     * 
     * <p>
     * Duro types map to Java classes as follows:
     * 
     * <dl>
     * <dt><code>boolean</code>
     * <dd><code>java.lang.Boolean</code>
     * <dt><code>string</code>
     * <dd><code>java.lang.String</code>
     * <dt><code>integer</code>
     * <dd><code>java.lang.Integer</code>
     * <dt><code>float</code>
     * <dd><code>java.lang.Double</code>
     * <dt><code>binary</code>
     * <dd><code>byte[]</code>
     * <dt><code>tuple { ... }</code>
     * <dd>{@link net.sf.duro.Tuple}</dd>
     * <dt><code>relation { ... }</code>
     * <dd><code>java.util.Set</code>
     * <dt><code>array</code>
     * <dd>Duro arrays map to Java arrays. For example,
     * <code>array integer</code> is converted to
     * <code>java.lang.Integer[]</code>.</dd>
     * <dt>types with declared possible representations
     * <dd><code>{@link net.sf.duro.PossrepObject}</code>
     * </dl>
     * 
     * @param expr
     *            The expression to evaluate
     * @return an object representing the result value.
     * @throws DException
     *             If the expression could not be evaluated.
     * 
     */
    public abstract Object evaluate(String expr);

    /**
     * Evaluates the expression <code>expr</code> and stores the
     * value in <code>dest</code>.
     * The type of <code>expr</code> must have possreps and <code>dest</code>
     * must have a setter for each property.
     * 
     * @param expr      the expression to evaluate
     * @param dest      the destination object
     * @throws DException
     */
    public abstract void evaluate(String expr, Object dest);

    /**
     * Evaluates expr, stores the result in an instance of class <code>destClass</code>
     * and returns that instance.
     * If <code>destClass</code> is an interface, a dynamic proxy is created.
     * Otherwise, the default constructor of <code>destClass</code> is called.
     * @param expr the expression to evaluate
     * @param destClass the class or interface the returned object is an instance of. 
     * @return an object of class <code>destClass</code> representing the result value.
     * @throws DException
     */
    public abstract <T> T evaluate(String expr, Class<T> destClass);

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
    public abstract void setVar(String name, Object v);
}
