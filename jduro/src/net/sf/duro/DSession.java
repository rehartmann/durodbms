package net.sf.duro;

import java.lang.reflect.Method;

/**
 * The central class used by Java applications to access DuroDBMS.
 *
 * @author Rene Hartmann
 *
 */
public class DSession {
    static {
        System.loadLibrary("jduro");
    }

    private long interp = 0L; // Contains the pointer to the Duro_interp
                              // structure

    private DSession() { } // Constructor is private

    native private void initInterp();

    native private void destroyInterp();

    native private void executeI(String s)
            throws ClassNotFoundException, NoSuchMethodException;

    native private Object evaluateI(String expr)
            throws ClassNotFoundException, NoSuchMethodException;

    native private void setVarI(String name, Object v);
    
    native private void implementTypeI(String typename);

    native private void createSelector(String prname, String typename, String classname);

    native private void createGetter(String opname, String typename, String possrepName,
            int compno, String method);
    
    native private void createSetter(String opname, String typename, String possrepName,
            int compno, String method);

    /**
     * Creates a DSession.
     * 
     * @return the DSession created.
     * @throws DException
     *             if a Duro error occurs
     */
    static public DSession createSession() {
        DSession instance = new DSession();
        synchronized (DSession.class) {
            instance.initInterp();
        }
        return instance;
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
     *             If a Duro error occurs.
     * @throws IllegalStateException
     *             If the code could not be executed.
     * 
     */
    public void execute(String code) {
        try {
            synchronized (DSession.class) {
                executeI(code);
            }
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException(ex);
        } catch (NoSuchMethodException ex) {
            throw new IllegalStateException(ex);
        }
    }

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
     * @throws DException
     *             If a Duro error occurs.
     * @throws IllegalStateException
     *             If the expression could not be evaluated.
     * 
     */
    public Object evaluate(String expr) {
        try {
            synchronized (DSession.class) {
                return evaluateI(expr);
            }
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException(ex);
        } catch (NoSuchMethodException ex) {
            throw new IllegalStateException(ex);
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
        synchronized (DSession.class) {
            setVarI(name, v);
        }
    }

    private static Class<?> getJavaType(Type type) {
        String typename = type.getName();
        if (typename.equals("integer"))
            return int.class;
        if (typename.equals("string"))
            return String.class;
        if (typename.equals("float"))
            return double.class;
        if (typename.equals("boolean"))
            return boolean.class;
        return null;
    }

    private String typeSignature(Class<?> rtype) {
        if (rtype == int.class) {
            return "I";
        }
        if (rtype == String.class) {
            return 'L' + String.class.getName().replace('.',  '/') + ';'; 
        }
        if (rtype == double.class) {
            return "D";
        }
        if (rtype == boolean.class) {
            return "Z";
        }
        return null;
    }

    public void implementType(ScalarType type, Class<?> implementorClass) {
        // Check if a default constructor exists
        try {
            implementorClass.getDeclaredConstructor();
        } catch(NoSuchMethodException e) {
            throw new IllegalArgumentException("class" + implementorClass.getName()
                    +" must have a default constructor", e);
        }

        Possrep[] possreps = type.getPossreps();
        synchronized (DSession.class) {
            for (int i = 0; i < possreps.length; i++) {
                createSelector(possreps[i].getName(), type.getName(),
                        implementorClass.getName().replace('.',  '/'));
                for (int j = 0; j < possreps[i].getComponents().length; j++) {
                    Method method;
                    String comp = possreps[i].getComponent(j).getName();
                    Class<?> jtype = getJavaType(possreps[i].getComponent(j).getType());
                    
                    String methodName = "get"
                            + comp.substring(0, 1).toUpperCase() + comp.substring(1);
                    try {
                        method = implementorClass.getMethod(methodName);
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("Getter method for property "
                                + comp + " not found", e);
                    }
                    if (jtype == null) {
                        throw new IllegalArgumentException("unsupported component type "
                                + possreps[i].getComponent(j).getType().getName());
                    }
                    if (method.getReturnType() != jtype) {
                        throw new IllegalArgumentException("getter return type does not match type of component "
                                + possreps[i].getComponent(j).getType().getName());
                    }                    
                    createGetter(type.getName() + "_get_" + comp, type.getName(),
                            possreps[i].getName(), j,
                            implementorClass.getName().replace('.',  '/') + "." + method.getName()
                                    + "()" + typeSignature(jtype));

                    methodName = "set" + comp.substring(0, 1).toUpperCase() + comp.substring(1);
                    try {
                        method = implementorClass.getMethod(methodName, jtype);
                    } catch (NoSuchMethodException e) {
                        throw new IllegalArgumentException("Getter method for property "
                                + comp + " not found", e);
                    }
                    
                    createSetter(type.getName() + "_set_" + comp, type.getName(),
                            possreps[i].getName(), j,
                            implementorClass.getName().replace('.',  '/') + "." + methodName
                                    + '(' + typeSignature(jtype) + ")V");
                }
            }

            implementTypeI(type.getName());
        }
    }
}
