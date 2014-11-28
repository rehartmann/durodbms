package net.sf.duro;

/**
 * A duro object of a scalar type with possible representations.
 * 
 * @author Rene Hartmann
 *
 */
public interface PossrepObject {
    /**
     * Sets a property of a PossrepObject
     * 
     * @param name
     *            The property name
     * @param value
     *            The new property value
     * @throws DException
     *             If a Duro error occurs
     */
    public void setProperty(String name, Object value) throws DException;

    /**
     * Gets the value of a property.
     * 
     * @param name
     *            The property name.
     * @return The property value
     * @throws DException
     *             If a Duro error occurs.
     */
    public Object getProperty(String name) throws DException;

    /**
     * Returns the type name of a PossrepObject
     * 
     * @return the type name.
     */
    public String getTypeName();

    /**
     * Returns the type of a PossrepObject
     * 
     * @return the type.
     */
    public ScalarType getType();

    /**
     * Releases the resources the PossrepObject instance holds.
     * 
     * @throws DException       If a Duro error occurs.
     */
    public void dispose() throws DException;
}
