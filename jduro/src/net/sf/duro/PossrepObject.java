package net.sf.duro;

/**
 * A duro object of a scalar type with possible represenations.
 * 
 * @author Rene Hartmann
 *
 */
public interface PossrepObject {
    /**
     * Set a property of a PossrepObject
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
     * Get the value of a property.
     * 
     * @param name
     *            The property name.
     * @return The property value
     * @throws DException
     *             If a Duro error occurs
     */
    public Object getProperty(String name) throws DException;

    /**
     * Return the type name of a PossrepObject
     * 
     * @return the type name.
     */
    public String getTypeName();

    /**
     * Return the type of a PossrepObject
     * 
     * @return the type.
     */
    public ScalarType getType();

    /**
     * Release the resources the PossrepObject holds.
     * 
     * @throws DException
     */
    public void dispose() throws DException;
}
