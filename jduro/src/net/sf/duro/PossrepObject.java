package net.sf.duro;

public interface PossrepObject {
    /**
     * Set a property of a PossrepObject 
     * @param name The property name
     * @param value The new property value
     * @throws DException If a Duro error occurs
     */
    public void setProperty(String name, Object value) throws DException;

    /**
     * Ge the value of a property.
     * @param name The property name.
     * @return The property value
     * @throws DException If a Duro error occurs
     */
    public Object getProperty(String name) throws DException;

    /**
     * Get the type name of a PossrepObject
     * @return the type name, or NULL if an error occured.
     */
    public String getTypeName();

    /**
     * Release the resources the PossrepObject holds.
     * @throws DException
     */
    public void dispose() throws DException;
}
