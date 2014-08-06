package net.sf.duro;

import java.io.Serializable;

/**
 * This class is the abstract superclass of classes whose instances
 * represent a DuroDBMS type.
 * @author Rene Hartmann
 *
 */
public abstract class Type implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Returns the name of a type.
     * @return the type name, or null if the type has no name.
     */
    public abstract String getName();

    /**
     * Determines if the type is scalar. 
     * @return true if the type is scalar, false if it is nonscalar.
     */
    public abstract boolean isScalar();
}
