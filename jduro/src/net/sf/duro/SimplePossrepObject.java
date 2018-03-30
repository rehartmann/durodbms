package net.sf.duro;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of PossrepObject in pure Java.
 * Instances of this class represent a value of a type with a single possible representation.
 * 
 * @author Rene Hartmann
 *
 */
public class SimplePossrepObject implements PossrepObject {
    private Map<String, Object> valueMap;
    private ScalarType type;

    public SimplePossrepObject(String typeName) {
        type = ScalarType.fromString(typeName);
        if (type == null) {
            type = new ScalarType(typeName, null);
        }
        valueMap = new HashMap<String, Object>();
    }

    @Override
    public void setProperty(String name, Object value) throws DException {
        valueMap.put(name, value);
    }

    @Override
    public Object getProperty(String name) throws DException {
        return valueMap.get(name);
    }

    @Override
    public String getTypeName() {
        if (type == null) {
            return null;
        }
        return type.getName();
    }

    /**
     * Determines if two PossrepObjects are equal using the DuroDBMS library
     * function RDB_obj_equals().
     */
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        
        PossrepObject probj;
        try {
            probj = (PossrepObject) obj;
        } catch (ClassCastException ex) {
            return false;
        }

        for (String propName: valueMap.keySet()) {
            if (!valueMap.get(propName).equals(probj.getProperty(propName)))
                return false;
        }
        return true;
    }

    /**
     * Returns a hash code for this PossrepObject, based on its property values.
     * 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        /* Use components of 1st possrep to calculate the hash code */
        int code = 0;
        for (String propName: valueMap.keySet()) {
            try {
                code += getProperty(propName).hashCode();
            } catch (DException ex) {
                throw new UnsupportedOperationException(ex);
            }
        }
        return code;
    }

    @Override
    public ScalarType getType() {
        return type;
    }

    @Override
    public void dispose() throws DException { }
}
