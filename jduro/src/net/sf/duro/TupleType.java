package net.sf.duro;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Instances of this class represent a DuroDBMS tuple type.
 * 
 * @author Rene Hartmann
 *
 */
public class TupleType extends Type {

    private static final long serialVersionUID = 1L;

    private Map<String, Type> attributeMap;

    public TupleType(NameTypePair[] attributes) {
        attributeMap = new HashMap<String, Type>();
        for (NameTypePair attr : attributes) {
            attributeMap.put(attr.getName(), attr.getType());
        }
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    public Set<String> attributeNames() {
        return attributeMap.keySet();
    }
}
