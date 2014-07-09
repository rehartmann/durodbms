package net.sf.duro;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TupleType extends Type {

    private Map<String, Type> attributeMap;

    public TupleType(VarDef[] attributes) {
	attributeMap = new HashMap<String, Type>();
        for (VarDef attr: attributes) {
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
