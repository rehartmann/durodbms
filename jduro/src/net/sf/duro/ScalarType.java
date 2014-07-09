package net.sf.duro;

import java.util.HashMap;
import java.util.Map;

public class ScalarType extends Type {
    private String name;
    private Possrep[] possreps;
    private static Map<String, ScalarType> typeMap = new HashMap<String, ScalarType>();

    public static ScalarType BOOLEAN = new ScalarType("boolean", null);
    public static ScalarType INTEGER = new ScalarType("integer", null);
    public static ScalarType STRING = new ScalarType("string", null);
    public static ScalarType FLOAT = new ScalarType("float", null);
    public static ScalarType BINARY = new ScalarType("binary", null);

    static {
	typeMap.put("boolean", BOOLEAN);
	typeMap.put("integer", INTEGER);
	typeMap.put("string", STRING);
	typeMap.put("float", FLOAT);
	typeMap.put("binary", BINARY);
    }
    
    protected ScalarType(String name, Possrep[] possreps) {
	this.name = name;
	this.possreps = possreps;
    }

    private static native Possrep[] typePossreps(String name,
	    DuroDSession session);

    @Override
    public String getName() {
	return name;
    }

    @Override
    public boolean isScalar() {
	return true;
    }

    public Possrep[] getPossreps() {
	return possreps;
    }

    public static ScalarType fromString(String typename, DSession session) {
	ScalarType type = typeMap.get(typename);
	if (type == null) {
	    Possrep[] possreps = typePossreps(typename, (DuroDSession) session);
	    if (possreps == null)
		return null;
	    
	    type = new ScalarType(typename, possreps);
	    typeMap.put(typename, type);
	}
	return type;
    }
}
