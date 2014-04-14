package net.sf.duro;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Tuple implements Serializable {

    private static final long serialVersionUID = -6556763635542844612L;

    private Map<String, Object> map;

    public Tuple() {
	map = new HashMap<String, Object>();
    }

    public Object getAttribute(String name) {
	return map.get(name);
    }

    public void setAttribute(String name, Object value) {
	map.put(name, value);
    }

    public Set<String> attributeNames() {
	return map.keySet();
    }

    public boolean isEmpty() {
	return map.isEmpty();
    }

    public int size() {
	return map.size();
    }

    public String toString() {
        StringBuffer buf = new StringBuffer("TUPLE { ");
        Iterator<String> it = attributeNames().iterator();
        while(it.hasNext()) {
            String key = it.next();
            buf.append(key);
            buf.append(' ');
            buf.append(map.get(key));
            buf.append(' ');
        }
        buf.append('}');
        return buf.toString();
    }

    public boolean equals(Object o) {
	Tuple t;
	try {
	    t = (Tuple) o;
	} catch (ClassCastException e) {
	    return false;
	}

	// Tuples must be of same size
	if (map.size() != t.map.size())
	    return false;

	// Compare attributes
        for (String k: attributeNames()) {
            if (!getAttribute(k).equals(t.getAttribute(k)))
        	return false;
        }
        return true;
    }
    
    public int hashCode() {
	int hc = 0;

	for (Object o: map.values()) {
            hc += o.hashCode();
        }
        return hc;
    }
}

