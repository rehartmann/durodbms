package net.sf.duro;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Instances of this class represent a DuroDBMS tuple.
 * 
 * @author Rene Hartmann
 *
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = -6556763635542844612L;

    private Map<String, Object> map;

    /**
     * Constructs an empty tuple.
     */
    public Tuple() {
        map = new HashMap<String, Object>();
    }

    /**
     * Returns the value of attribute <var>name</var>.
     * 
     * @param name
     *            The attribute name.
     * @return The attribute value.
     */
    public Object getAttribute(String name) {
        return map.get(name);
    }

    /**
     * Set the attribute <var>name</var>.
     * 
     * @param name
     *            The attribute name. Must not be null.
     * @param value
     *            The attribute value. Must not be null.
     */
    public void setAttribute(String name, Object value) {
        if (name == null || value == null) {
            // null is not permitted
            throw new NullPointerException();
        }
        map.put(name, value);
    }

    /**
     * Returns a Set of all attribute names of this Tuple.
     * 
     * @return a Set of all attribute names of this Tuple.
     */
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
        Object val;
        StringBuffer buf = new StringBuffer("TUPLE { ");
        Iterator<String> it = attributeNames().iterator();
        while (it.hasNext()) {
            String key = it.next();
            buf.append(key);
            buf.append(' ');
            val = map.get(key);

            // Put the value in quotes if it's a string
            if (val instanceof String) {
                buf.append('\'');
            }
            buf.append(val);
            if (val instanceof String) {
                buf.append('\'');
            }
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
        for (String k : attributeNames()) {
            if (!getAttribute(k).equals(t.getAttribute(k)))
                return false;
        }
        return true;
    }

    public int hashCode() {
        int hc = 0;

        for (Object o : map.values()) {
            hc += o.hashCode();
        }
        return hc;
    }
}
