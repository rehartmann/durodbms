package net.sf.duro;

public interface DSession extends AutoCloseable {
    public void execute(String s) throws DException;

    public Object evaluate(String expr) throws DException;

    public Object setVar(String name, Object v) throws DException;

    public void close() throws DException;
}
