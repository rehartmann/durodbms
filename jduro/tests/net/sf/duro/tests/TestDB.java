package net.sf.duro.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.Tuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDB {

    private DSession session;

    @Before
    public void setUp() throws DException {
        session = DSession.createSession();
        session.execute("create_env('dbenv');"
                + "create_db('D');");
    }

    @After
    public void tearDown() throws DException {
        session.close();

        // Delete environment directory
        File envdir = new File("dbenv");
        for (File f : envdir.listFiles()) {
            f.delete();
        }
        envdir.delete();
    }

    @Test
    public void test() throws DException {
        session.execute("connect('dbenv');"
                + "current_db := 'D';"
                + "begin tx;"
                + "var emp real rel {id int, name string, m_salary float} key {id};"
                + "var x_emp virtual extend emp : { y_salary := m_salary * 12.0 };"
                + "insert emp tup {id 1, name 'John Smith', m_salary 3000.0 };"
                + "commit;");
        session.execute("begin tx;");
        Tuple t = (Tuple) session.evaluate("TUPLE FROM x_emp");
        session.execute("commit;");

        Tuple xt = new Tuple();
        xt.setAttribute("id", Integer.valueOf(1));
        xt.setAttribute("name", "John Smith");
        xt.setAttribute("m_salary", Double.valueOf(3000.0));
        xt.setAttribute("y_salary", Double.valueOf(36000.0));

        assertEquals(xt, t);
    }

}
