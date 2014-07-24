package net.sf.duro.tests;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Set;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.DuroDSession;
import net.sf.duro.Tuple;
import net.sf.duro.UpdatableBoolean;
import net.sf.duro.UpdatableDouble;
import net.sf.duro.UpdatableInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUserDefOp {

    private DSession session;

    @Before
    public void setUp() throws Exception {
	session = DuroDSession.createSession();

	session.execute("create_env('dbenv');"
		      + "create_db('D');"
		      + "current_db := 'D';");
    }

    @After
    public void tearDown() throws Exception {
	session.close();

	// Delete environment directory
	File envdir = new File("dbenv");
	for (File f: envdir.listFiles()) {
	    f.delete();
	}
	envdir.delete();
    }

    // Implements the user-defined operator test1
    @SuppressWarnings("unused")
    private static String test1() {
	return "Hello";
    }

    @Test
    public void testReadonlyOp0Params() throws DException {
        session.execute("begin tx;");

        session.execute("operator test1() returns string extern 'Java'"
        	+ " 'net.sf.duro.tests.TestUserDefOp.test1';"
        	+ " end operator;");
        assertEquals("Hello", session.evaluate("test1()"));

        session.execute("commit;");
    }

    // Implements the user-defined operator test2
    @SuppressWarnings("unused")
    private static String test2(String arg1, Integer arg2, Double arg3,
	    Boolean arg4, byte[] arg5, Tuple arg6, Set arg7, String[] arg8) {
	return arg1 + arg2 + arg3 + arg4 + (int) arg5[0] + (int) arg5[1]
		+ arg6.getAttribute("s")
		+ ((Tuple) arg7.toArray()[0]).getAttribute("n")
		+ arg8[0];
    }

    @Test
    public void testReadonlyOp8Params() throws DException {
        session.execute("begin tx;");

        session.execute("operator test2(p1 string, p2 int, p3 float,"
        		+ "p4 boolean, p5 binary, p6 tuple { s string }, "
        		+ "p7 rel { n int }, p8 array string)"
        	+ " returns string extern 'Java'"
        	+ " 'net.sf.duro.tests.TestUserDefOp.test2';"
        	+ " end operator;");
        try {
        assertEquals("yo24.2true15x33tt",
        	session.evaluate("test2('yo', 2, 4.2, true, X'0105',"
        		+ " tuple { s 'x' }, rel { tup { n 33 } }, array('tt'))"));
        } catch (DException ex) {
            ex.printStackTrace();
            throw ex;
        }
        session.execute("commit;");
    }

    // Implements the user-defined operator test4u
    @SuppressWarnings("unused")
    private static void test8u(StringBuilder au, String a,
	    UpdatableInteger bu, Integer b, UpdatableBoolean cu, Boolean c,
	    UpdatableDouble du, Double d) {
	au.append(a);
	bu.setValue(bu.intValue() + b.intValue());
	cu.setValue(c.booleanValue());
	du.setValue(du.doubleValue() + d.doubleValue());
    }

    @Test
    public void testUpdateOp8Params() throws DException {
        session.execute("begin tx;");

        session.execute("operator test8u "
                + "(au string, a string, bu int, b int, cu bool, c bool, du float, d float)"
        	+ " updates { au, bu, cu, du }"
        	+ " extern 'Java' 'net.sf.duro.tests.TestUserDefOp.test8u';"
        	+ " end operator;");
        try {
            session.execute("var au init 'Hello';"
        	    + "var bu init 5;"
        	    + "var cu init false;"
        	    + "var du init 2.0;"
        	    + "test8u(au, 'Goodbye', bu, 14, cu, true, du, 3.0);");
        } catch (DException ex) {
            ex.printStackTrace();
            throw ex;
        }
        assertEquals("HelloGoodbye", session.evaluate("au"));
        assertEquals(19, session.evaluate("bu"));
        assertEquals(true, session.evaluate("cu"));
        assertEquals(5.0, session.evaluate("du"));

        session.execute("commit;");
    }
}
