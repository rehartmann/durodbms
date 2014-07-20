package net.sf.duro.tests;

import static org.junit.Assert.*;

import java.io.File;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.DuroDSession;
import net.sf.duro.Tuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUserDefOp {

    private DSession session;

    // Implements the user-defined operator test1
    @SuppressWarnings("unused")
    private static String test1() {
	return "Hello";
    }

    // Implements the user-defined operator test2
    @SuppressWarnings("unused")
    private static String test2(String arg1, Integer arg2, Double arg3,
	    Tuple arg4) {
	return arg1 + arg2 + arg3 + arg4.getAttribute("a");
    }

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

    @Test
    public void testReadonlyOp0Params() throws DException {
        session.execute("begin tx;");

        session.execute("operator test1() returns string extern 'Java'"
        	+ " 'net.sf.duro.tests.TestUserDefOp.test1';"
        	+ " end operator;");
        assertEquals("Hello", session.evaluate("test1()"));

        session.execute("commit;");
    }

    @Test
    public void testReadonlyOp4Params() throws DException {
        session.execute("begin tx;");

        session.execute("operator test2(p1 string, p2 int, p3 float," +
        		"p4 tuple { a string })"
        	+ "returns string extern 'Java'"
        	+ " 'net.sf.duro.tests.TestUserDefOp.test2';"
        	+ " end operator;");
        assertEquals("yo24.2x",
        	session.evaluate("test2('yo', 2, 4.2, tuple { a 'x' })"));

        session.execute("commit;");
    }
}
