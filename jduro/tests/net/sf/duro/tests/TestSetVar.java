package net.sf.duro.tests;

import static org.junit.Assert.*;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.DuroDSession;
import net.sf.duro.Tuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSetVar {

    private DSession session;

    @Before
    public void setUp() throws Exception {
	session = DuroDSession.createSession();
    }

    @After
    public void tearDown() throws Exception {
	session.close();
    }

    @Test
    public void testInteger() throws DException {
	session.execute("var n int;");
	session.setVar("n", Integer.valueOf(343));
	assertEquals(Integer.valueOf(343), session.evaluate("n"));	
    }

    @Test
    public void testString() throws DException {
	session.execute("var s string;");
	session.setVar("s", "Crystal");
	assertEquals("Crystal", session.evaluate("s"));
    }

    @Test
    public void testBoolean() throws DException {
	session.execute("var b boolean;");
	session.setVar("b", Boolean.TRUE);
	assertEquals(Boolean.TRUE, session.evaluate("b"));
    }

    @Test
    public void testFloat() throws DException {
	session.execute("var f float;");
	session.setVar("f", Double.valueOf(7.8));
	assertEquals(Double.valueOf(7.8), session.evaluate("f"));
    }

    @Test
    public void testTuple() throws DException {
	session.execute("var t tuple { a string };");

	Tuple t = new Tuple();
	t.setAttribute("a", "Casablanca");
	session.setVar("t", t);
	assertEquals(t, session.evaluate("t"));	
    }

}
