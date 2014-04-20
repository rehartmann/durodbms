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
	try {
            session.setVar("n", Double.valueOf(1.141));
	    fail("assignment of integer variable to float value was successful");
	} catch (IllegalArgumentException ex) {}
	session.setVar("n", Integer.valueOf(343));
	assertEquals(Integer.valueOf(343), session.evaluate("n"));	
    }

    @Test
    public void testString() throws DException {
	session.execute("var s string;");
	Tuple t = new Tuple();
	t.setAttribute("a", "ui");
	try {
	    session.setVar("s", t);
	    fail("assignment of string variable to tuple value was successful");
	} catch (IllegalArgumentException ex) {}
	session.setVar("s", "Crystal");
	assertEquals("Crystal", session.evaluate("s"));
    }

    @Test
    public void testBoolean() throws DException {
	session.execute("var b boolean;");
	try {
	    session.setVar("b", Integer.valueOf(1));
	    fail("assignment of boolean variable to integer value was successful");
	} catch (IllegalArgumentException ex) {}
	session.setVar("b", Boolean.TRUE);
	assertEquals(Boolean.TRUE, session.evaluate("b"));
    }

    @Test
    public void testFloat() throws DException {
	session.execute("var f float;");
	try {
	    session.setVar("f", new byte[] { (byte) 1, (byte) 0xf0 });
	    fail("assignment of float variable to binary value was successful");
	} catch (IllegalArgumentException ex) {}

        session.setVar("f", Double.valueOf(7.88));

	assertEquals(Double.valueOf(7.88), session.evaluate("f"));
    }

    @Test
    public void testBinary() throws DException {
	session.execute("var bin binary;");
	try {
	    session.setVar("bin", Boolean.FALSE);
	    fail("assignment of binary variable to boolean value was successful");
	} catch (IllegalArgumentException ex) {}
	session.setVar("bin", new byte[] { (byte) 1, (byte) 0xf0 });
	assertArrayEquals(new byte[] { (byte) 1, (byte) 0xf0 },
		(byte[]) session.evaluate("bin"));
    }

    @Test
    public void testTuple() throws DException {
	session.execute("var t tuple { a string };");

	Tuple t;	

	// Test missing attribute
	try {
            t = new Tuple();
	    session.setVar("t", t);
	    fail("assigning tuple variable succeeded despite missing attribute");
	} catch (IllegalArgumentException ex) {}

	// Test tuple with too many attributes
	try {
	    t = new Tuple();
	    t.setAttribute("a", "yoYo");
	    t.setAttribute("b", "yoYoo");
	    session.setVar("t", t);	
	    fail("assigning tuple variable succeeded despite additional attribute");
	} catch (IllegalArgumentException ex) {}

	// Test wrong attribute name
	try {
	    t = new Tuple();
	    t.setAttribute("b", "yoYo");
	    session.setVar("t", t);
	    fail("assigning tuple variable succeeded despite wrong attribute name");
	} catch (IllegalArgumentException ex) {}

	// Test wrong attribute name
	try {
	    t = new Tuple();
	    t.setAttribute("a", Integer.valueOf(1));
	    session.setVar("t", t);
	    fail("assigning tuple variable succeeded despite attribute type mismatch");
	} catch (DException ex) {}
	t = new Tuple();
	t.setAttribute("a", "yoYo");
	session.setVar("t", t);
	assertEquals(t, session.evaluate("t"));	
    }
}
