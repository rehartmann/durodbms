package net.sf.duro.tests;

import static org.junit.Assert.*;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.DuroDSession;
import net.sf.duro.Tuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEvaluate {

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
	/* !! If a DExcption is thrown, its msg property will not be displayed
	 * because tearDown() destroys the interpreter before the error is printed
	 */
	assertEquals(Integer.valueOf(4), session.evaluate("2 + 2"));
    }

    @Test
    public void testString() throws DException {
	assertEquals("Oxygen", session.evaluate("\'Oxy\' || \'gen\'"));
    }

    @Test
    public void testBoolean() throws DException {
	assertEquals(Boolean.TRUE, session.evaluate("true"));
    }

    @Test
    public void testFloat() throws DException {
	assertEquals(Double.valueOf(13.1d), session.evaluate("13.1"));	
    }

    @Test
    public void testBinary() throws DException {	
	assertArrayEquals(new byte[] { (byte) 0, (byte) 1, (byte) 0xfc },
		(byte[])session.evaluate("X'0001fc'"));	
    }

    @Test
    public void testTuple() throws DException {
	Tuple t = new Tuple();
	t.setAttribute("s", "Yo");

	assertEquals(t, session.evaluate("tuple { s 'Yo'}"));	
    }
    
}
