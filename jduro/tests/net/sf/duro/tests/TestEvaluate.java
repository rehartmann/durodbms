package net.sf.duro.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import net.sf.duro.DException;
import net.sf.duro.DSession;
import net.sf.duro.Tuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEvaluate {

    private DSession session;

    @Before
    public void setUp() throws Exception {
        session = DSession.createSession();
    }

    @After
    public void tearDown() throws Exception {
        session.close();
    }

    @Test
    public void testInteger() {
        /*
         * !! If a DExcption is thrown, its msg property will not be displayed
         * because tearDown() destroys the interpreter before the error is
         * printed
         */
        assertEquals(Integer.valueOf(4), session.evaluate("2 + 2"));
    }

    @Test
    public void testString() {
        assertEquals("Oxygen", session.evaluate("\'Oxy\' || \'gen\'"));
    }

    @Test
    public void testBoolean() {
        assertEquals(Boolean.TRUE, session.evaluate("true"));
    }

    @Test
    public void testFloat() {
        assertEquals(Double.valueOf(13.1d), session.evaluate("13.1"));
    }

    @Test
    public void testBinary() {
        assertArrayEquals(new byte[] { (byte) 0, (byte) 1, (byte) 0xfc },
                (byte[]) session.evaluate("X'0001fc'"));
    }

    @Test
    public void testTuple() {
        Tuple t = new Tuple();
        t.setAttribute("s", "Yo");

        assertEquals(t, session.evaluate("tuple { s 'Yo'}"));
    }

    @Test
    public void testRelation() {
        Tuple t = new Tuple();
        t.setAttribute("s", "Yo");

        Set<Tuple> set = new HashSet<Tuple>();
        set.add(t);

        assertEquals(set, session.evaluate("relation { tuple { s 'Yo'} }"));
    }

    @Test
    public void testArray() {
        byte[][] barr = new byte[2][];
        barr[0] = new byte[] { (byte) 0x40, (byte) 0x41 };
        barr[1] = new byte[] { (byte) 0 };

        assertArrayEquals(barr,
                (byte[][]) session.evaluate("array(X'4041', X'00')"));
    }
}
