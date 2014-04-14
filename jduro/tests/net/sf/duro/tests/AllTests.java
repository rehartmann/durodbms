package net.sf.duro.tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestEvaluate.class, TestSetVar.class, TestExecute.class })
public class AllTests {

}
