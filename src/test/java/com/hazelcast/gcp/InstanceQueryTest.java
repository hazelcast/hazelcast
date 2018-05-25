package com.hazelcast.gcp;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class InstanceQueryTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public InstanceQueryTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( InstanceQueryTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testInstanceQuery() throws java.io.IOException,java.security.GeneralSecurityException
    {
        assertTrue( true );
	InstanceQuery instanceQuery = new InstanceQuery();
	String[] args = new String[2];
	args[0] = "hazelcast-33";
	args[1] = "us-east1-b";
	instanceQuery.main(args);
    }
}
