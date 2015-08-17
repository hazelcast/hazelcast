package com.hazelcast.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class VersionCheckTest extends HazelcastTestSupport {


    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hz1;

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance();
    }

    @Test
    public void testVersionCheckParameters() throws Exception {

        Node node1 = TestUtil.getNode(hz1);
        VersionCheck check = new VersionCheck();
        Map<String, String> parameters = check.doCheck(node1, "test_version", false);

        assertEquals(parameters.get("version"), "test_version");
        assertEquals(parameters.get("m"), node1.getLocalMember().getUuid() );
        assertEquals(parameters.get("e"), "false");
        assertEquals(parameters.get("l"), "NULL");
        assertEquals(parameters.get("p"), "source");
        assertEquals(parameters.get("crsz"), "A");
        assertEquals(parameters.get("cssz"), "A");
        assertEquals(parameters.get("hdgb"), "0");
        assertEquals(parameters.get("ccpp"), "0");
        assertEquals(parameters.get("cdn"), "0");
        assertEquals(parameters.get("cjv"), "0");
        assertFalse(Integer.parseInt(parameters.get("cuptm")) < 0);
        assertNotEquals(parameters.get("nuptm"), "0");
        assertNotEquals(parameters.get("nuptm"), parameters.get("cuptm"));
    }

    @Test
    public void testConvertToLetter() throws Exception {

        VersionCheck check = new VersionCheck();
        assertEquals("A", check.convertToLetter(4));
        assertEquals("B", check.convertToLetter(9));
        assertEquals("C", check.convertToLetter(19));
        assertEquals("D", check.convertToLetter(39));
        assertEquals("E", check.convertToLetter(59));
        assertEquals("F", check.convertToLetter(99));
        assertEquals("G", check.convertToLetter(149));
        assertEquals("H", check.convertToLetter(299));
        assertEquals("J", check.convertToLetter(599));
        assertEquals("I", check.convertToLetter(1000));
    }
}
