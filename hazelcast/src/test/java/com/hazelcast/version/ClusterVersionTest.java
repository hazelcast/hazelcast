package com.hazelcast.version;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterVersionTest {

    @Test
    public void constituents() throws Exception {
        ClusterVersion version = ClusterVersion.of(3, 9);
        assertEquals(3, version.getMajor());
        assertEquals(9, version.getMinor());
    }

    @Test(expected = IllegalArgumentException.class)
    public void ofMalformed() throws Exception {
        ClusterVersion.of("3,9");
    }

    @Test
    public void equals() throws Exception {
        assertEquals(ClusterVersion.UNKNOWN, ClusterVersion.UNKNOWN);
        assertEquals(ClusterVersion.of(3, 9), ClusterVersion.of(3, 9));
        assertEquals(ClusterVersion.of(3, 9), ClusterVersion.of("3.9"));

        assertFalse(ClusterVersion.of("3.8").equals(ClusterVersion.of(4, 0)));
        assertFalse(ClusterVersion.UNKNOWN.equals(ClusterVersion.of(4, 1)));

        assertFalse(ClusterVersion.UNKNOWN.equals(new Object()));
    }

    @Test
    public void compareTo() throws Exception {
        assertEquals(0, ClusterVersion.of(3, 9).compareTo(ClusterVersion.of(3, 9)));
        assertEquals(1, ClusterVersion.of(3, 10).compareTo(ClusterVersion.of(3, 9)));
        assertEquals(1, ClusterVersion.of(4, 0).compareTo(ClusterVersion.of(3, 9)));
        assertEquals(-1, ClusterVersion.of(3, 9).compareTo(ClusterVersion.of(3, 10)));
        assertEquals(-1, ClusterVersion.of(3, 9).compareTo(ClusterVersion.of(4, 10)));
    }

    @Test
    public void hashCodeTest() throws Exception {
        assertEquals(ClusterVersion.UNKNOWN.hashCode(), ClusterVersion.UNKNOWN.hashCode());

        assertTrue(ClusterVersion.UNKNOWN.hashCode() != ClusterVersion.of(3, 4).hashCode());
    }

    @Test
    public void testSerialization() {
        ClusterVersion given = ClusterVersion.of(3, 9);
        SerializationServiceV1 ss = new DefaultSerializationServiceBuilder().setVersion(SerializationServiceV1.VERSION_1).build();
        ClusterVersion deserialized = ss.toObject(ss.toData(given));

        assertEquals(deserialized, given);
    }

    @Test
    public void toStringTest() throws Exception {
        assertEquals("3.8", ClusterVersion.of(3, 8).toString());
    }

}