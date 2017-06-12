package com.hazelcast.internal.cluster.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembersViewMetadataTest {

    @Test
    public void equalsAndHashCode() throws Exception {
        final MembersViewMetadata metadata
                = new MembersViewMetadata(new Address("localhost", 1234), "memberUUID", new Address("localhost", 4321), 0);

        assertEqualAndHashCode(metadata, metadata);
        assertNotEquals(metadata, null);
        assertNotEquals(metadata, "");
        assertEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), "memberUUID", new Address("localhost", 4321), 0));

        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 999), "memberUUID", new Address("localhost", 4321), 0));
        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), "memberUUID999", new Address("localhost", 4321), 0));
        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), "memberUUID", new Address("localhost", 999), 0));
        assertNotEqualAndHashCode(
                metadata,
                new MembersViewMetadata(new Address("localhost", 1234), "memberUUID", new Address("localhost", 4321), 999));
    }

    private static void assertEqualAndHashCode(Object o1, Object o2) {
        assertEquals(o1, o2);
        assertEquals(o1.hashCode(), o2.hashCode());
    }

    private static void assertNotEqualAndHashCode(Object o1, Object o2) {
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
    }
}
