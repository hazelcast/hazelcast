package com.hazelcast.spi.partitiongroup;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class PartitionGroupMetaDataTest extends HazelcastTestSupport {

    @Test
    public void testEnum() {
        assertEnumCoverage(PartitionGroupMetaData.class);
    }
}
