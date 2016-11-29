package com.hazelcast.internal.cluster.impl;

import com.hazelcast.nio.Version;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VersionsTest {

    @Test
    public void version_3_8() throws Exception {
        assertEquals(Version.of(8), Versions.V3_8);
    }

}