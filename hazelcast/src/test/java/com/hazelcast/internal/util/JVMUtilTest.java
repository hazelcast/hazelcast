/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.util;

import com.hazelcast.internal.tpcengine.util.JVM;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assumptions.assumeThat;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Invokes all {@link JVMUtil} method to ensure no exception is thrown.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JVMUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(JVMUtil.class);
    }

    @Test
    public void testUsedMemory() {
        Assert.assertTrue(JVMUtil.usedMemory(Runtime.getRuntime()) > 0);
    }

    @Test
    public void testGetPid() {
        long legacyPidResult = getPidLegacy();

        assumeThat(legacyPidResult).isNotEqualTo(-1);
        assertEquals(legacyPidResult, JVMUtil.getPid());
    }

    @Test
    public void testObjectLayoutCompressedOopsMatchesHotSpotOption() {
        Boolean expected = JVMUtil.isHotSpotCompressedOopsOrNull();
        Boolean actual = JVMUtil.isObjectLayoutCompressedOopsOrNull();

        assumeThat(expected).isNotNull();
        assumeThat(actual).isNotNull();

        assertEquals(expected, actual);
    }

    @Test
    public void testIsCompressedOopsMatchesHotSpotOption() {
        Boolean compressedOops = JVMUtil.isHotSpotCompressedOopsOrNull();
        assumeThat(compressedOops).isNotNull();

        assertEquals(compressedOops.booleanValue(), JVMUtil.isCompressedOops());
    }

    @Test
    public void testIsCompressedClassPointersMatchesHotSpotOption() {
        Boolean compressedClassPointers = JVMUtil.isHotSpotCompressedClassPointersOrNull();
        assumeThat(compressedClassPointers).isNotNull();

        assertEquals(compressedClassPointers, JVMUtil.isCompressedClassPointers());
    }

    @Test
    public void testIsCompactObjectHeadersMatchesHotSpotOption() {
        Boolean compactObjectHeaders = JVMUtil.isHotSpotCompactObjectHeadersOrNull();
        assumeThat(compactObjectHeaders).isNotNull();

        assertEquals(compactObjectHeaders, JVMUtil.isCompactObjectHeaders());
    }

    @Test
    public void testObjectHeaderSizeMatchesObjectLayout() {
        Integer objectHeaderSize = JVMUtil.getObjectHeaderSizeOrNull();
        assumeThat(objectHeaderSize).isNotNull();

        assertEquals(JVMUtil.OBJECT_HEADER_SIZE, objectHeaderSize.intValue());
    }

    @Test
    public void testObjectHeaderSizeOn32BitJvm() {
        assumeThat(JVM.is32bit()).isTrue();

        assertEquals(8, JVMUtil.OBJECT_HEADER_SIZE);
    }

    @Test
    public void testObjectHeaderSizeWithCompactObjectHeaders() {
        assumeThat(JVMUtil.isCompactObjectHeaders()).isTrue();

        assertEquals(8, JVMUtil.OBJECT_HEADER_SIZE);
    }

    @Test
    public void testObjectHeaderSizeWithCompressedClassPointers() {
        assumeThat(JVM.is32bit()).isFalse();
        assumeThat(JVMUtil.isCompactObjectHeaders()).isFalse();
        assumeThat(JVMUtil.isCompressedClassPointers()).isTrue();

        assertEquals(12, JVMUtil.OBJECT_HEADER_SIZE);
    }

    @Test
    public void testObjectHeaderSizeWithUncompressedClassPointers() {
        assumeThat(JVM.is32bit()).isFalse();
        assumeThat(JVMUtil.isCompactObjectHeaders()).isFalse();
        assumeThat(JVMUtil.isCompressedClassPointers()).isFalse();

        assertEquals(16, JVMUtil.OBJECT_HEADER_SIZE);
    }

    /**
     * Returns the process ID. The algorithm does not guarantee it will be able
     * to get the correct process ID, in which case it returns {@code -1}.
     */
    private static long getPidLegacy() {
        String name = ManagementFactory.getRuntimeMXBean().getName();

        if (name == null) {
            return -1;
        }
        int separatorIndex = name.indexOf("@");
        if (separatorIndex < 0) {
            return -1;
        }
        String potentialPid = name.substring(0, separatorIndex);
        try {
            return Long.parseLong(potentialPid);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

}
