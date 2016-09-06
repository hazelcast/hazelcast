package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JVMUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(JVMUtil.class);
    }

    // just invoke each JVMUtil static method to ensure no exception is thrown
    @Test
    public void testIsCompressedOops() {
        JVMUtil.isCompressedOops();
    }

    @Test
    public void testIsHotSpotCompressedOopsOrNull() {
        JVMUtil.isHotSpotCompressedOopsOrNull();
    }

    @Test
    public void testIsObjectLayoutCompressedOopsOrNull() {
        JVMUtil.isObjectLayoutCompressedOopsOrNull();
    }

    @Test
    public void testIs32bitJVM() {
        JVMUtil.is32bitJVM();
    }

    // Prints the size of object reference as calculated by JVMUtil.
    // When running under Hotspot 64-bit:
    // - JDK 6u23+ should report 4 (CompressedOops enabled by default)
    // - JDK 7 with -Xmx <= 32G or without any -Xmx specified should report 4 (CompressedOops enabled), otherwise 8
    // - explicitly starting with -XX:+UseCompressedOops should report 4, otherwise 8
    public static void main(String[] args) {
        System.out.println("Size of reference: " + JVMUtil.REFERENCE_COST_IN_BYTES);
    }
}
