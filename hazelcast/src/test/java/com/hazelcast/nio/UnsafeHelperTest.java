package com.hazelcast.nio;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.UnsafeHelper.findUnsafeIfAllowed;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class UnsafeHelperTest extends HazelcastTestSupport {

    String unsafeMode;
    String arch;

    @Before
    public void setUp() throws Exception {
        unsafeMode = System.getProperty("hazelcast.unsafe.mode");
        arch = System.getProperty("os.arch");
    }

    @After
    public void tearDown() throws Exception {
        if (unsafeMode != null) {
            System.setProperty("hazelcast.unsafe.mode", unsafeMode);
        }
        if (arch != null) {
            System.setProperty("os.arch", arch);
        }
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(UnsafeHelper.class);
    }

    @Test
    public void testFindUnsafeIfAllowed() {
        assertNotNull(findUnsafeIfAllowed());
    }

    @Test
    public void testFindUnsafeIfAllowed_disabled() {
        System.setProperty("hazelcast.unsafe.mode", "disabled");
        assertNull(findUnsafeIfAllowed());
    }

    @Test
    public void testFindUnsafeIfAllowed_unalignedOS() {
        System.setProperty("os.arch", "unaligned");
        assertNull(findUnsafeIfAllowed());
    }

    @Test
    public void testFindUnsafeIfAllowed_unalignedOS_forced() {
        System.setProperty("os.arch", "unaligned");
        System.setProperty("hazelcast.unsafe.mode", "enforced");
        assertNotNull(findUnsafeIfAllowed());
    }
}
