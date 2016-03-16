package com.hazelcast.instance;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for {@link GroupProperty} and {@link GroupProperties} classes.
 * <p/>
 * Need to run with {@link HazelcastSerialClassRunner} due to tests with System environment variables.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class GroupPropertiesTest {


    @Test(expected = NullPointerException.class)
    public void constructor_withNullConfig() {
        new GroupProperties(null);
    }
}
