package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
