package com.hazelcast.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.StringUtil.timeToString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class StringUtilTest extends HazelcastTestSupport {

    @Test
    public void testTimeToString(){
        assertEquals("2016-04-13 12:34:55.914", timeToString(1460540095914l));
    }
}
