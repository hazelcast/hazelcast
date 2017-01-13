package com.hazelcast.internal.ascii.rest;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})

public class RestValueTest extends HazelcastTestSupport {

    private static final byte[] PAYLOAD = new byte[]{23, 42};

    private RestValue restValue = new RestValue();

    @Test
    public void testSetContentType() {
        restValue.setContentType(PAYLOAD);

        assertEquals(PAYLOAD, restValue.getContentType());
        assertContains(restValue.toString(), "contentType='" + bytesToString(PAYLOAD));
    }

    @Test
    public void testSetValue() {
        restValue.setValue(PAYLOAD);

        assertEquals(PAYLOAD, restValue.getValue());
        assertContains(restValue.toString(), "value.length=" + PAYLOAD.length);
    }

    @Test
    public void testToString() {
        assertContains(restValue.toString(), "unknown-content-type");
        assertContains(restValue.toString(), "value.length=0");
    }

    @Test
    public void testToString_withText() {
        byte[] value = stringToBytes("foobar");
        byte[] contentType = stringToBytes("text");

        restValue = new RestValue(value, contentType);

        assertContains(restValue.toString(), "contentType='text'");
        assertContains(restValue.toString(), "value=\"foobar\"");
    }
}
