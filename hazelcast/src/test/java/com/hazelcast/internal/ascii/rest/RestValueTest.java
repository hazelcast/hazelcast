/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})

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
