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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BufferingInputStreamTest {

    private final byte[] mockInput = {1, 2, 3, 4};

    private BufferingInputStream in;

    @Before
    public void setUp() {
        in = new BufferingInputStream(new ByteArrayInputStream(mockInput), 1 << 16);
    }

    @After
    public void tearDown() throws Exception {
        closeResource(in);
    }

    @Test
    public void readByteByByte() throws Exception {
        for (byte b : mockInput) {
            assertEquals(b, (byte) in.read());
        }
        assertEquals(-1, in.read());
    }

    @Test
    public void readBufByBuf() throws Exception {
        // given
        byte[] buf = new byte[mockInput.length / 2];
        int streamPos = 0;

        // when - then
        for (int count; (count = in.read(buf)) != -1; ) {
            for (int i = 0; i < count; i++) {
                assertEquals(mockInput[streamPos++], buf[i]);
            }
        }
        assertEquals(mockInput.length, streamPos);
    }
}
