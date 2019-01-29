/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertArrayEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class StackTraceElementCodecTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(StackTraceElementCodec.class);
    }

    @Test
    public void testEncodeDecodeStackTrace() {
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        testEncodeDecodeStackTrace(stackTrace);
    }

    private void testEncodeDecodeStackTrace(StackTraceElement[] stackTrace) {
        int size = 0;
        for (StackTraceElement stackTraceElement : stackTrace) {
            size += StackTraceElementCodec.calculateDataSize(stackTraceElement);
        }

        ClientMessage clientMessage = ClientMessage.createForEncode(size);
        for (StackTraceElement stackTraceElement : stackTrace) {
            StackTraceElementCodec.encode(stackTraceElement, clientMessage);
        }

        ClientMessage clientMessage2 = ClientMessage.createForDecode(clientMessage.buffer(), 0);
        StackTraceElement[] stackTrace2 = new StackTraceElement[stackTrace.length];

        for (int i = 0; i < stackTrace.length; i++) {
            stackTrace2[i] = StackTraceElementCodec.decode(clientMessage2);
        }

        assertArrayEquals(stackTrace, stackTrace2);
    }

    @Test
    public void testEncodeDecodeStackTraceWithNullFileName() {
        StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        stackTrace = nullifyFileNames(stackTrace);

        testEncodeDecodeStackTrace(stackTrace);
    }

    private StackTraceElement[] nullifyFileNames(StackTraceElement[] stackTrace) {
        StackTraceElement[] stackTraceWithNullFileName = new Throwable().getStackTrace();
        int i = 0;
        for (StackTraceElement stackTraceElement : stackTrace) {
            stackTraceWithNullFileName[i++] = new StackTraceElement(stackTraceElement.getClassName(),
                    stackTraceElement.getMethodName(), null, stackTraceElement.getLineNumber());
        }
        return stackTraceWithNullFileName;
    }
}
