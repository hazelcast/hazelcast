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
import org.junit.internal.ComparisonCriteria;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

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

        // Use a custom array elements equality comparison due to Java 9 changes in StackTraceElement class
        new StackTraceElementComparisonCriteria().arrayEquals(null, stackTrace, stackTrace2);
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

    /**
     * Helper class to assert 2 StackTraceElement arrays are equals, where the equality is only based on fields available in
     * Java 6 (and also 7, 8). The reason for using this helper class is the fact Java 9 adds several new fields to the
     * {@link StackTraceElement} class and Hazelcast encoder/decoder works just with the ones from Java 6.
     */
    public static class StackTraceElementComparisonCriteria extends ComparisonCriteria {
        @Override
        protected void assertElementsEqual(Object expected, Object actual) {
            if (expected instanceof StackTraceElement && actual instanceof StackTraceElement) {
                StackTraceElement st1 = (StackTraceElement) expected;
                StackTraceElement st2 = (StackTraceElement) actual;
                assertEquals("ClassNames have to be equal", st1.getClassName(), st2.getClassName());
                assertEquals("LineNumbers have to be equal", st1.getLineNumber(), st2.getLineNumber());
                assertEquals("MethodNames have to be equal", st1.getMethodName(), st2.getMethodName());
                assertEquals("FileNames have to be equal", st1.getFileName(), st2.getFileName());
            } else {
                assertEquals(expected, actual);
            }
        }
    }
}
