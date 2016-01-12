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
