package com.hazelcast.nio.serialization;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ByteArrayObjectDataOutputTest {

    private SerializationService mockSerializationService;

    @Before
    public void setUp() {
        mockSerializationService = mock(SerializationService.class);
    }

    @Test
    public void testWriteShort_explicitPosition() throws IOException {
        for (short s : new short[]{Short.MIN_VALUE, 0, Short.MAX_VALUE}) {
            ByteArrayObjectDataOutput dataOutput = new ByteArrayObjectDataOutput(2, mockSerializationService,
                    ByteOrder.BIG_ENDIAN);
            dataOutput.writeShort(0);
            dataOutput.writeShort(0, s);

            short actual = readShort(dataOutput.toByteArray());
            assertEquals(s, actual);
        }
    }

    @Test
    public void testWriteChars() throws IOException {
        String s = "fooo";

        ByteArrayObjectDataOutput dataOutput = new ByteArrayObjectDataOutput(2, mockSerializationService,
                ByteOrder.BIG_ENDIAN);
        dataOutput.writeChars(s);

        ByteArrayObjectDataInput dataInput = new ByteArrayObjectDataInput(dataOutput.toByteArray(),
                mockSerializationService, ByteOrder.BIG_ENDIAN);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            sb.append(dataInput.readChar());
        }
        assertEquals(s, sb.toString());
    }

    private short readShort(byte[] buffer) {
        int mostSig = buffer[0] & 0xff;
        int leastSig = buffer[1] & 0xff;
        return (short) ((mostSig << 8) + leastSig);
    }

}
