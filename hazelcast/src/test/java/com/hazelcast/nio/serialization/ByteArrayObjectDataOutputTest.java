package com.hazelcast.nio.serialization;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.IOException;

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
            ByteArrayObjectDataOutput dataOutput = new ByteArrayObjectDataOutput(2, mockSerializationService);
            dataOutput.writeShort(0);
            dataOutput.writeShort(0, s);

            short actual = readShort(dataOutput.toByteArray());
            assertEquals(s, actual);
        }
    }

    @Test
    public void testWriteChars() throws IOException {
        String s = "fooo";

        ByteArrayObjectDataOutput dataOutput = new ByteArrayObjectDataOutput(2, mockSerializationService);
        dataOutput.writeChars(s);

        ByteArrayObjectDataInput dataInput = new ByteArrayObjectDataInput(dataOutput.toByteArray(), mockSerializationService);
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
