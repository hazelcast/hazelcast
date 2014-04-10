package com.hazelcast.nio.serialization;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SerializationContextImplTest extends HazelcastTestSupport {

    @Test
    public void testCompression() throws IOException {
        String s = generateString(100000);

        byte[] bytes = s.getBytes();
        ByteArrayObjectDataOutput compressedDataOutput = new ByteArrayObjectDataOutput(s.length() * 5, null);
        SerializationContextImpl.compress(bytes, compressedDataOutput);

        ByteArrayObjectDataOutput decompressedDataOutput = new ByteArrayObjectDataOutput(s.length() * 5, null);
        SerializationContextImpl.decompress(compressedDataOutput.toByteArray(), decompressedDataOutput);

        String found = new String(decompressedDataOutput.toByteArray());
        assertEquals(s, found);
    }

    private String generateString(int i) {
        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        for (int k = 0; k < i; k++) {
            char c = (char) (random.nextInt(26) + 'a');
            sb.append(c);
        }
        return sb.toString();
    }
}
