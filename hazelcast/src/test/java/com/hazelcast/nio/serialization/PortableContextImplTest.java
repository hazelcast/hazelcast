package com.hazelcast.nio.serialization;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PortableContextImplTest extends HazelcastTestSupport {

    @Test
    public void testCompression() throws IOException {
        String s = HazelcastTestSupport.generateRandomString(100000);

        byte[] bytes = s.getBytes();
        ByteArrayObjectDataOutput compressedDataOutput = new ByteArrayObjectDataOutput(s.length() * 5, null);
        PortableContextImpl.compress(bytes, compressedDataOutput);

        ByteArrayObjectDataOutput decompressedDataOutput = new ByteArrayObjectDataOutput(s.length() * 5, null);
        PortableContextImpl.decompress(compressedDataOutput.toByteArray(), decompressedDataOutput);

        String found = new String(decompressedDataOutput.toByteArray());
        assertEquals(s, found);
    }

}
