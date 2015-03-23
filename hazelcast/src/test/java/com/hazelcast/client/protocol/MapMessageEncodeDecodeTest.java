package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.map.MapPutParameters;
import com.hazelcast.client.impl.protocol.util.ParameterFlyweight;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Encode Decode Tests
 */
public class MapMessageEncodeDecodeTest {


    private static final String NAME = "name";
    private static final byte[] DATA = new byte[]{(byte) 0x61, (byte) 0x62, (byte) 0x63};
    private static final long THE_LONG = 0xFFFFl;
    private static final boolean THE_BOOLEAN = true;

    private ParameterFlyweight flyweight = new ParameterFlyweight();
    private ByteBuffer byteBuffer;

    @Before
    public void setUp() {
        byteBuffer = ByteBuffer.allocate(1024);
        flyweight.wrap(byteBuffer);
    }

    @After
    public void tearDown() { }

    @Test
    public void shouldEncode_PUT() {
        MapPutParameters.encode(flyweight, NAME, DATA, DATA, THE_LONG, THE_LONG, THE_BOOLEAN);

        flyweight.index(0);
        final MapPutParameters putParameters = MapPutParameters.decode(flyweight);

        assertEquals(NAME, putParameters.name);
        assertArrayEquals(DATA, putParameters.key);
        assertArrayEquals(DATA, putParameters.value);
        assertEquals(THE_LONG, putParameters.threadId);
        assertEquals(THE_LONG, putParameters.ttl);
        assertEquals(THE_BOOLEAN, putParameters.async);


    }
}
