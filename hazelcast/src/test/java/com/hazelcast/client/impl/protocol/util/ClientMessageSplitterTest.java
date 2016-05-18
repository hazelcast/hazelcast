package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.client.impl.protocol.util.ClientMessageSplitter.getNumberOfSubFrames;
import static com.hazelcast.client.impl.protocol.util.ClientMessageSplitter.getSubFrame;
import static com.hazelcast.client.impl.protocol.util.ClientMessageSplitter.getSubFrames;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMessageSplitterTest extends HazelcastTestSupport {

    private ClientMessage clientMessage;

    @Before
    public void setUp() throws Exception {
        SafeBuffer byteBuffer = new SafeBuffer(new byte[1024]);
        clientMessage = ClientMessage.createForEncode(byteBuffer, 0);
        clientMessage.setFrameLength(1024);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientMessageSplitter.class);
    }

    @Test
    @RequireAssertEnabled
    public void testGetSubFrames() {
        List<ClientMessage> frames = getSubFrames(128, clientMessage);

        assertEquals(10, frames.size());
    }

    @Test
    @RequireAssertEnabled
    public void testGetSubFrame_whenFrameSizeGreaterThanFrameLength_thenReturnOriginalMessage() {
        List<ClientMessage> frame = getSubFrames(1025, clientMessage);

        assertEquals(1, frame.size());
        assertEquals(clientMessage, frame.get(0));
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetSubFrames_whenInvalidFrameSize_thenThrowAssertionError() {
        getSubFrames(ClientMessage.HEADER_SIZE - 1, clientMessage);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetNumberOfSubFrames_whenInvalidFrameSize_thenThrowAssertionError() {
        getNumberOfSubFrames(ClientMessage.HEADER_SIZE - 1, clientMessage);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetSubFrame_whenNegativeFrameIndex_thenThrowAssertionError() {
        getSubFrame(ClientMessage.HEADER_SIZE + 1, -1, 10, clientMessage);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetSubFrame_whenFrameIndexGreaterNumberOfFrames_thenThrowAssertionError() {
        getSubFrame(ClientMessage.HEADER_SIZE + 1, 10, 5, clientMessage);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetSubFrame_whenInvalidFrameSize_thenThrowAssertionError() {
        getSubFrame(ClientMessage.HEADER_SIZE - 1, 0, 5, clientMessage);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testGetSubFrame_whenFrameSizeGreaterThanFrameLength_withInvalidFrameIndex_thenThrowAssertionError() {
        getSubFrame(1025, 1, 5, clientMessage);
    }
}
