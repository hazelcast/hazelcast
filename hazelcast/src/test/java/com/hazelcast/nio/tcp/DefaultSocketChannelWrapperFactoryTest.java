package com.hazelcast.nio.tcp;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultSocketChannelWrapperFactoryTest extends HazelcastTestSupport{

    private DefaultSocketChannelWrapperFactory factory;

    @Before
    public void setup(){
        factory = new DefaultSocketChannelWrapperFactory();
    }

    @Test
    public void wrapSocketChannel() throws Exception {
        SocketChannel socketChannel = mock(SocketChannel.class);
        SocketChannelWrapper wrapper = factory.wrapSocketChannel(socketChannel, false);

        assertInstanceOf(DefaultSocketChannelWrapper.class, wrapper);
    }

    @Test
    public void isSSlEnabled(){
        boolean result = factory.isSSlEnabled();
        assertFalse(result);
    }
}
