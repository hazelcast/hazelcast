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

package com.hazelcast.internal.networking;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static com.hazelcast.nio.IOUtil.closeResource;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractChannelTest {

    private SocketChannel socketChannel;
    private TestChannel channel;

    @Before
    public void setUp() throws Exception {
        socketChannel = SocketChannel.open();
        channel = new TestChannel(socketChannel, false);
    }

    @After
    public void tearDown() {
        closeResource(channel);
        closeResource(socketChannel);
    }

    @Test
    public void testClose_whenCalledTwice_thenCloseIsSuccessful() throws Exception {
        channel.close();
        channel.close();

        assertTrue(channel.isClosed());
    }

    @Test
    public void testClose_whenExceptionIsThrownOnClose_thenCloseIsSuccessful() throws Exception {
        channel.throwExceptionOnClose = true;

        channel.close();

        assertTrue(channel.isClosed());
    }

    @Test
    public void testClose_whenExceptionIsThrownOnListener_thenCloseIsSuccessful() throws Exception {
        channel.addCloseListener(new TestChannelCloseListener());

        channel.close();

        assertTrue(channel.isClosed());
    }

    private static class TestChannel extends AbstractChannel {

        private boolean throwExceptionOnClose;

        TestChannel(SocketChannel socketChannel, boolean clientMode) {
            super(socketChannel, clientMode);
        }

        @Override
        protected void onClose() throws IOException {
            super.onClose();
            if (throwExceptionOnClose) {
                throw new IOException("Expected exception");
            }
        }

        @Override
        public long lastReadTimeMillis() {
            return 0;
        }

        @Override
        public long lastWriteTimeMillis() {
            return 0;
        }

        @Override
        public boolean write(OutboundFrame frame) {
            return false;
        }

        @Override
        public void flush() {
        }
    }

    private static class TestChannelCloseListener implements ChannelCloseListener {

        @Override
        public void onClose(Channel channel) {
            throw new IllegalStateException("Expected exception");
        }
    }
}
