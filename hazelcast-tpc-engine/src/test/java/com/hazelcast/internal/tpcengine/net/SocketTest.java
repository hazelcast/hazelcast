/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.net;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SocketTest {

    @Test
    public void test_construction() {
        MockChannel channel = new MockChannel();

        assertNull(channel.getCloseCause());
        assertNull(channel.getCloseCause());
        assertFalse(channel.isClosed());
    }

    @Test
    public void test_setCloseListener_whenAlreadySet() {
        MockChannel channel = new MockChannel();
        AbstractAsyncSocket.CloseListener oldCloseListener = mock(AbstractAsyncSocket.CloseListener.class);
        Executor oldExecutor = mock(Executor.class);
        channel.setCloseListener(oldCloseListener, oldExecutor);

        assertThrows(IllegalStateException.class, () -> channel.setCloseListener(mock(AbstractAsyncSocket.CloseListener.class), mock(Executor.class)));
    }

    @Test
    public void test_setCloseListener_whenListenerNull() {
        MockChannel channel = new MockChannel();

        assertThrows(NullPointerException.class, () -> channel.setCloseListener(null, mock(Executor.class)));
    }

    @Test
    public void test_setCloseListener_whenExecutorNull() {
        MockChannel channel = new MockChannel();

        assertThrows(NullPointerException.class, () -> channel.setCloseListener(mock(AbstractAsyncSocket.CloseListener.class), null));
    }

    @Test
    public void test_close_whenCloseListenerConfigured() {
        MockChannel channel = new MockChannel();
        Executor executor = command -> {
            command.run();
        };
        AbstractAsyncSocket.CloseListener listener = mock(AbstractAsyncSocket.CloseListener.class);
        channel.setCloseListener(listener, executor);

        channel.close();

        verify(listener).onClose(channel);
    }

    @Test
    public void test_close_whenCloseListenerConfiguredAndchannelAlreadyClosed() {
        MockChannel channel = new MockChannel();
        channel.close();

        Executor executor = command -> {
            command.run();
        };
        AbstractAsyncSocket.CloseListener listener = mock(AbstractAsyncSocket.CloseListener.class);
        channel.setCloseListener(listener, executor);

        verify(listener).onClose(channel);
    }

    @Test
    public void test_close_whenCloseListenerThrowsException_thenIgnore() {
        MockChannel channel = new MockChannel();
        Executor executor = command -> {
            command.run();
        };
        AbstractAsyncSocket.CloseListener listener = mock(AbstractAsyncSocket.CloseListener.class);
        channel.setCloseListener(listener, executor);

        doThrow(new RuntimeException()).when(listener).onClose(channel);
        channel.close();

        channel.close();
        assertTrue(channel.isClosed());
    }

    @Test
    public void test_close_whenClose0ThrowsException_thenIgnore() throws IOException {
        MockChannel channel = new MockChannel();
        channel.exceptionToThrow = new IOException();

        channel.close();

        assertTrue(channel.isClosed());
    }

    @Test
    public void test_close() {
        MockChannel channel = new MockChannel();
        channel.close();

        assertTrue(channel.isClosed());
        assertNull(channel.getCloseCause());
        assertNull(channel.getCloseReason());
        assertEquals(1, channel.closeCalls);
    }

    @Test
    public void test_close_withReasonAndCause() {
        MockChannel channel = new MockChannel();
        String reason = "foo";
        Throwable cause = new Exception();
        channel.close(reason, cause);

        assertTrue(channel.isClosed());
        assertSame(cause, channel.getCloseCause());
        assertSame(reason, channel.getCloseReason());
        assertEquals(1, channel.closeCalls);
    }

    @Test
    public void test_close_withReasonOnly() {
        MockChannel channel = new MockChannel();
        String reason = "foo";
        channel.close(reason, null);

        assertTrue(channel.isClosed());
        assertNull(channel.getCloseCause());
        assertSame(reason, channel.getCloseReason());
        assertEquals(1, channel.closeCalls);
    }

    @Test
    public void test_close_withCauseOnly() {
        MockChannel channel = new MockChannel();
        Throwable cause = new Exception();
        channel.close(null, cause);

        assertTrue(channel.isClosed());
        assertNull(channel.getCloseReason());
        assertSame(cause, channel.getCloseCause());
        assertEquals(1, channel.closeCalls);
    }


    @Test
    public void test_close_whenAlreadyClosed() {
        MockChannel channel = new MockChannel();
        String reason = "foo";
        Throwable cause = new Exception();
        channel.close(reason, cause);

        channel.close();

        assertTrue(channel.isClosed());
        assertSame(cause, channel.getCloseCause());
        assertSame(reason, channel.getCloseReason());
        assertEquals(1, channel.closeCalls);
    }


    public static class MockChannel extends AbstractAsyncSocket {
        int closeCalls;
        IOException exceptionToThrow;

        @Override
        protected void close0() throws IOException {
            closeCalls++;
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
        }
    }
}
