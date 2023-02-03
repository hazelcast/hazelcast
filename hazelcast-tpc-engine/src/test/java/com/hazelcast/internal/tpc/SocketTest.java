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

package com.hazelcast.internal.tpc;

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
        MockSocket socket = new MockSocket();

        assertNull(socket.getCloseCause());
        assertNull(socket.getCloseCause());
        assertFalse(socket.isClosed());
    }

    @Test
    public void test_setCloseListener_whenAlreadySet() {
        MockSocket socket = new MockSocket();
        Socket.CloseListener oldCloseListener = mock(Socket.CloseListener.class);
        Executor oldExecutor = mock(Executor.class);
        socket.setCloseListener(oldCloseListener, oldExecutor);

        assertThrows(IllegalStateException.class, () -> socket.setCloseListener(mock(Socket.CloseListener.class), mock(Executor.class)));
    }

    @Test
    public void test_setCloseListener_whenListenerNull() {
        MockSocket socket = new MockSocket();

        assertThrows(NullPointerException.class, () -> socket.setCloseListener(null, mock(Executor.class)));
    }

    @Test
    public void test_setCloseListener_whenExecutorNull() {
        MockSocket socket = new MockSocket();

        assertThrows(NullPointerException.class, () -> socket.setCloseListener(mock(Socket.CloseListener.class), null));
    }

    @Test
    public void test_close_whenCloseListenerConfigured() {
        MockSocket socket = new MockSocket();
        Executor executor = command -> {
            command.run();
        };
        Socket.CloseListener listener = mock(Socket.CloseListener.class);
        socket.setCloseListener(listener, executor);

        socket.close();

        verify(listener).onClose(socket);
    }

    @Test
    public void test_close_whenCloseListenerConfiguredAndSocketAlreadyClosed() {
        MockSocket socket = new MockSocket();
        socket.close();

        Executor executor = command -> {
            command.run();
        };
        Socket.CloseListener listener = mock(Socket.CloseListener.class);
        socket.setCloseListener(listener, executor);

        verify(listener).onClose(socket);
    }

    @Test
    public void test_close_whenCloseListenerThrowsException_thenIgnore() {
        MockSocket socket = new MockSocket();
        Executor executor = command -> {
            command.run();
        };
        Socket.CloseListener listener = mock(Socket.CloseListener.class);
        socket.setCloseListener(listener, executor);

        doThrow(new RuntimeException()).when(listener).onClose(socket);
        socket.close();

        socket.close();
        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close_whenClose0ThrowsException_thenIgnore() throws IOException {
        MockSocket socket = new MockSocket();
        socket.exceptionToThrow = new IOException();

        socket.close();

        assertTrue(socket.isClosed());
    }

    @Test
    public void test_close() {
        MockSocket socket = new MockSocket();
        socket.close();

        assertTrue(socket.isClosed());
        assertNull(socket.getCloseCause());
        assertNull(socket.getCloseReason());
        assertEquals(1, socket.closeCalls);
    }

    @Test
    public void test_close_withReasonAndCause() {
        MockSocket socket = new MockSocket();
        String reason = "foo";
        Throwable cause = new Exception();
        socket.close(reason, cause);

        assertTrue(socket.isClosed());
        assertSame(cause, socket.getCloseCause());
        assertSame(reason, socket.getCloseReason());
        assertEquals(1, socket.closeCalls);
    }

    @Test
    public void test_close_withReasonOnly() {
        MockSocket socket = new MockSocket();
        String reason = "foo";
        socket.close(reason, null);

        assertTrue(socket.isClosed());
        assertNull(socket.getCloseCause());
        assertSame(reason, socket.getCloseReason());
        assertEquals(1, socket.closeCalls);
    }

    @Test
    public void test_close_withCauseOnly() {
        MockSocket socket = new MockSocket();
        Throwable cause = new Exception();
        socket.close(null, cause);

        assertTrue(socket.isClosed());
        assertNull(socket.getCloseReason());
        assertSame(cause, socket.getCloseCause());
        assertEquals(1, socket.closeCalls);
    }


    @Test
    public void test_close_whenAlreadyClosed() {
        MockSocket socket = new MockSocket();
        String reason = "foo";
        Throwable cause = new Exception();
        socket.close(reason, cause);

        socket.close();

        assertTrue(socket.isClosed());
        assertSame(cause, socket.getCloseCause());
        assertSame(reason, socket.getCloseReason());
        assertEquals(1, socket.closeCalls);
    }


    public static class MockSocket extends Socket {
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
