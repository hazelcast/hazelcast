/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the NioThread. A lot of the internal are tested using a mock-selector. This gives us full
 * control on testing the edge cases which are extremely hard to realize with a real selector.
 */
public abstract class NioThreadAbstractTest extends HazelcastTestSupport {

    private ChannelErrorHandler errorHandler;
    private ILogger logger;
    private MockSelector selector;
    private NioPipeline handler;
    NioThread thread;

    @Before
    public void setup() {
        logger = Logger.getLogger(NioThread.class);
        errorHandler = mock(ChannelErrorHandler.class);
        selector = new MockSelector();
        handler = mock(NioPipeline.class);
    }

    @After
    public void tearDown() {
        if (thread != null) {
            thread.shutdown();
        }
    }

    protected abstract SelectorMode selectorMode();

    /**
     * Subclasses that need to do some setup after the IO thread was created but
     * before starting it should override this method.
     */
    protected void beforeStartThread() {

    }

    private void startThread() {
        thread = new NioThread("foo", logger, errorHandler, selectorMode(), selector, null);
        beforeStartThread();
        thread.start();
    }

    @Test
    public void whenValidSelectionKey_thenHandlerCalled() {
        startThread();

        SelectionKey selectionKey = mock(SelectionKey.class);
        selectionKey.attach(handler);
        when(selectionKey.isValid()).thenReturn(true);

        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(handler).process();
            }
        });
        assertEquals(1, thread.getEventCount());
        assertStillRunning();
    }

    @Test
    public void whenInvalidSelectionKey_thenHandlerOnFailureCalledWithCancelledKeyException() {
        startThread();

        SelectionKey selectionKey = mock(SelectionKey.class);
        selectionKey.attach(handler);
        when(selectionKey.isValid()).thenReturn(false);
        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                verify(handler).onError(isA(CancelledKeyException.class));
            }
        });
        assertStillRunning();
    }

    @Test
    public void whenHandlerThrowException_thenHandlerOnFailureCalledWithThatException() throws Exception {
        startThread();

        SelectionKey selectionKey = mock(SelectionKey.class);
        selectionKey.attach(handler);
        when(selectionKey.isValid()).thenReturn(true);
        doThrow(new ExpectedRuntimeException()).when(handler).process();

        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                verify(handler).onError(isA(ExpectedRuntimeException.class));
            }
        });
        assertStillRunning();
    }

    @Test
    public void whenSelectThrowsIOException_thenKeepRunning() {
        startThread();

        selector.scheduleSelectThrowsIOException();

        assertStillRunning();
    }

    @Test
    public void whenSelectThrowsOOME_thenThreadTerminates() {
        startThread();

        selector.scheduleSelectThrowsOOME();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(thread.isAlive());
            }
        });

        verify(errorHandler).onError((Channel) isNull(), any(OutOfMemoryError.class));
    }

    @Test
    public void testToString() {
        startThread();
        assertEquals(thread.getName(), thread.toString());
    }

    public void assertStillRunning() {
        // we verify that the thread is still running by scheduling a selection-key event and checking if the
        // handler is being called.
        final NioPipeline handler = mock(NioPipeline.class);
        SelectionKey selectionKey = mock(SelectionKey.class);
        selectionKey.attach(handler);
        when(selectionKey.isValid()).thenReturn(true);

        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(handler).process();
            }
        });
    }

    class MockSelector extends Selector {
        final BlockingQueue<SelectorAction> actionQueue = new LinkedBlockingQueue<SelectorAction>();
        Set<SelectionKey> pendingKeys;

        void scheduleSelectAction(SelectionKey selectionKey) {
            SelectorAction selectorAction = new SelectorAction();
            selectorAction.keys.add(selectionKey);
            actionQueue.add(selectorAction);
        }

        void scheduleSelectThrowsIOException() {
            SelectorAction selectorAction = new SelectorAction();
            selectorAction.selectThrowsIOException = true;
            actionQueue.add(selectorAction);
        }

        void scheduleSelectThrowsOOME() {
            SelectorAction selectorAction = new SelectorAction();
            selectorAction.selectThrowsOOME = true;
            actionQueue.add(selectorAction);
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public SelectorProvider provider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<SelectionKey> keys() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<SelectionKey> selectedKeys() {
            if (pendingKeys == null) {
                throw new IllegalArgumentException();
            }
            return pendingKeys;
        }

        @Override
        public int selectNow() throws IOException {
            return select(0);
        }

        @Override
        public int select(long timeout) throws IOException {
            try {
                SelectorAction action = actionQueue.poll(timeout, TimeUnit.MILLISECONDS);
                if (action == null) {
                    // there was a timeout.
                    return 0;
                }

                if (action.selectThrowsIOException) {
                    throw new IOException();
                }

                if (action.selectThrowsOOME) {
                    throw new OutOfMemoryError();
                }

                pendingKeys = action.keys;
                return pendingKeys.size();
            } catch (InterruptedException e) {
                // should not happen, so lets propagate it.
                throw new RuntimeException(e);
            }
        }

        @Override
        public int select() {
            // not needed for the time being.
            throw new UnsupportedOperationException();
        }

        @Override
        public Selector wakeup() {
            actionQueue.add(new SelectorAction());
            return this;
        }

        @Override
        public void close() {
        }
    }

    static class SelectorAction {

        final Set<SelectionKey> keys = new HashSet<SelectionKey>();

        boolean selectThrowsIOException;
        boolean selectThrowsOOME;
    }
}
