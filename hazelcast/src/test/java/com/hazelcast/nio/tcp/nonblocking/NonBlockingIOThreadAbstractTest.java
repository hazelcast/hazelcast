package com.hazelcast.nio.tcp.nonblocking;

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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the NonBlockingIOThread. A lot of the internal are tested using a mock-selector. This gives us full
 * control on testing the edge cases which are extremely hard to realize with a real selector.
 */
public abstract class NonBlockingIOThreadAbstractTest extends HazelcastTestSupport {

    private NonBlockingIOThreadOutOfMemoryHandler oomeHandler;
    private ILogger logger;
    private MockSelector selector;
    private SelectionHandler handler;
    private NonBlockingIOThread thread;

    @Before
    public void setup() {
        logger = Logger.getLogger(NonBlockingIOThread.class);
        oomeHandler = mock(NonBlockingIOThreadOutOfMemoryHandler.class);
        selector = new MockSelector();
        handler = mock(SelectionHandler.class);
    }

    @After
    public void tearDown() {
        if (thread != null) {
            thread.shutdown();
        }
    }

    protected abstract boolean selectNow();

    private void startThread() {
        thread = new NonBlockingIOThread(null, "foo", logger, oomeHandler, selectNow(), selector);
        thread.start();
    }

    @Test
    public void whenValidSelectionKey_thenHandlerCalled() throws Exception {
        startThread();

        SelectionKey selectionKey = mock(SelectionKey.class);
        selectionKey.attach(handler);
        when(selectionKey.isValid()).thenReturn(true);

        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(handler).handle();
            }
        });
        assertEquals(1, thread.getEventCount());
        assertStillRunning();
    }

    @Test
    public void whenInvalidSelectionKey_thenHandlerOnFailureCalledWithCancelledKeyException() throws Exception {
        startThread();

        SelectionKey selectionKey = mock(SelectionKey.class);
        selectionKey.attach(handler);
        when(selectionKey.isValid()).thenReturn(false);
        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(handler).onFailure(isA(CancelledKeyException.class));
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
        doThrow(new ExpectedRuntimeException()).when(handler).handle();

        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(handler).onFailure(isA(ExpectedRuntimeException.class));
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
            public void run() throws Exception {
                assertFalse(thread.isAlive());
            }
        });

        verify(oomeHandler).handle(any(OutOfMemoryError.class));
    }

    @Test
    public void testToString() {
        startThread();
        assertEquals(thread.getName(), thread.toString());
    }

    public void assertStillRunning() {
        // we verify that the thread is still running by scheduling a selection-key event and checking if the
        // handler is being called.
        final SelectionHandler handler = mock(SelectionHandler.class);
        SelectionKey selectionKey = mock(SelectionKey.class);
        selectionKey.attach(handler);
        when(selectionKey.isValid()).thenReturn(true);

        selector.scheduleSelectAction(selectionKey);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(handler).handle();
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
        public int select() throws IOException {
            // not needed for the time being.
            throw new UnsupportedOperationException();
        }

        @Override
        public Selector wakeup() {
            actionQueue.add(new SelectorAction());
            return this;
        }

        @Override
        public void close() throws IOException {
        }
    }

    static class SelectorAction {
        final Set<SelectionKey> keys = new HashSet<SelectionKey>();
        boolean selectThrowsIOException = false;
        boolean selectThrowsOOME = false;
    }
}
