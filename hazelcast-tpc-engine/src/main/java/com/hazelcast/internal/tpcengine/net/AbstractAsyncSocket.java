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

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * The socket for TPC engine that captures common functionality for the
 * {@link AsyncSocket} and {@link AsyncServerSocket}.
 */
public abstract class AbstractAsyncSocket implements Closeable {

    protected final ConcurrentMap<?, ?> context = new ConcurrentHashMap<>();

    protected final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    protected final AtomicReference<State> state = new AtomicReference<>(State.OPEN);
    private volatile String closeReason;
    private volatile Throwable closeCause;
    private CloseListener closeListener;
    private Executor closeExecutor;
    private boolean closeListenerChecked;

    /**
     * Allows for objects to be bound to this {@link AbstractAsyncSocket}. Useful for the lookup
     * of services and other dependencies.
     * <p/>
     * This method is thread-safe.
     */
    public final ConcurrentMap<?, ?> context() {
        return context;
    }

    /**
     * Configures the {@link CloseListener}.
     * <p/>
     * Can only be configured once.
     * <p/>
     * This call is threadsafe.
     * <p/>
     * If the method is called when the socket already is closed, the {@link CloseListener}
     * is notified.
     *
     * @param listener the close listener to set.
     * @param executor the executor used to execute the close listener.
     * @throws NullPointerException  if listener or executor is null.
     * @throws IllegalStateException if a close listener is already set.
     */
    public final void setCloseListener(CloseListener listener, Executor executor) {
        checkNotNull(executor, "executor");
        checkNotNull(listener, "listener");

        // Using lock to make sure that there is a matching listener/executor.
        boolean closeListenerChecked0;
        synchronized (this) {
            if (closeListener != null) {
                throw new IllegalStateException("Can't reset the closeListener");
            }

            this.closeExecutor = executor;
            this.closeListener = listener;
            closeListenerChecked0 = closeListenerChecked;
        }

        if (closeListenerChecked0) {
            // the closing thread already checked the closeListener and therefor
            // it hasn't seen the close listener that is now being set. So we need
            // to notify the close listener ourselves to prevent omitting the
            // notification.
            notifyCloseListener(listener, executor);
        }
    }

    /**
     * Checks if the socket is closed.
     * <p/>
     * This method is thread-safe.
     *
     * @return true if closed, false otherwise.
     */
    public final boolean isClosed() {
        return state.get() == State.CLOSED;
    }

    /**
     * Closes the socket with a <code>null</code> reason and cause.
     * <p/>
     * If the socket is already closed, the call is ignored.
     * <p/>
     * This method is thread-safe.
     * <p/>
     * This method doesn't throw an exception.
     */
    @Override
    public final void close() {
        close(null, null);
    }

    /**
     * Closes the socket.
     * <p/>
     * If the socket is already closed, the call is ignored.
     * <p/>
     * This method is thread-safe.
     *
     * @param reason the reason this socket is going to be closed.
     *               Is allowed to be <code>null</code>.
     * @param cause  the Throwable that caused this socket to be closed.
     *               Is allowed to be <code>null</code>.
     */
    @SuppressWarnings("java:S3776")
    public final void close(String reason, Throwable cause) {
        if (!state.compareAndSet(State.OPEN, State.CLOSING)) {
            return;
        }

        this.closeReason = reason;
        this.closeCause = cause;

        if (cause == null) {
            if (logger.isInfoEnabled()) {
                if (reason == null) {
                    logger.info("Closing  " + this);
                } else {
                    logger.info("Closing " + this + " due to " + reason);
                }
            }
        } else {
            if (logger.isWarningEnabled()) {
                if (reason == null) {
                    logger.warning("Closing  " + this, cause);
                } else {
                    logger.warning("Closing " + this + " due to " + reason, cause);
                }
            }
        }

        try {
            close0();
        } catch (Exception e) {
            logger.warning(e);
        } finally {
            state.set(State.CLOSED);
        }

        CloseListener closeListener0;
        Executor closeExecutor0;
        synchronized (this) {
            closeListenerChecked = true;
            closeListener0 = closeListener;
            closeExecutor0 = closeExecutor;
            // this will signal to a different thread calling the setCloseListener that
            // the socket is closed but the thread calling the close will not check any
            // change to the closeListener after this point.
        }

        if (closeListener0 != null) {
            notifyCloseListener(closeListener0, closeExecutor0);
        }
    }

    private void notifyCloseListener(CloseListener closeListener, Executor closeExecutor) {
        closeExecutor.execute(() -> {
            try {
                closeListener.onClose(AbstractAsyncSocket.this);
            } catch (Exception e) {
                logger.warning(e);
            }
        });
    }

    /**
     * Does the actual closing. No guarantee is made on which thread this is called.
     * <p/>
     * Is guaranteed to be called at most once.
     *
     * @throws IOException if something goes wrong while closing the socket.
     */
    protected abstract void close0() throws IOException;

    /**
     * Gets the reason this socket was closed. Can be <code>null</code> if no reason
     * was given or if the socket is still active. It is purely meant for debugging to
     * shed some light on why sockets are closed.
     * <p>
     * This method is thread-safe and can be called at any moment.
     * <p>
     * If the socket is closed and no reason is available, it is very likely that the
     * close cause does contain the reason of closing.
     *
     * @return the reason this socket was closed.
     * @see #getCloseCause()
     * @see #close(String, Throwable)
     */
    public String getCloseReason() {
        return closeReason;
    }

    /**
     * Gets the cause this socket was closed. Can be <code>null</code> if no cause was
     * given or if the socket is still active. It is purely meant for debugging to shed
     * some light on why sockets are closed.
     * <p>
     * This method is thread-safe.
     *
     * @return the cause of closing this socket.
     * @see #getCloseReason() ()
     * @see #close(String, Throwable)
     */
    public Throwable getCloseCause() {
        return closeCause;
    }

    protected enum State {
        OPEN,
        CLOSING,
        CLOSED
    }

    /**
     * A Listener that allows you to listen to the socket closing.
     */
    public interface CloseListener {
        void onClose(AbstractAsyncSocket socket);
    }
}
