/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.instance.OutOfMemoryHandlerHelper;

/**
 * Handler for <code>OutOfMemoryError</code>.
 * <p>
 * When an <code>OutOfMemoryError</code> is caught by Hazelcast threads,
 * <code>OutOfMemoryHandler</code> is called for ALL <code>HazelcastInstance</code>s
 * known by the current JVM (actually ClassLoader).
 * </p>
 *
 * <p>
 * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called even if JVM throws
 * <tt>OutOfMemoryError</tt>
 * because the error may be thrown from an external (user) thread, so
 * Hazelcast may not be informed about <tt>OutOfMemoryError</tt>.
 * </p>
 *
 * @see OutOfMemoryError
 * @see Hazelcast#setOutOfMemoryHandler(OutOfMemoryHandler)
 *
 */
public abstract class OutOfMemoryHandler {

    /**
     * When an <code>OutOfMemoryError</code> is caught by Hazelcast threads,
     * this method is called for ALL <code>HazelcastInstance</code>s
     * knows by current JVM (actually ClassLoader).
     *
     * <p>
     * User can shutdown the <tt>HazelcastInstance</tt>, call <code>System.exit()</code>,
     * just log the error, etc.
     * The default handler tries to close socket connections to other nodes and shutdown the
     * <tt>HazelcastInstance</tt>.
     * </p>
     *
     * <p>
     * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called even if JVM throws
     * <tt>OutOfMemoryError</tt>
     * because the error may be thrown from an external (user) thread
     * and Hazelcast may not be informed about <tt>OutOfMemoryError</tt>.
     * </p>
     *
     * @see OutOfMemoryHandler#inactivate(HazelcastInstance)
     * @see OutOfMemoryHandler#tryCloseConnections(HazelcastInstance)
     * @see OutOfMemoryHandler#tryStopThreads(HazelcastInstance)
     * @see OutOfMemoryHandler#tryShutdown(HazelcastInstance)
     *
     * @param oome OutOfMemoryError thrown by JVM
     * @param hazelcastInstances All HazelcastInstances known by JVM,
     *                           can include inactive or NULL instances.
     */
    public abstract void onOutOfMemory(OutOfMemoryError oome, HazelcastInstance[] hazelcastInstances);

    /**
     * Decides if the OutOfMemoryError thrown will be handled by this OutOfMemoryHandler or not.
     * If <tt>true</tt> is return, {@link #onOutOfMemory(OutOfMemoryError, HazelcastInstance[])} will be called
     * to handle error, otherwise OutOfMemoryError will be ignored.
     *
     * @param oome OutOfMemoryError thrown by JVM
     * @return true if OutOfMemoryError will be handled, false otherwise
     */
    public boolean shouldHandle(OutOfMemoryError oome) {
        return true;
    }

    /**
     * Inactivates <tt>HazelcastInstance</tt>; leaves threads, the connections are untouched.
     *
     * @param hazelcastInstance the Hazelcast instance to inactivate
     */
    protected final void inactivate(final HazelcastInstance hazelcastInstance) {
        OutOfMemoryHandlerHelper.inactivate(hazelcastInstance);
    }

    /**
     * Tries to close the server socket and connections to other <tt>HazelcastInstance</tt>s.
     *
     * @param hazelcastInstance the Hazelcast instance to close server socket
     */
    protected final void tryCloseConnections(HazelcastInstance hazelcastInstance) {
        OutOfMemoryHandlerHelper.tryCloseConnections(hazelcastInstance);
    }

    /**
     * Tries to stop internal Hazelcast threads (such as service thread, IO threads, executor threads).
     *
     * @param hazelcastInstance the Hazelcast instance to stop internal threads
     */
    protected final void tryStopThreads(final HazelcastInstance hazelcastInstance) {
        OutOfMemoryHandlerHelper.tryStopThreads(hazelcastInstance);
    }

    /**
     * Tries to shutdown <tt>HazelcastInstance</tt> forcefully;
     * including closing sockets and connections, stopping threads, etc.
     *
     * @param hazelcastInstance the Hazelcast instance to shutdown
     */
    protected final void tryShutdown(final HazelcastInstance hazelcastInstance) {
        OutOfMemoryHandlerHelper.tryShutdown(hazelcastInstance);
    }
}
