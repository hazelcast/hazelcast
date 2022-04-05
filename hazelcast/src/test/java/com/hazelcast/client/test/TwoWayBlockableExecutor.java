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

package com.hazelcast.client.test;

import com.hazelcast.logging.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Utility class to help mitigate temporary unresponsive node or client
 * <p>
 * TwoWayBlockableExecutor acts like network between client and network
 * <p>
 * IncomingMessages is carrying messages coming from server to   client
 * OutgoingMessages is carrying messages going  to   server from client
 * <p>
 * with block* and unblock* methods one can stop/continue all messages from outside at any moment
 */
class TwoWayBlockableExecutor {

    static class LockPair {

        ReadWriteLock incomingLock;
        ReadWriteLock outgoingLock;

        LockPair(ReadWriteLock incomingLock, ReadWriteLock outgoingLock) {
            this.incomingLock = incomingLock;
            this.outgoingLock = outgoingLock;
        }

        void blockIncoming() {
            incomingLock.writeLock().lock();
        }

        void unblockIncoming() {
            incomingLock.writeLock().unlock();
        }

        void blockOutgoing() {
            outgoingLock.writeLock().lock();
        }

        void unblockOutgoing() {
            outgoingLock.writeLock().unlock();
        }

    }

    private final ExecutorService incomingMessages = Executors.newSingleThreadExecutor();
    private final ExecutorService outgoingMessages = Executors.newSingleThreadExecutor();
    private final LockPair lockPair;

    TwoWayBlockableExecutor(LockPair lockPair) {
        this.lockPair = lockPair;
    }

    void shutdownIncoming() {
        incomingMessages.shutdown();
    }

    void shutdownOutgoing() {
        outgoingMessages.shutdown();
    }

    class BlockableRunnable implements Runnable {

        private final Runnable runnable;
        private final Lock lock;

        BlockableRunnable(Runnable runnable, Lock lock) {
            this.runnable = runnable;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                lock.lockInterruptibly();
                try {
                    runnable.run();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void executeIncoming(Runnable runnable) {
        try {
            incomingMessages.execute(new BlockableRunnable(runnable, lockPair.incomingLock.readLock()));
        } catch (RejectedExecutionException rejected) {
            Logger.getLogger(getClass()).warning("Dropping incoming runnable since other end closed. " + runnable);
        }
    }

    void executeOutgoing(Runnable runnable) {
        try {
            outgoingMessages.execute(new BlockableRunnable(runnable, lockPair.outgoingLock.readLock()));
        } catch (RejectedExecutionException rejected) {
            Logger.getLogger(getClass()).warning("Dropping outgoing runnable since other end closed. " + runnable);
        }
    }
}
