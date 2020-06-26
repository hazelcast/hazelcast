/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import java.util.concurrent.CountDownLatch;

public class BlockingExec extends AbstractExec {

    private final Exec exec;
    private final Blocker blocker;

    public BlockingExec(Exec exec, Blocker blocker) {
        super(exec.getId());

        this.exec = exec;
        this.blocker = blocker;
    }

    @Override
    protected void setup0(QueryFragmentContext ctx) {
        exec.setup(ctx);
    }

    @Override
    protected IterationResult advance0() {
        blocker.awaitUnblock();

        return exec.advance();
    }

    @Override
    protected RowBatch currentBatch0() {
        return exec.currentBatch();
    }

    public static class Blocker {

        private final CountDownLatch awaitLatch = new CountDownLatch(1);
        private final CountDownLatch latch = new CountDownLatch(1);

        public void awaitReached() {
            try {
                awaitLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }
        }

        private void awaitUnblock() {
            awaitLatch.countDown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }
        }

        public void unblock() {
            latch.countDown();
        }

        public void unblockAfter(long timeout) {
            Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(timeout);

                    unblock();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new RuntimeException(e);
                }
            });

            thread.setDaemon(true);
            thread.start();
        }
    }
}
