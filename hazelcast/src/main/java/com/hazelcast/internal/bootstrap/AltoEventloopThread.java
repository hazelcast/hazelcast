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

package com.hazelcast.internal.bootstrap;


import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThreadImpl;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the TPC version of the {@link PartitionOperationThreadImpl}.
 */
public class AltoEventloopThread extends HazelcastManagedThread implements PartitionOperationThread {

    private final static AtomicInteger THREAD_ID = new AtomicInteger();

    private final int threadId;

    public AltoEventloopThread(Runnable target) {
        super(target);
        this.threadId = THREAD_ID.incrementAndGet();
    }

    @Override
    public int getThreadId() {
        return threadId;
    }
}
