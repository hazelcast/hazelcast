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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.impl.PartitionSpecificRunnable;

abstract class PartitionSpecificCallable<E> implements PartitionSpecificRunnable {

    private static final Object NO_RESPONSE = new Object() {
        @Override
        public String toString() {
            return "NO_RESPONSE";
        }
    };

    private int partitionId;
    private volatile E result;
    private volatile Throwable throwable;

    PartitionSpecificCallable() {
    }

    PartitionSpecificCallable(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void run() {
        try {
            result = call();
        } catch (Throwable t) {
            throwable = t;
        }
    }

    public E getResult() {
        if (throwable != null) {
            throw new RuntimeException("An error was encountered", throwable);
        }
        return result;
    }

    public Throwable getThrowable() {
        if (throwable == null) {
            throw new RuntimeException("Can't getThrowable if there is no throwable. Found value: " + result);
        }
        return throwable;
    }

    public boolean completed() {
        return result != NO_RESPONSE || throwable != null;
    }

    public abstract E call();
}
