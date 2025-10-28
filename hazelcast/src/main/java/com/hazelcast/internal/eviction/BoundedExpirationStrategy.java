/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.eviction;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * An expiration strategy which iterates over some element container in a
 * bounded manner. If a full iteration could not be completed within the
 * limits the next call will start from the last
 *
 * @param <E> The element type
 */
public final class BoundedExpirationStrategy<E> {

    private final Consumer<? super E> tryExpire;
    private final Iterable<? extends E> elements;
    private final int elementLimitPerPoll;
    private final Duration pollDurationLimit;

    private final ReentrantLock lock;

    private Iterator<? extends E> elementIterator;

    public BoundedExpirationStrategy(Iterable<? extends E> elements,
                                     Consumer<? super E> tryExpire, int elementLimitPerPoll,
                                     Duration pollDurationLimit) {
        this.elements = elements;
        this.tryExpire = tryExpire;
        this.elementLimitPerPoll = elementLimitPerPoll;
        this.pollDurationLimit = pollDurationLimit;
        this.lock = new ReentrantLock();
    }

    public void doExpiration() {
        if (lock.tryLock()) {
            try {
                int elementsTraversed = 0;
                long startTime = System.nanoTime();
                if (elementIterator == null || !elementIterator.hasNext()) {
                    elementIterator = elements.iterator();
                }
                while (elementIterator.hasNext()) {
                    E element = elementIterator.next();
                    tryExpire.accept(element);
                    long elapsedNanos = System.nanoTime() - startTime;
                    if (++elementsTraversed >= elementLimitPerPoll || elapsedNanos >= pollDurationLimit.toNanos()) {
                        return;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
