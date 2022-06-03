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

package com.hazelcast.tpc.engine;


import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class Future<E> {

    private final static Object EMPTY = new Object();

    public static <E> Future<E> newReadyFuture(E value) {
        return new Future<>(value);
    }

    public static <E> Future<E> newFuture() {
        return new Future<E>(EMPTY);
    }

    private Object value;
    private boolean exceptional;
    Eventloop eventloop;
    private List<BiConsumer<E, Throwable>> consumers = new ArrayList<>();

    private Future(Object value) {
        this.value = value;
    }

    public void completeExceptionally(Throwable value) {
        checkNotNull(value);

        if (this.value != EMPTY) {
            throw new IllegalStateException();
        }
        this.value = value;
        this.exceptional = true;

        if (!consumers.isEmpty()) {
            for (BiConsumer consumer : consumers) {
                try {
                    consumer.accept(null, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void complete(Object value) {
        if (this.value != EMPTY) {
            throw new IllegalStateException();
        }
        this.value = value;

        if (!consumers.isEmpty()) {
            for (BiConsumer consumer : consumers) {
                try {
                    consumer.accept(value, null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void then(BiConsumer<E, Throwable> consumer) {
        if (this.value != EMPTY) {
            // todo: from pool
            Task<E> task = new Task<>();
            task.consumer = consumer;
            task.value = value;
            task.exceptional = exceptional;

            eventloop.execute(task);
        } else {
            consumers.add(consumer);
        }
    }

    private static class Task<E> implements EventloopTask {
        private BiConsumer<E, Throwable> consumer;
        private Object value;
        private boolean exceptional;

        @Override
        public void run() throws Exception {
            if (exceptional) {
                consumer.accept(null, (Throwable) value);
            } else {
                consumer.accept((E) value, null);
            }
        }
    }
}
