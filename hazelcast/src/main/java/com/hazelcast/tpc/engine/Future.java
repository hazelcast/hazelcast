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
import java.util.function.Consumer;

public class Future<E> {

    private final static Object EMPTY = new Object();

    public static <E> Future<E> newReadyFuture(E value) {
        return new Future<>(value);
    }

    public static <E> Future<E> newFuture() {
        return new Future<E>(EMPTY);
    }

    private Object value;
    Eventloop eventloop;
    private List<Consumer> consumers = new ArrayList<>();

    private Future(Object value) {
        this.value = value;
    }

    public void completeExceptionally(Throwable throwable){
        complete(new Exceptional(throwable));
    }

    public void complete(Object value) {
        if (this.value != EMPTY) {
            throw new IllegalStateException();
        }
        this.value = value;

        if (!consumers.isEmpty()) {
            for (Consumer consumer : consumers) {
                try {
                    consumer.accept(value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //todo: better handling of exceptional.
    public void then(Consumer<E> consumer) {
        if (this.value != EMPTY) {

            // todo: from pool
            Task task = new Task();
            task.consumer = consumer;
            task.value = value;

            eventloop.execute(task);
        } else {
            consumers.add(consumer);
        }
    }

    private static class Task implements EventloopTask {
        private Consumer consumer;
        private Object value;

        @Override
        public void run() throws Exception {
            consumer.accept(value);
        }
    }

    private class Exceptional{
        private Throwable value;

        public Exceptional(Throwable value) {
            this.value = value;
        }
    }
}
