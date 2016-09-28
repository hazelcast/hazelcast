/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Chunk;
import com.hazelcast.jet2.Consumer;
import com.hazelcast.jet2.Cursor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConsumerTasklet implements Tasklet {

    private final List<Input> inputs;
    private final Consumer consumer;

    public ConsumerTasklet(Consumer<?> consumer, Map<String, Input> inputs) {
        this.consumer = consumer;
        this.inputs = new ArrayList<>(inputs.values());
    }

    @Override
    public TaskletResult call() {
        boolean consumedSome;
        do {
            consumedSome = consumeInputs();
        } while (consumedSome);

        if (inputs.isEmpty()) {
            consumer.complete();
            return TaskletResult.DONE;
        } else {
            return TaskletResult.NOT_DONE_BACKOFF;
        }
    }

    private boolean consumeInputs() {
        //TODO: avoid creating new iterator here
        Iterator<Input> iterator = inputs.iterator();
        boolean consumedSome = false;
        while (iterator.hasNext()) {
            Chunk chunk = iterator.next().nextChunk();
            //input is exhausted
            if (chunk == null) {
                iterator.remove();
                continue;
            }
            if (chunk.isEmpty()) {
                continue;
            }

            Cursor cursor = chunk.cursor();
            while (cursor.advance()) {
                consumer.consume(cursor.value());
            }
            consumedSome = true;
        }
        return consumedSome;
    }
}
