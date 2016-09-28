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

import com.hazelcast.jet2.Cursor;
import com.hazelcast.jet2.Producer;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.Map;

public class ProducerTasklet implements Tasklet {

    private final Producer<?> producer;
    private Object lastProduced;
    private Cursor<Output> outputCursor;
    private boolean lastPushSuccess = true;

    public ProducerTasklet(Producer<?> producer, Map<String, Output> outputs) {
        Preconditions.checkNotNull(producer, "producer");
        Preconditions.checkTrue(outputs.size() > 0, "There must be at least one output");

        this.producer = producer;
        this.outputCursor = new ListCursor<>(new ArrayList<>(outputs.values()));
    }

    @Override
    public TaskletResult call() {
        if (!lastPushSuccess && !tryPush()) {
            return TaskletResult.NOT_DONE_BACKOFF;
        }

        do {
            lastProduced = producer.next();
            // we reached the end of the input
            if (lastProduced == null) {
                return TaskletResult.DONE;
            }
            outputCursor.reset();
            outputCursor.advance();
            lastPushSuccess = tryPush();
        } while (lastPushSuccess);
        return TaskletResult.NOT_DONE;
    }

    private boolean tryPush() {
        do {
            if (!outputCursor.value().collect(lastProduced)) {
                return false;
            }
        } while (outputCursor.advance());
        return true;
    }

}

