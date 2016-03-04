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

package com.hazelcast.jet.impl.container.task.processors;

import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class ConsumersProcessor {
    private final ObjectConsumer[] consumers;
    private boolean consumed;
    private ProducerInputStream inputStream;

    public ConsumersProcessor(ObjectConsumer[] consumers) {
        this.consumers = consumers;
    }

    public boolean process(ProducerInputStream inputStream) throws Exception {
        boolean success = true;
        boolean consumed = false;

        if (this.inputStream == null) {
            this.inputStream = inputStream;

            for (ObjectConsumer consumer : this.consumers) {
                consumer.consumeChunk(inputStream);
                success = success && consumer.isFlushed();
                consumed = consumed || consumer.lastConsumedCount() > 0;
            }
        } else {
            for (ObjectConsumer consumer : this.consumers) {
                success = success & consumer.isFlushed();
                consumed = consumed || consumer.lastConsumedCount() > 0;
            }
        }

        if (success) {
            this.inputStream = null;
        }

        this.consumed = consumed;
        return success;
    }

    public boolean isConsumed() {
        return this.consumed;
    }

    public void reset() {
        this.consumed = false;
        this.inputStream = null;
    }
}
