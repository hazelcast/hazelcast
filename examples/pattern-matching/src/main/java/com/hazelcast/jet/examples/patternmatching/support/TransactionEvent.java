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

package com.hazelcast.jet.examples.patternmatching.support;

import java.io.Serializable;

/**
 * We use java.io.{@link java.io.Serializable} here for the sake of simplicity.
 * In production, Hazelcast Custom Serialization should be used.
 */
public class TransactionEvent implements Serializable {

    private final Type type;
    private final long transactionId;
    private final long timestamp;

    public enum Type {
        START, END
    }

    public TransactionEvent(long timestamp, long transactionId, Type type) {
        this.timestamp = timestamp;
        this.transactionId = transactionId;
        this.type = type;
    }

    public Type type() {
        return type;
    }

    public long transactionId() {
        return transactionId;
    }

    public long timestamp() {
        return timestamp;
    }
}
