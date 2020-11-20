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

package com.hazelcast.sql.impl.calcite.validate.types;

import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayDeque;
import java.util.Deque;

public final class HazelcastTrackingReturnTypeInference {

    private static final ThreadLocal<Deque<RelDataType>> QUEUE = ThreadLocal.withInitial(ArrayDeque::new);

    private HazelcastTrackingReturnTypeInference() {
        // No-op
    }

    public static void push(RelDataType callType) {
        QUEUE.get().push(callType);
    }

    public static RelDataType peek() {
        return QUEUE.get().peek();
    }

    public static RelDataType poll() {
        return QUEUE.get().poll();
    }
}
