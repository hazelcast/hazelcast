/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.parameters;

final class TemplateConstants {

    static final int MAP_TEMPLATE_ID = 1;
    static final int MULTIMAP_TEMPLATE_ID = 2;
    static final int QUEUE_TEMPLATE_ID = 3;
    static final int TOPIC_TEMPLATE_ID = 4;
    static final int LIST_TEMPLATE_ID = 5;
    static final int SET_TEMPLATE_ID = 6;
    static final int LOCK_TEMPLATE_ID = 7;
    static final int CONDITION_TEMPLATE_ID = 8;
    static final int EXECUTOR_TEMPLATE_ID = 9;
    static final int ATOMIC_LONG_TEMPLATE_ID = 10;
    static final int ATOMIC_REFERENCE_TEMPLATE_ID = 11;
    static final int COUNTDOWN_LATCH_TEMPLATE_ID = 12;
    static final int SEMAPHORE_TEMPLATE_ID = 13;
    static final int REPLICATED_MAP_TEMPLATE_ID = 14;
    static final int MAP_REDUCE_TEMPLATE_ID = 15;
    static final int TX_MAP_TEMPLATE_ID = 16;
    static final int TX_MULTIMAP_TEMPLATE_ID = 17;
    static final int TX_SET_TEMPLATE_ID = 18;
    static final int TX_LIST_TEMPLATE_ID = 19;
    static final int TX_QUEUE_TEMPLATE_ID = 20;

    private TemplateConstants() {
    }
}
