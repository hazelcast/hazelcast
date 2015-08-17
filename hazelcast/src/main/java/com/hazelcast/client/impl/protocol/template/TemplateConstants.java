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

package com.hazelcast.client.impl.protocol.template;

public final class TemplateConstants {

    public static final int CLIENT_TEMPLATE_ID = 0;
    public static final int MAP_TEMPLATE_ID = 1;
    public static final int MULTIMAP_TEMPLATE_ID = 2;
    public static final int QUEUE_TEMPLATE_ID = 3;
    public static final int TOPIC_TEMPLATE_ID = 4;
    public static final int LIST_TEMPLATE_ID = 5;
    public static final int SET_TEMPLATE_ID = 6;
    public static final int LOCK_TEMPLATE_ID = 7;
    public static final int CONDITION_TEMPLATE_ID = 8;
    public static final int EXECUTOR_TEMPLATE_ID = 9;
    public static final int ATOMIC_LONG_TEMPLATE_ID = 10;
    public static final int ATOMIC_REFERENCE_TEMPLATE_ID = 11;
    public static final int COUNTDOWN_LATCH_TEMPLATE_ID = 12;
    public static final int SEMAPHORE_TEMPLATE_ID = 13;
    public static final int REPLICATED_MAP_TEMPLATE_ID = 14;
    public static final int MAP_REDUCE_TEMPLATE_ID = 15;
    public static final int TX_MAP_TEMPLATE_ID = 16;
    public static final int TX_MULTIMAP_TEMPLATE_ID = 17;
    public static final int TX_SET_TEMPLATE_ID = 18;
    public static final int TX_LIST_TEMPLATE_ID = 19;
    public static final int TX_QUEUE_TEMPLATE_ID = 20;
    public static final int JCACHE_TEMPLATE_ID = 21;
    public static final int XA_TRANSACTION_TEMPLATE_ID = 22;
    public static final int TRANSACTION_TEMPLATE_ID = 23;
    public static final int ENTERPRISE_MAP_TEMPLATE_ID = 24;
    public static final int RINGBUFFER_TEMPLATE_ID = 25;

    private TemplateConstants() {
    }
}
