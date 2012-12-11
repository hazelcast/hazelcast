/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.core.Instance.InstanceType;

/**
 * Holds the prefix constants used by Hazelcast.
 */
public final class Prefix {

    /**
     * The Constant MAP_BASED: "m:"
     */
    public static final String MAP_BASED = "m:";

    /**
     * The Constant MAP: "c:"
     */
    public static final String MAP = "c:";

    /**
     * The Constant AS_LIST: "l:"
     */
    public static final String AS_LIST = "l:";

    /**
     * The Constant AS_SET: "s:"
     */
    public static final String AS_SET = "s:";

    /**
     * The Constant SET: "m:s:"
     */
    public static final String SET = MAP_BASED + AS_SET;

    /**
     * The Constant QUEUE: "q:"
     */
    public static final String QUEUE = "q:";

    /**
     * The Constant TOPIC: "t:"
     */
    public static final String TOPIC = "t:";

    /**
     * The Constant IDGEN: "i:"
     */
    public static final String IDGEN = "i:";

    /**
     * The Constant ATOMIC_NUMBER: "a:"
     */
    public static final String ATOMIC_NUMBER = "a:";

    /**
     * The Constant AS_MULTIMAP: "u:"
     */
    public static final String AS_MULTIMAP = "u:";

    /**
     * The Constant MULTIMAP: "m:u:"
     */
    public static final String MULTIMAP = MAP_BASED + AS_MULTIMAP;

    /**
     * The Constant MAP_FOR_QUEUE: "c:q:"
     */
    public static final String MAP_FOR_QUEUE = MAP + QUEUE;

    /**
     * The Constant QUEUE_LIST: "q:l:"
     */
    public static final String QUEUE_LIST = QUEUE + AS_LIST;
    /**
     * The Constant MAP_OF_LIST: "c:q:l:"
     */
    public static final String MAP_OF_LIST = MAP_FOR_QUEUE + AS_LIST;

    /**
     * The Constant EXECUTOR_SERVICE: "x:"
     */
    public static final String EXECUTOR_SERVICE = "x:";

    /**
     * The Constant SEMAPHORE: "4:"
     */
    public static final String SEMAPHORE = "4:";

    /**
     * The Constant COUNT_DOWN_LATCH: "d:"
     */
    public static final String COUNT_DOWN_LATCH = "d:";

    /**
     * The Constant HAZELCAST: "__hz_"
     */
    public static final String HAZELCAST = "__hz_";

    /**
     * The Constant MAP_HAZELCAST: "c:__hz_"
     */
    public static final String MAP_HAZELCAST = MAP + HAZELCAST;

    /**
     * The Constant QUEUE_HAZELCAST: "q:__hz_"
     */
    public static final String QUEUE_HAZELCAST = QUEUE + HAZELCAST;

    /**
     * The Constant TOPIC_HAZELCAST: "t:__hz_"
     */
    public static final String TOPIC_HAZELCAST = TOPIC + HAZELCAST;

    /**
     * The Constant ATOMIC_NUMBER_HAZELCAST: "a:__hz_"
     */
    public static final String ATOMIC_NUMBER_HAZELCAST = ATOMIC_NUMBER + HAZELCAST;

    /**
     * The Constant LOCKS_MAP_HAZELCAST: "c:__hz_Locks"
     */
    public static final String LOCKS_MAP_HAZELCAST = MAP_HAZELCAST + "Locks";


    public static InstanceType getInstanceType(final String name) {
        if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
            return InstanceType.ATOMIC_NUMBER;
        } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
            return InstanceType.COUNT_DOWN_LATCH;
        } else if (name.startsWith(Prefix.IDGEN)) {
            return InstanceType.ID_GENERATOR;
        } else if (name.startsWith(Prefix.AS_LIST)) {
            return InstanceType.LIST;
        } else if (name.startsWith(Prefix.MAP)) {
            return InstanceType.MAP;
        } else if (name.startsWith(Prefix.MULTIMAP)) {
            return InstanceType.MULTIMAP;
        } else if (name.startsWith(Prefix.QUEUE)) {
            return InstanceType.QUEUE;
        } else if (name.startsWith(Prefix.SEMAPHORE)) {
            return InstanceType.SEMAPHORE;
        } else if (name.startsWith(Prefix.SET)) {
            return InstanceType.SET;
        } else if (name.startsWith(Prefix.TOPIC)) {
            return InstanceType.TOPIC;
        } else {
            throw new RuntimeException("Unknown InstanceType " + name);
        }
    }

    /**
     * Private constructor to avoid instances of the class.
     */
    private Prefix() {
    }
}
