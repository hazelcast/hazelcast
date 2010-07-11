/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

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
     * The Constant LIST: "m:l:"
     */
    public static final String LIST = MAP_BASED + AS_LIST;

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
     * The Constant ATOMIC_NUMBER: a:"
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
     * Private constructor to avoid instances of the class.
     */
    private Prefix() {
    }
}
