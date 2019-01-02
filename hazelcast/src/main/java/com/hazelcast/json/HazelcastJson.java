/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.json;


import com.hazelcast.core.HazelcastJsonValue;

/**
 * Utility class for creating {@link HazelcastJsonValue}
 */
public final class HazelcastJson {

    private HazelcastJson() {
        // not meant to be instantiated
    }

    /**
     * Create a HazelcastJsonValue from a string. This method does not the
     * validity of the underlying Json string. Invalid Json strings may cause
     * wrong results in queries.
     *
     * @param string
     * @return
     */
    public static HazelcastJsonValue fromString(String string) {
        return new HazelcastJsonImpl(string);
    }
}
