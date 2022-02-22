/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.function;

/**
 * This is a (long,long) primitive specialisation of a BiConsumer
 */
public interface LongLongConsumer {
    /**
     * Accept a key and value that comes as a tuple of longs.
     *
     * @param key   for the tuple.
     * @param value for the tuple.
     */
    void accept(long key, long value);
}
