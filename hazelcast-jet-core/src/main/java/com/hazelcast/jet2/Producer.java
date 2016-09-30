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

package com.hazelcast.jet2;

public interface Producer<O> {

    /**
     * Pushes a batch of items into the supplied collector.
     *
     * @return <code>true</code> if all items have now been produced, <code>false</code> if not yet.
     */
    boolean produce(OutputCollector<? super O> collector);

    /**
     * Tells whether this producer's {@link #produce(OutputCollector)} method performs any
     * blocking operations (such as using a blocking I/O API). By returning
     * <code>false</code> the producer promises not to spend any time waiting for a blocking
     * operation to complete.
     */
    default boolean isBlocking() {
        return false;
    }
}
