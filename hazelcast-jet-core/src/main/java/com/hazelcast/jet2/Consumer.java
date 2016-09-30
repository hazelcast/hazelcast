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

public interface Consumer<T> {

    /**
     * Try to consume the given item. If item is not consumed immediately, the method will be called again with the
     * same item.
     * @return true if item is consumed, false otherwise
     */
    boolean consume(T object);

    /**
     * Called once all the input has been exhausted.
     */
    void complete();

    /**
     * Return if {@link Consumer} performs blocking operations (such as IO) on calls to {@link #consume(Object)}.
     */
    default boolean isBlocking() {
        return false;
    }
}
