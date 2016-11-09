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

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Javadoc pending.
 */
public interface Inbox {

    /**
     * Javadoc pending
     */
    Object peek();

    /**
     * Javadoc pending
     */
    Object poll();

    /**
     * Javadoc pending
     */
    Object remove();

    /**
     * Javadoc pending
     */
    default <E> int drainTo(Collection<E> collection) {
        int drained = 0;
        //noinspection unchecked
        for (E o; (o = (E) poll()) != null; drained++) {
            collection.add(o);
        }
        return drained;
    }

    /**
     * Javadoc pending
     */
    default <E> int drain(Consumer<E> consumer) {
        int consumed = 0;
        //noinspection unchecked
        for (E o; (o = (E) poll()) != null; consumed++) {
            consumer.accept(o);
        }
        return consumed;
    }
}
