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

package com.hazelcast.jet.io.tuple;

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * A general, untyped tuple.
 */
public interface Tuple extends DataSerializable {

    /** Assigns the given object to the component at the given index. */
    void set(int index, Object o);

    /** Gets the component at the given index. */
    <T> T get(int index);

    /** Returns the number of components in this tuple. */
    int size();

    /** Returns an array of this tuple's components. */
    Object[] toArray();
}
