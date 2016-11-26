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

package com.hazelcast.mapreduce;

import com.hazelcast.nio.serialization.impl.BinaryInterface;

import java.io.Serializable;

/**
 * This interface is used to pre-evaluate keys before spreading the MapReduce task to the cluster.
 * Pre-selecting keys can speed up the job since not all partitions may be used.
 *
 * @param <Key> key type
 * @since 3.2
 * @deprecated Hazelcast Jet will replace, maybe re-implement this API shortly
 */
@Deprecated
@BinaryInterface
public interface KeyPredicate<Key>
        extends Serializable {

    /**
     * This method evaluates whether or not to select a key.
     *
     * @param key The key to evaluate
     * @return true if the MapReduce task should be executed on this key, false otherwise
     */
    boolean evaluate(Key key);

}
