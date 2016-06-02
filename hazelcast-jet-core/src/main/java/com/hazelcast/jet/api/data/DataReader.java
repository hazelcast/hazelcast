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

package com.hazelcast.jet.api.data;

import com.hazelcast.jet.internal.api.actor.ObjectProducer;

/**
 * Represents abstract reader from some source with data;
 */
public interface DataReader extends ObjectProducer {
    /**
     * @return - if there is next objects to read, false - otherwise;
     */
    boolean hasNext();

    /**
     * @return - current reader's position;
     */
    long position();

    /**
     * @return - true if data read process will be in Hazelcast partition-thread; false-otherwise;
     */
    boolean readFromPartitionThread();

    /**
     * Read next chunk of objects;
     * It performs read always from current thread;
     *
     * @return - corresponding chunk of objects;
     */
    Object[] read();

    /**
     * Read next chunk of objects;
     *
     * It will take into account result of {@link #readFromPartitionThread() readFromPartitionThread} method;
     *
     * @return corresponding chunk of objects;
     */
    Object[] produce();

    /**
     * @return - reader's partition id;
     */
    int getPartitionId();
}
