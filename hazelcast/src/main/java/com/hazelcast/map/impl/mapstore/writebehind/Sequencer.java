/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.nio.serialization.DataSerializable;

/**
 * Responsible for giving a sequence-number to a
 * {@link com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry DelayedEntry} when write-behind mode is on.
 * Used in {@link WriteBehindStore} implementation.
 */
public interface Sequencer extends DataSerializable {

    /**
     * Returns head sequence number.
     *
     * @return current sequence number.
     */
    long headSequence();

    /**
     * Returns current sequence number.
     *
     * @return current sequence number.
     */
    long tailSequence();

    /**
     * Returns next sequence number.
     *
     * @return next sequence number.
     */
    long incrementHead();

    /**
     * Returns next sequence number.
     *
     * @return next sequence number.
     */
    long incrementTail();

    /**
     * Set head sequence number to the supplied sequence.
     *
     * @param sequence supplied sequence.
     */
    void setHeadSequence(long sequence);

    /**
     * Set tail sequence number to the supplied sequence.
     *
     * @param sequence supplied sequence.
     */
    void setTailSequence(long sequence);


    /**
     * Initializes this sequencer after init returns this sequencer will have zero as current value.
     */
    void init();

}
