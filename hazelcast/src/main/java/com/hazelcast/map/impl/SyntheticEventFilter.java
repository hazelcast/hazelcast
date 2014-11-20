/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.EventFilter;

import java.io.IOException;

/**
 * Wrapper class for synthetic events.
 * <p/>
 * Synthetic events are caused by map internals such as eviction or expiration;
 * other events are natural events like an explicit call to {@link com.hazelcast.core.IMap#put}
 * or {@link com.hazelcast.core.IMap#evict}.
 * <p/>
 * This event filter is used to prevent near cache evictions caused by internal eviction or expiration
 * operations in {@link com.hazelcast.core.IMap}, since near cache should use its own eviction/expiration mechanism,
 * this filter helps to provide right near cache behavior.
 */
public class SyntheticEventFilter implements EventFilter, DataSerializable {

    private EventFilter filter;

    public SyntheticEventFilter() {
    }

    public SyntheticEventFilter(EventFilter filter) {
        this.filter = filter;
    }

    public EventFilter getFilter() {
        return filter;
    }

    @Override
    public boolean eval(Object event) {
        return filter.eval(event);
    }

    /**
     * Writes object fields to output stream
     *
     * @param out output
     * @throws java.io.IOException
     */
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(filter);
    }

    /**
     * Reads fields from the input stream
     *
     * @param in input
     * @throws java.io.IOException
     */
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        filter = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SyntheticEventFilter that = (SyntheticEventFilter) o;

        return !(filter != null ? !filter.equals(that.filter) : that.filter != null);

    }

    @Override
    public int hashCode() {
        return filter != null ? filter.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "SyntheticEventFilter{"
                + "filter=" + filter
                + '}';
    }
}
