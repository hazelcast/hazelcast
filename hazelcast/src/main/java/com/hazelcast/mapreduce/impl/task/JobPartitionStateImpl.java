/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.nio.Address;

/**
 * This class holds information about the current processing state and the owner of a partition
 */
public class JobPartitionStateImpl
        implements JobPartitionState {

    private final Address address;
    private final State state;

    public JobPartitionStateImpl(Address address, State state) {
        this.address = address;
        this.state = state;
    }

    @Override
    public Address getOwner() {
        return address;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        return "JobPartitionStateImpl{" + "state=" + state + ", address=" + address + '}';
    }

}
