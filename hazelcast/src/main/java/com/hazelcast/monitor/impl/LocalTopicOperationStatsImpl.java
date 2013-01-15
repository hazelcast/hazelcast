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

package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.LocalTopicOperationStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class LocalTopicOperationStatsImpl extends LocalOperationStatsSupport
        implements LocalTopicOperationStats {

    long numberOfPublishes;
    long numberOfReceives;

    void writeDataInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(numberOfPublishes);
        out.writeLong(numberOfReceives);
    }

    void readDataInternal(ObjectDataInput in) throws IOException {
        numberOfPublishes = in.readLong();
        numberOfReceives = in.readLong();
    }

    public long getNumberOfPublishes() {
        return numberOfPublishes;
    }

    public long getNumberOfReceivedMessages() {
        return numberOfReceives;
    }
}
