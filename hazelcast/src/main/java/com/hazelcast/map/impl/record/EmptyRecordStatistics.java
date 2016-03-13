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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Empty {@link RecordStatistics} implementation which returns zero for all stats.
 */
public class EmptyRecordStatistics implements RecordStatistics {

    public EmptyRecordStatistics() {
    }

    @Override
    public long getExpirationTime() {
        return 0;
    }

    @Override
    public void setExpirationTime(long expirationTime) {

    }

    @Override
    public void store() {

    }

    @Override
    public long getLastStoredTime() {
        return 0;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {

    }

    @Override
    public long getMemoryCost() {
        return 0;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
