/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;

public final class RawJobMetrics implements IdentifiedDataSerializable {

    private long timestamp;
    private byte[] blob;

    RawJobMetrics() { //needed for deserialization
    }

    private RawJobMetrics(long timestamp, byte[] blob) {
        this.timestamp = timestamp;
        this.blob = blob;
    }

    public static RawJobMetrics empty() {
        return of(null);
    }

    public static RawJobMetrics of(@Nullable byte[] blob) {
        return new RawJobMetrics(System.currentTimeMillis(), blob);
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Nullable
    public byte[] getBlob() {
        return blob;
    }

    @Override
    public int getFactoryId() {
        return JetMetricsDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetMetricsDataSerializerHook.RAW_JOB_METRICS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(blob);
        out.writeLong(timestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        blob = in.readByteArray();
        timestamp = in.readLong();
    }

    @Override
    public int hashCode() {
        return (int) timestamp * 31 + Arrays.hashCode(blob);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        RawJobMetrics that;
        return Arrays.equals(blob, (that = (RawJobMetrics) obj).blob)
                && this.timestamp == that.timestamp;
    }

    @Override
    public String toString() {
        return Arrays.toString(blob) + " @ " + timestamp;
    }
}
