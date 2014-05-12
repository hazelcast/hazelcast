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

package com.hazelcast.map.record;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 *  Record info.
 */
public class RecordInfo implements DataSerializable {
    protected RecordStatistics statistics;
    protected long version;
    protected long evictionCriteriaNumber;
    protected long ttl;
    protected long creationTime;
    protected long lastAccessTime;
    protected long lastUpdateTime;

    public RecordInfo() {
    }

    public RecordInfo(RecordInfo recordInfo) {
        this.statistics = recordInfo.statistics;
        this.version = recordInfo.version;
        this.evictionCriteriaNumber = recordInfo.evictionCriteriaNumber;
        this.ttl = recordInfo.ttl;
        this.creationTime = recordInfo.creationTime;
        this.lastAccessTime = recordInfo.lastAccessTime;
        this.lastUpdateTime = recordInfo.lastUpdateTime;
    }

    public RecordStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(RecordStatistics statistics) {
        this.statistics = statistics;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getEvictionCriteriaNumber() {
        return evictionCriteriaNumber;
    }

    public void setEvictionCriteriaNumber(long evictionCriteriaNumber) {
        this.evictionCriteriaNumber = evictionCriteriaNumber;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (statistics != null) {
            out.writeBoolean(true);
            statistics.writeData(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(version);
        out.writeLong(evictionCriteriaNumber);
        out.writeLong(ttl);
        out.writeLong(creationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        boolean statsEnabled = in.readBoolean();
        if (statsEnabled) {
            statistics = new RecordStatistics();
            statistics.readData(in);
        }
        version = in.readLong();
        evictionCriteriaNumber = in.readLong();
        ttl = in.readLong();
        creationTime = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("RecordInfo{");
        builder.append("statistics=");
        builder.append(statistics);
        builder.append(", version=");
        builder.append(version);
        builder.append(", evictionCriteriaNumber=");
        builder.append(evictionCriteriaNumber);
        builder.append(", ttl=");
        builder.append(ttl);
        builder.append(", creationTime=");
        builder.append(creationTime);
        builder.append(", lastAccessTime=");
        builder.append(lastAccessTime);
        builder.append(", lastUpdateTime=");
        builder.append(lastUpdateTime);
        builder.append('}');
        return builder.toString();
    }
}
