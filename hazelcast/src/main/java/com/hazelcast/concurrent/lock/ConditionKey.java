/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.WaitNotifyKey;

public final class ConditionKey implements WaitNotifyKey {

    private final String name;
    private final Data key;
    private final String conditionId;
    private final long threadId;
    private final String uuid;

    public ConditionKey(String name, Data key, String conditionId, String uuid, long threadId) {
        this.name = name;
        this.key = key;
        this.conditionId = conditionId;
        this.uuid = uuid;
        this.threadId = threadId;
    }

    @Override
    public String getServiceName() {
        return LockServiceImpl.SERVICE_NAME;
    }

    public String getUuid() {
        return uuid;
    }

    @Override
    public String getObjectName() {
        return name;
    }

    public Data getKey() {
        return key;
    }

    public String getConditionId() {
        return conditionId;
    }

    public long getThreadId() {
        return threadId;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConditionKey)) {
            return false;
        }
        ConditionKey that = (ConditionKey) o;
        if (threadId != that.threadId) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!key.equals(that.key)) {
            return false;
        }
        if (!uuid.equals(that.uuid)) {
            return false;
        }
        return conditionId.equals(that.conditionId);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + conditionId.hashCode();
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        result = 31 * result + uuid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConditionKey{"
                + "name='" + name + '\''
                + ", key=" + key
                + ", conditionId='" + conditionId + '\''
                + ", threadId=" + threadId
                + '}';
    }
}
