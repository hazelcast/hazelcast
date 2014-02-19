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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.WaitNotifyKey;

public final class ConditionKey implements WaitNotifyKey {

    private final String name;
    private final Data key;
    private final String conditionId;

    public ConditionKey(String name, Data key, String conditionId) {
        this.name = name;
        this.key = key;
        this.conditionId = conditionId;
    }

    @Override
    public String getServiceName() {
        return LockServiceImpl.SERVICE_NAME;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConditionKey that = (ConditionKey) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (conditionId != null ? !conditionId.equals(that.conditionId) : that.conditionId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (conditionId != null ? conditionId.hashCode() : 0);
        return result;
    }
}
