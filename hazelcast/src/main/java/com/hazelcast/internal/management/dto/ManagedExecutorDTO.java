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

package com.hazelcast.internal.management.dto;

import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.executor.ManagedExecutorService;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getLong;
import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.internal.jmx.ManagedExecutorServiceMBean}.
 */
public class ManagedExecutorDTO implements JsonSerializable {

    public String name;
    public int queueSize;
    public int poolSize;
    public int remainingQueueCapacity;
    public int maximumPoolSize;
    public boolean isTerminated;
    public long completedTaskCount;

    public ManagedExecutorDTO() {
    }

    public ManagedExecutorDTO(ManagedExecutorService executorService) {
        this.name = executorService.getName();
        this.queueSize = executorService.getQueueSize();
        this.poolSize = executorService.getPoolSize();
        this.remainingQueueCapacity = executorService.getRemainingQueueCapacity();
        this.maximumPoolSize = executorService.getMaximumPoolSize();
        this.isTerminated = executorService.isTerminated();
        this.completedTaskCount = executorService.getCompletedTaskCount();
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("name", name);
        root.add("queueSize", queueSize);
        root.add("poolSize", poolSize);
        root.add("remainingQueueCapacity", remainingQueueCapacity);
        root.add("maximumPoolSize", maximumPoolSize);
        root.add("isTerminated", isTerminated);
        root.add("completedTaskCount", completedTaskCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        name = getString(json, "name");
        queueSize = getInt(json, "queueSize");
        poolSize = getInt(json, "poolSize");
        remainingQueueCapacity = getInt(json, "remainingQueueCapacity");
        maximumPoolSize = getInt(json, "maximumPoolSize");
        isTerminated = getBoolean(json, "isTerminated");
        completedTaskCount = getLong(json, "completedTaskCount");
    }
}
