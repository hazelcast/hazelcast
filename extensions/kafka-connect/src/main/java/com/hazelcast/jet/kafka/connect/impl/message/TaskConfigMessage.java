/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.connect.impl.message;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TaskConfigMessage implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private List<Map<String, String>> taskConfigs;

    public List<Map<String, String>> getTaskConfigs() {
        return taskConfigs;
    }

    public void setTaskConfigs(List<Map<String, String>> taskConfigs) {
        this.taskConfigs = taskConfigs;
    }
}
