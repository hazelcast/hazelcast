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

package com.hazelcast.internal.jmx;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.logging.impl.LoggingServiceImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import static com.hazelcast.internal.jmx.ManagementService.quote;

public class LoggingServiceMBean extends HazelcastMBean<LoggingServiceImpl> {

    protected LoggingServiceMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService service) {
        super((LoggingServiceImpl) hazelcastInstance.getLoggingService(), service);

        Map<String, String> properties = new HashMap<>();
        properties.put("type", quote("HazelcastInstance.LoggingService"));
        properties.put("name", quote(hazelcastInstance.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("level")
    @ManagedDescription("Current level set")
    public String getLevel() {
        Level level = managedObject.getLevel();
        return level == null ? null : level.getName();
    }

    @ManagedAnnotation(value = "setLevel", operation = true)
    @ManagedDescription("Set level")
    public void setLevel(String level) {
        managedObject.setLevel(level);
    }

    @ManagedAnnotation(value = "resetLevel", operation = true)
    @ManagedDescription("Reset level")
    public void resetLevel() {
        managedObject.resetLevel();
    }

}
