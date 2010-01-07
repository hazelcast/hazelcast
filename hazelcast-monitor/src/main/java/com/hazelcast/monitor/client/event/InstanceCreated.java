/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.monitor.client.event;

import com.hazelcast.monitor.client.InstanceType;

import java.io.Serializable;
import java.util.Date;

public class InstanceCreated extends InstanceEvent implements Serializable {
    private Date date;

    public InstanceCreated(Date date) {
        this.date = date;

    }

    public InstanceCreated(int clusterId, InstanceType instanceType, String name) {
        super(clusterId, instanceType, name);
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.INSTANCE_CREATED;
    }
}