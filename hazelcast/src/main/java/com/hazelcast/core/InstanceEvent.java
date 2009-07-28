/*
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.core;

import java.util.EventObject;

public class InstanceEvent extends EventObject {

    public enum InstanceEventType {
        CREATED, DESTROYED
    }

    private final InstanceEventType instanceEventType;
    private final Instance instance;

    public InstanceEvent(InstanceEventType instanceEventType, Instance instance) {
        super(instance);
        this.instanceEventType = instanceEventType;
        this.instance = instance;
    }

    public InstanceEventType getEventType() {
        return instanceEventType;
    }

    public Instance.InstanceType getInstanceType() {
        return instance.getInstanceType();
    }

    public Instance getInstance() {
        return instance;
    }
}