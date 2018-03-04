/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.util.counters.Counter;

/**
 * Trigger for the ChannelReader to inject the appropriate counters. This is a
 * temporary solution, it would be best if the counters could be defined directly
 * on handlers and automatically get registered + unregistered.
 */
public abstract class ChannelInboundHandlerWithCounters implements ChannelInboundHandler {
    protected Counter normalPacketsRead;
    protected Counter priorityPacketsRead;

    public void setNormalPacketsRead(Counter normalPacketsRead) {
        this.normalPacketsRead = normalPacketsRead;
    }

    public void setPriorityPacketsRead(Counter priorityPacketsRead) {
        this.priorityPacketsRead = priorityPacketsRead;
    }
}
