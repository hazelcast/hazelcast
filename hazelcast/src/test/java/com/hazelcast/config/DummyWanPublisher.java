/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.WanPublisher;

import java.io.Serializable;

public class DummyWanPublisher implements WanPublisher, Serializable {
    @Override
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig publisherConfig) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void doPrepublicationChecks() {

    }

    @Override
    public void publishReplicationEvent(WanEvent eventObject) {

    }

    @Override
    public void publishReplicationEventBackup(WanEvent eventObject) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
