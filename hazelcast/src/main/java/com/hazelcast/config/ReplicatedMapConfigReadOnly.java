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

package com.hazelcast.config;

import java.util.concurrent.ExecutorService;

class ReplicatedMapConfigReadOnly extends ReplicatedMapConfig {

    public ReplicatedMapConfigReadOnly(ReplicatedMapConfig replicatedMapConfig) {
        super(replicatedMapConfig);
    }

    @Override
    public void setReplicatorExecutorService(ExecutorService replicatorExecutorService) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setReplicationDelayMillis(long replicationDelayMillis) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setConcurrencyLevel(int concurrencyLevel) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
