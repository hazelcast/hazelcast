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

package com.hazelcast.config;

/**
 * Commons for quorum config builders.
 */
public abstract class QuorumConfigBuilder {

    /**
     * Quorum size: expected number of live members in cluster for quorum to be present.
     */
    protected int size;

    /**
     * Whether this quorum config is enabled or not
     */
    protected boolean enabled = true;

    public QuorumConfigBuilder enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public QuorumConfigBuilder withQuorumSize(int size) {
        this.size = size;
        return this;
    }

    public abstract QuorumConfig build();

}
