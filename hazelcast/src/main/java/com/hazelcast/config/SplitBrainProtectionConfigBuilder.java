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

package com.hazelcast.config;

/**
 * Commons for split brain protection config builders.
 */
public abstract class SplitBrainProtectionConfigBuilder {

    /**
     * The minimum cluster size: expected number of live members in cluster not to be considered it split
     */
    protected int minimumClusterSize;

    /**
     * Whether this split brain protection config is enabled or not
     */
    protected boolean enabled = true;

    public SplitBrainProtectionConfigBuilder enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public SplitBrainProtectionConfigBuilder withSplitBrainProtectionSize(int minimumClusterSize) {
        this.minimumClusterSize = minimumClusterSize;
        return this;
    }

    public abstract SplitBrainProtectionConfig build();

}
