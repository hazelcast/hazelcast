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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.config.SplitBrainProtectionConfig;

/**
 * Marker interface for operations which need to control whether the check
 * for split brain protection will be performed. Internal operations (not triggered by the
 * user) may need to skip the split brain protection check to conserve consistency of data.
 *
 * @see SplitBrainProtectionConfig
 * @see SplitBrainProtectionCheckAwareOperation
 * @see ReadonlyOperation
 */
public interface SplitBrainProtectionCheckAwareOperation {

    /**
     * Returns {@code true} if the split brain protection check should be performed. Operations
     * which require a split brain protection check may get rejected if there are not enough
     * members in the cluster.
     */
    boolean shouldCheckSplitBrainProtection();
}
