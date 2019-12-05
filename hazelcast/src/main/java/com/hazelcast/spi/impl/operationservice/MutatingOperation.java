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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.config.SplitBrainProtectionConfig;

/**
 * Marker interface for operations that change state/data.
 * Used for split brain protection to reject operations if
 * the split brain protection size not satisfied.
 * <p>
 * Operations implementing {@link BackupOperation} should
 * not be marked with this interface.
 *
 * @see SplitBrainProtectionConfig
 * @see SplitBrainProtectionCheckAwareOperation
 * @see ReadonlyOperation
 */
public interface MutatingOperation {
}
