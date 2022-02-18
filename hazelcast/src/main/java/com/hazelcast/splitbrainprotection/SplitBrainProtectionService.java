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

package com.hazelcast.splitbrainprotection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Split brain protection service can be used to trigger cluster split brain protections at any time.
 * Normally split brain protections are done when any change happens on the member list.
 */
public interface SplitBrainProtectionService {

    /**
     * Returns the {@link SplitBrainProtection} instance for a given split brain protection name.
     *
     * @param splitBrainProtectionName name of the split brain protection
     * @return {@link SplitBrainProtection}
     * @throws IllegalArgumentException if no split brain protection found for given name
     * @throws NullPointerException     if splitBrainProtectionName is null
     */
    @Nonnull
    SplitBrainProtection getSplitBrainProtection(@Nonnull String splitBrainProtectionName) throws IllegalArgumentException;

    /**
     * Ensures that the split brain protection with the given name is present.
     * Throws a SplitBrainProtectionException if split brain protection not present.
     * Does not throw exception if splitBrainProtectionName null or split brain protection undefined.
     *
     * If (requiredSplitBrainProtectionPermissionType == READ)
     *    -&gt; will check for presence of READ or READ_WRITE split brain protection<br>
     * If (requiredSplitBrainProtectionPermissionType == WRITE)
     *    -&gt; will check for presence of WRITE or READ_WRITE split brain protection<br>
     * If (requiredSplitBrainProtectionPermissionType == READ_WRITE)
     *    -&gt; will check for presence of READ_WRITE split brain protection<br>
     *
     * @param splitBrainProtectionName split brain protection name to ensure, can be null or empty
     * @param requiredSplitBrainProtectionPermissionType type of split brain protection required
     * @throws SplitBrainProtectionException if split brain protection defined and not present
     */
    void ensureNoSplitBrain(@Nullable String splitBrainProtectionName,
                            @Nonnull SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType)
            throws SplitBrainProtectionException;
}
