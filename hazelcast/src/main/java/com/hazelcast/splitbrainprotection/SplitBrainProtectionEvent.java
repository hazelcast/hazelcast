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

import com.hazelcast.cluster.Member;

import java.util.Collection;
import java.util.EventObject;

/**
 * An Event that is send when a {@link SplitBrainProtection} changes.
 * <p>
 * Hold member list, split brain protection threshold and the split brain protection result.
 */
public class SplitBrainProtectionEvent extends EventObject {

    private final int threshold;
    private final Collection<Member> currentMembers;
    private final boolean presence;

    public SplitBrainProtectionEvent(Object source, int threshold, Collection<Member> currentMembers, boolean presence) {
        super(source);
        this.threshold = threshold;
        this.currentMembers = currentMembers;
        this.presence = presence;
    }

    /**
     * Returns the predefined split brain protection threshold
     *
     * @return int threshold
     */
    public int getThreshold() {
        return threshold;
    }

    /**
     * Returns the snapshot of member list at the time split brain protection happened
     *
     * @return {@code Collection&lt;Member&gt;} collection of members
     */
    public Collection<Member> getCurrentMembers() {
        return currentMembers;
    }

    /**
     * Returns the presence of the split brain protection
     *
     * @return boolean presence of the split brain protection
     */
    public boolean isPresent() {
        return presence;
    }

    @Override
    public String toString() {
        return "SplitBrainProtectionEvent{"
                + "threshold=" + threshold
                + ", currentMembers=" + currentMembers
                + ", presence=" + presence
                + '}';
    }
}
