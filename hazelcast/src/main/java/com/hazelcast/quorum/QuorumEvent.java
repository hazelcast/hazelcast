/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum;

import com.hazelcast.core.Member;
import java.util.Collection;
import java.util.EventObject;

/**
 * An Event that is send when a {@link Quorum} changes.
 * <p/>
 * Hold member list, quorum threshold and the quorum result.
 */
public class QuorumEvent extends EventObject {

    private final int threshold;
    private final Collection<Member> currentMembers;
    private final boolean presence;

    public QuorumEvent(Object source, int threshold, Collection<Member> currentMembers, boolean presence) {
        super(source);
        this.threshold = threshold;
        this.currentMembers = currentMembers;
        this.presence = presence;
    }

    /**
     * Returns the predefined quorum threshold
     *
     * @return int threshold
     */
    public int getThreshold() {
        return threshold;
    }

    /**
     * Returns the snapshot of member list at the time quorum happened
     *
     * @return Collection<Member> collection of members
     */
    public Collection<Member> getCurrentMembers() {
        return currentMembers;
    }

    /**
     * Returns the presence of the quorum
     *
     * @return boolean presence of the quorum
     */
    public boolean isPresent() {
        return presence;
    }

}
