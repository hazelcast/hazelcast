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

package com.hazelcast.cluster.memberselector;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;

/**
 * Selects a member only if all of the sub-selectors succeed
 */
class AndMemberSelector
        implements MemberSelector {

    private final MemberSelector[] selectors;

    public AndMemberSelector(MemberSelector... selectors) {
        this.selectors = selectors;
    }

    @Override
    public boolean select(Member member) {
        for (MemberSelector selector : selectors) {
            if (!selector.select(member)) {
                return false;
            }
        }

        return true;
    }
}
