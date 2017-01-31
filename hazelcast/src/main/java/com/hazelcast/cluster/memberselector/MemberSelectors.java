/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * A utility class to get {@link com.hazelcast.core.MemberSelector} instances.
 */
public final class MemberSelectors {

    /**
     * A {@link com.hazelcast.core.MemberSelector} instance that selects only lite members that own no partition
     */

    public static final MemberSelector LITE_MEMBER_SELECTOR = new MemberSelector() {
        @Override
        public boolean select(Member member) {
            return member.isLiteMember();
        }
    };

    /**
     * A {@link com.hazelcast.core.MemberSelector} instance that selects only data members that own a partition
     */

    public static final MemberSelector DATA_MEMBER_SELECTOR = new MemberSelector() {
        @Override
        public boolean select(Member member) {
            return !member.isLiteMember();
        }
    };

    /**
     * A {@link com.hazelcast.core.MemberSelector} instance that selects only local members
     */

    public static final MemberSelector LOCAL_MEMBER_SELECTOR = new MemberSelector() {
        @Override
        public boolean select(Member member) {
            return member.localMember();
        }
    };

    /**
     * A {@link com.hazelcast.core.MemberSelector} instance that selects only remote members
     */

    public static final MemberSelector NON_LOCAL_MEMBER_SELECTOR = new MemberSelector() {
        @Override
        public boolean select(Member member) {
            return !member.localMember();
        }
    };

    private MemberSelectors() {
    }

    /**
     * Selects a member when one of the selectors succeed
     * @param selectors {@link com.hazelcast.core.MemberSelector} instances to iterate
     * @return a {@link com.hazelcast.core.MemberSelector} that selects a member when one of the sub-selectors succeed
     */
    public static MemberSelector or(MemberSelector... selectors) {
        return new OrMemberSelector(selectors);
    }

    /**
     * Selects a member when all of the selectors succeed
     * @param selectors {@link com.hazelcast.core.MemberSelector} instances to iterate
     * @return a {@link com.hazelcast.core.MemberSelector} that selects a member when all of the sub-selectors succeed
     */
    public static MemberSelector and(MemberSelector... selectors) {
        return new AndMemberSelector(selectors);
    }

}
