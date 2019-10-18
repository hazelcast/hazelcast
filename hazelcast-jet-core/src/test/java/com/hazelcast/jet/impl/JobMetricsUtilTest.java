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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class JobMetricsUtilTest {

    @Test
    public void prefixNameWithMemberTags() throws Throwable {
        UUID uuid = UuidUtil.newUnsecureUUID();
        Member member = new MemberImpl(
                new Address("127.0.0.1", 12345),
                MemberVersion.UNKNOWN,
                true,
                uuid
        );

        String memberPrefix = JobMetricsUtil.getMemberPrefix(member);
        String prefixedName = JobMetricsUtil.addPrefixToDescriptor("[tag1=val1,tag2=val2]", memberPrefix);

        assertEquals(
                "[member=" + uuid.toString() + ",address=[127.0.0.1]:12345,tag1=val1,tag2=val2]",
                prefixedName
        );
    }

}
