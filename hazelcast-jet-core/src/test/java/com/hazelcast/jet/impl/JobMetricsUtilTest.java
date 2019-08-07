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

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.Util;
import com.hazelcast.nio.Address;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class JobMetricsUtilTest {

    @Test
    public void prefixNameWithMemberTags() throws Throwable {
        MemberInfo member = new MemberInfo(
                new Address("127.0.0.1", 12345),
                Util.idToString(1834287L),
                Collections.emptyMap(),
                MemberVersion.UNKNOWN
        );

        String memberPrefix = JobMetricsUtil.getMemberPrefix(member);
        String prefixedName = JobMetricsUtil.addPrefixToDescriptor("[tag1=val1,tag2=val2]", memberPrefix);

        assertEquals(
                "[member=0000-0000-001b-fd2f,address=[127.0.0.1]:12345,tag1=val1,tag2=val2]",
                prefixedName
        );
    }

}
