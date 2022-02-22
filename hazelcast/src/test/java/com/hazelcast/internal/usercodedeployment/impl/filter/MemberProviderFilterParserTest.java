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

package com.hazelcast.internal.usercodedeployment.impl.filter;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.util.filter.AlwaysApplyFilter;
import com.hazelcast.internal.util.filter.Filter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberProviderFilterParserTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(MemberProviderFilterParser.class);
    }

    @Test
    public void whenStringIsNull_thenReturnAlwaysMatchingInstance() {
        Filter<Member> memberFilter = MemberProviderFilterParser.parseMemberFilter(null);
        assertTrue(memberFilter instanceof AlwaysApplyFilter);
    }

    @Test
    public void whenStringIsWhitespace_thenReturnAlwaysMatchingInstance() {
        Filter<Member> memberFilter = MemberProviderFilterParser.parseMemberFilter("  ");
        assertTrue(memberFilter instanceof AlwaysApplyFilter);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenStringHasUnknownPrefix_thenThrowIllegalArgumentException() {
        MemberProviderFilterParser.parseMemberFilter("FOOO");
    }

    @Test
    public void givenMemberAttributeFilterIsUsed_whenMemberAttributeIsPresent_thenFilterMatches() {
        Filter<Member> memberFilter = MemberProviderFilterParser.parseMemberFilter("HAS_ATTRIBUTE:foo");
        Map<String, String> attributes = ImmutableMap.of(
                "foo", "bar"
        );
        Member mockMember = createMockMemberWithAttributes(attributes);
        assertTrue(memberFilter.accept(mockMember));
    }

    @Test
    public void givenMemberAttributeFilterIsUsed_whenMemberAttributeIsNotPresent_thenFilterDoesNotMatch() {
        Filter<Member> memberFilter = MemberProviderFilterParser.parseMemberFilter("HAS_ATTRIBUTE:foo");
        Map<String, String> attributes = ImmutableMap.of(
                "bar", "other"
        );
        Member mockMember = createMockMemberWithAttributes(attributes);
        assertFalse(memberFilter.accept(mockMember));
    }

    private static Member createMockMemberWithAttributes(Map<String, String> attributes) {
        Member mockMember = mock(Member.class);
        when(mockMember.getAttributes()).thenReturn(attributes);
        return mockMember;
    }
}
