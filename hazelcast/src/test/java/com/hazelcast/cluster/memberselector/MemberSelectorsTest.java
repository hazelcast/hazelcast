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

package com.hazelcast.cluster.memberselector;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MemberSelectorsTest {

    @Mock
    private Member member = mock(Member.class);

    @Test
    public void testLiteMemberSelector() {
        when(member.isLiteMember()).thenReturn(true, true);
        assertTrue(LITE_MEMBER_SELECTOR.select(member));
        assertFalse(DATA_MEMBER_SELECTOR.select(member));
    }

    @Test
    public void testDataMemberSelector() {
        assertFalse(LITE_MEMBER_SELECTOR.select(member));
        assertTrue(DATA_MEMBER_SELECTOR.select(member));
    }

    @Test
    public void testLocalMemberSelector() {
        when(member.localMember()).thenReturn(true, true);
        assertTrue(LOCAL_MEMBER_SELECTOR.select(member));
        assertFalse(NON_LOCAL_MEMBER_SELECTOR.select(member));
    }

    @Test
    public void testNonLocalMemberSelector() {
        assertFalse(LOCAL_MEMBER_SELECTOR.select(member));
        assertTrue(NON_LOCAL_MEMBER_SELECTOR.select(member));
    }

    @Test
    public void testAndMemberSelector() {
        when(member.localMember()).thenReturn(true);
        MemberSelector selector = MemberSelectors.and(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR);
        assertFalse(selector.select(member));
        verify(member).localMember();
        verify(member).isLiteMember();
    }

    @Test
    public void testAndMemberSelector2() {
        MemberSelector selector = MemberSelectors.and(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR);
        assertFalse(selector.select(member));
        verify(member).localMember();
        verify(member, never()).isLiteMember();
    }

    @Test
    public void testAndMemberSelector3() {
        when(member.localMember()).thenReturn(true);
        when(member.isLiteMember()).thenReturn(true);
        MemberSelector selector = MemberSelectors.and(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR);
        assertTrue(selector.select(member));
        verify(member).localMember();
        verify(member).isLiteMember();
    }

    @Test
    public void testOrMemberSelector() {
        when(member.localMember()).thenReturn(true);
        MemberSelector selector = MemberSelectors.or(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR);
        assertTrue(selector.select(member));
        verify(member).localMember();
        verify(member, never()).isLiteMember();
    }

    @Test
    public void testOrMemberSelector2() {
        MemberSelector selector = MemberSelectors.or(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR);
        assertFalse(selector.select(member));
        verify(member).localMember();
        verify(member).isLiteMember();
    }

    @Test
    public void testOrMemberSelector3() {
        when(member.localMember()).thenReturn(true);
        when(member.isLiteMember()).thenReturn(true);
        MemberSelector selector = MemberSelectors.or(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR);
        assertTrue(selector.select(member));
        verify(member).localMember();
        verify(member, never()).isLiteMember();
    }

    @Test
    public void testOrMemberSelector4() {
        when(member.isLiteMember()).thenReturn(true);
        MemberSelector selector = MemberSelectors.or(LOCAL_MEMBER_SELECTOR, LITE_MEMBER_SELECTOR);
        assertTrue(selector.select(member));
        verify(member).localMember();
        verify(member).isLiteMember();
    }

}
