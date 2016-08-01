package com.hazelcast.internal.distributedclassloading.impl.filter;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.Member;
import com.hazelcast.internal.util.filter.AlwaysApplyFilter;
import com.hazelcast.internal.util.filter.Filter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberProviderFilterParserTest extends HazelcastTestSupport {

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
        Filter<Member> memberFilter = MemberProviderFilterParser.parseMemberFilter("FOOO");
    }

    @Test
    public void givenMemberAttributeFilterIsUsed_whenMemberAttributeIsPresent_thenFilterMatches() {
        Filter<Member> memberFilter = MemberProviderFilterParser.parseMemberFilter("HAS_ATTRIBUTE:foo");
        Map<String, Object> attributes = ImmutableMap.of(
                "foo", (Object)"bar"
        );
        Member mockMember = createMockMemberWithAttributes(attributes);
        assertTrue(memberFilter.accept(mockMember));
    }

    @Test
    public void givenMemberAttributeFilterIsUsed_whenMemberAttributeIsNotPresent_thenFilterDoesNotMatch() {
        Filter<Member> memberFilter = MemberProviderFilterParser.parseMemberFilter("HAS_ATTRIBUTE:foo");
        Map<String, Object> attributes = ImmutableMap.of(
                "bar", (Object)"other"
        );
        Member mockMember = createMockMemberWithAttributes(attributes);
        assertFalse(memberFilter.accept(mockMember));
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(MemberProviderFilterParser.class);
    }

    private static Member createMockMemberWithAttributes(Map<String, Object> attributes) {
        Member mockMember = mock(Member.class);
        when(mockMember.getAttributes()).thenReturn(attributes);
        return mockMember;
    }

}