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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.internal.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LikePredicateTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testILikePredicateUnicodeCase() {
        assertTrue(new ILikePredicate("this", "Hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new ILikePredicate("this", "hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new ILikePredicate("this", "Хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
        assertTrue(new ILikePredicate("this", "хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateUnicodeCase() {
        assertTrue(new LikePredicate("this", "Hazelcast%").apply(entry("Hazelcast is here!")));
        assertFalse(new LikePredicate("this", "hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "Хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
        assertFalse(new LikePredicate("this", "хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateSyntax() {
        assertTrue(new LikePredicate("this", "%Hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "%here_").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "%%").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "%%").apply(entry("")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateSyntaxEscape() {
        assertTrue(new LikePredicate("this", "%\\_is\\_%").apply(entry("Hazelcast_is_here!")));
        assertTrue(new LikePredicate("this", "%is\\%here!").apply(entry("Hazelcast%is%here!")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void negative_testLikePredicateSyntax() {
        assertFalse(new LikePredicate("this", "_Hazelcast%").apply(entry("Hazelcast is here!")));
        assertFalse(new LikePredicate("this", "_").apply(entry("")));
        assertFalse(new LikePredicate("this", "Hazelcast%").apply(entry("")));
    }

    @Test
    public void likePredicateIsNotIndexed_whenBitmapIndexIsUsed() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.BITMAP));

        assertFalse(new LikePredicate("this", "string%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenHashIndexIsUsed() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.HASH));

        assertFalse(new LikePredicate("this", "string%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenUnderscoreWildcardIsUsed() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.SORTED));

        assertFalse(new LikePredicate("this", "string_").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsUsedAtTheBeginning() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.SORTED));

        assertFalse(new LikePredicate("this", "%string").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsUsedMultipleTimes() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.SORTED));

        assertFalse(new LikePredicate("this", "sub%string%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsEscaped() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.SORTED));

        assertFalse(new LikePredicate("this", "sub\\%").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub\\\\\\%").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub\\%string\\%").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub\\str\\%").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsNotIndexed_whenPercentWildcardIsNotTheLastSymbol() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.SORTED));

        assertFalse(new LikePredicate("this", "sub%str").isIndexed(queryContext));
        assertFalse(new LikePredicate("this", "sub%   ").isIndexed(queryContext));
    }

    @Test
    public void likePredicateIsIndexed_whenPercentWildcardIsUsed_andIndexIsSorted() {
        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.matchIndex("this", QueryContext.IndexMatchHint.PREFER_ORDERED)).thenReturn(createIndex(IndexType.SORTED));

        assertTrue(new LikePredicate("this", "sub%").isIndexed(queryContext));
        assertTrue(new LikePredicate("this", "sub\\\\%").isIndexed(queryContext));
        assertTrue(new LikePredicate("this", "sub\\%string%").isIndexed(queryContext));
        assertTrue(new LikePredicate("this", "sub\\_string%").isIndexed(queryContext));
    }

    @Nonnull
    private Index createIndex(IndexType indexType) {
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        Extractors mockExtractors = Extractors.newBuilder(mockSerializationService).build();
        IndexConfig config = IndexUtils.createTestIndexConfig(indexType, "this");
        return new IndexImpl(
                config,
                mockSerializationService,
                mockExtractors,
                IndexCopyBehavior.COPY_ON_READ,
                PerIndexStats.EMPTY,
                MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT
        );
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(LikePredicate.class)
            .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
            .withRedefinedSuperclass()
            .verify();
    }

}
