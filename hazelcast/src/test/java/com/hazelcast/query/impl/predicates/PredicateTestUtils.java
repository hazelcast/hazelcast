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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.internal.util.UuidUtil;
import org.mockito.internal.stubbing.answers.ReturnsArgumentAt;

import java.util.Map;

import static com.hazelcast.instance.impl.TestUtil.toData;
import static com.hazelcast.internal.util.Preconditions.checkInstanceOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Convenient utility methods to create mock predicates
 */
public final class PredicateTestUtils {

    private PredicateTestUtils() {
    }

    /**
     * Create a negatable mock predicate. The created mock predicate returns passed the predicate
     * passed as negation argument
     *
     * @param negation predicate to be return by created mock predicate on negate()
     * @return negatable predicate.
     */
    static Predicate createMockNegatablePredicate(Predicate negation) {
        NegatablePredicate negatablePredicate = mock(NegatablePredicate.class, withSettings().extraInterfaces(Predicate.class));
        when(negatablePredicate.negate()).thenReturn(negation);
        return (Predicate) negatablePredicate;
    }

    static Predicate createMockVisitablePredicate() {
        VisitablePredicate visitablePredicate = mock(VisitablePredicate.class, withSettings().extraInterfaces(Predicate.class));
        when(visitablePredicate.accept((Visitor) anyObject(), (Indexes) anyObject())).thenReturn((Predicate) visitablePredicate);
        return (Predicate) visitablePredicate;
    }

    static Predicate createMockVisitablePredicate(Predicate transformed) {
        VisitablePredicate visitablePredicate = mock(VisitablePredicate.class, withSettings().extraInterfaces(Predicate.class));
        when(visitablePredicate.accept((Visitor) anyObject(), (Indexes) anyObject())).thenReturn(transformed);
        return (Predicate) visitablePredicate;
    }

    static Visitor createPassthroughVisitor() {
        Visitor visitor = mock(Visitor.class);
        when(visitor.visit((AndPredicate) anyObject(), (Indexes) anyObject())).thenAnswer(new ReturnsArgumentAt(0));
        when(visitor.visit((OrPredicate) anyObject(), (Indexes) anyObject())).thenAnswer(new ReturnsArgumentAt(0));
        when(visitor.visit((NotPredicate) anyObject(), (Indexes) anyObject())).thenAnswer(new ReturnsArgumentAt(0));

        return visitor;
    }

    static Visitor createDelegatingVisitor(Predicate delegate) {
        Visitor visitor = mock(Visitor.class);
        when(visitor.visit((AndPredicate) anyObject(), (Indexes) anyObject())).thenReturn(delegate);
        when(visitor.visit((OrPredicate) anyObject(), (Indexes) anyObject())).thenReturn(delegate);
        when(visitor.visit((NotPredicate) anyObject(), (Indexes) anyObject())).thenReturn(delegate);

        return visitor;
    }

    public static String getAttributeName(Predicate predicate) {
        checkInstanceOf(AbstractPredicate.class, predicate);
        return ((AbstractPredicate) predicate).attributeName;
    }

    public static String setAttributeName(Predicate predicate, String attributeName) {
        checkInstanceOf(AbstractPredicate.class, predicate);
        ((AbstractPredicate) predicate).attributeName = attributeName;
        return attributeName;
    }

    public static Map.Entry entry(Object value) {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        return new QueryEntry(serializationService, toData(UuidUtil.newUnsecureUUID()),
                value, newExtractor(serializationService));
    }

    protected static Extractors newExtractor(InternalSerializationService serializationService) {
        return Extractors.newBuilder(serializationService).build();
    }

    public static Map.Entry entry(Object key, Object value) {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        return new QueryEntry(serializationService, toData(key), value,
                newExtractor(serializationService));
    }
}
