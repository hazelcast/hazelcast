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

package com.hazelcast.query.impl.extractor;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure;
import com.hazelcast.query.impl.predicates.AbstractPredicate;
import com.hazelcast.query.impl.predicates.PredicateTestUtils;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.StringUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Convenience class that enables creating short and readable extraction specification tests
 */
public class AbstractExtractionSpecification extends HazelcastTestSupport {

    /**
     * Parametrisation axis for indexing
     */
    public enum Index {
        NO_INDEX, UNORDERED, ORDERED
    }

    /**
     * Parametrisation axis for storage type: single-value, list, array, portable-array
     */
    public enum Multivalue {
        SINGLE,
        ARRAY,
        LIST,
        PORTABLE
    }

    public interface PortableAware {
        <T> T getPortable();
    }

    /**
     * Holder for objects used as input data in the extraction specification test
     */
    protected static class Input {
        Object[] objects;

        public static Input of(Object... objects) {
            Input input = new Input();
            input.objects = objects;
            return input;
        }
    }

    /**
     * Query executed extraction specification test
     */
    protected static class Query {
        AbstractPredicate predicate;
        String expression;

        public static Query of(Predicate predicate, Multivalue mv) {
            AbstractPredicate ap = (AbstractPredicate) predicate;
            Query query = new Query();
            query.expression = parametrize(PredicateTestUtils.getAttributeName(ap), mv);
            PredicateTestUtils.setAttributeName((AbstractPredicate) predicate, query.expression);
            query.predicate = ap;
            return query;
        }
    }

    /**
     * Holder for objects used as expected output in the extraction specification test
     */
    protected static class Expected {
        Object[] objects;
        Class<? extends Throwable>[] throwables;

        public static Expected of(Object... objects) {
            Expected expected = new Expected();
            expected.objects = objects;
            return expected;
        }

        public static Expected of(Class<? extends Throwable> throwable) {
            Expected expected = new Expected();
            expected.throwables = new Class[]{throwable};
            return expected;
        }

        public static Expected of(Class<? extends Throwable>... throwables) {
            Expected expected = new Expected();
            expected.throwables = throwables;
            return expected;
        }

        public static Expected empty() {
            Expected expected = new Expected();
            expected.objects = new ComplexTestDataStructure.Person[0];
            return expected;
        }
    }

    /**
     * Parametrises queries replacing underscore to underscore_collection_type.
     * e.g. limb_[0], may be replaced to limb_array[0] or limb_list[0].
     * Of course the underlying lists or arrays has to exists in the test datastructure.
     * Parametrisation enables reusing the same tests for multiple multi-values types.
     */
    protected static String parametrize(String expression, AbstractExtractionTest.Multivalue mv) {
        if (expression != null && !expression.contains("__")) {
            return expression.replaceAll("_", "_" + mv.name().toLowerCase(StringUtil.LOCALE_INTERNAL));
        }
        return expression;
    }

    /**
     * Generates combinations of parameters and outputs them in the *horrible* JUnit format
     */
    protected static Collection<Object[]> axes(List<InMemoryFormat> formats, List<Index> indexes,
                                               List<Multivalue> multivalues) {
        List<Object[]> combinations = new ArrayList<Object[]>();
        for (InMemoryFormat inMemoryFormat : formats) {
            for (Index index : indexes) {
                for (Multivalue multivalue : multivalues) {
                    combinations.add(new Object[]{inMemoryFormat, index, multivalue});
                }
            }
        }
        return combinations;
    }
}
