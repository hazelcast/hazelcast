/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.aggregation;

import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.aggregation.impl.AcceptAllSupplier;
import com.hazelcast.query.Predicate;

import java.util.Map;

public abstract class Supplier<KeyIn, ValueIn, ValueOut> {

    public abstract ValueOut apply(Map.Entry<KeyIn, ValueIn> entry);

    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> all() {
        return new AcceptAllSupplier(null);
    }

    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> all(
            PropertyExtractor<ValueIn, ValueOut> propertyExtractor) {

        return new AcceptAllSupplier(propertyExtractor);
    }

    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromPredicate(
            Predicate<KeyIn, ValueIn> predicate) {

        return new PredicateSupplier<KeyIn, ValueIn, ValueOut>(predicate);
    }

    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromPredicate(Predicate<KeyIn, ValueIn> predicate,
                                                                                              Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier) {

        return new PredicateSupplier<KeyIn, ValueIn, ValueOut>(predicate, chainedSupplier);
    }

    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromKeyPredicate(
            KeyPredicate<KeyIn> keyPredicate) {

        return new KeyPredicateSupplier<KeyIn, ValueIn, ValueOut>(keyPredicate);
    }

    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromKeyPredicate(KeyPredicate<KeyIn> keyPredicate,
                                                                                                 Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier) {

        return new KeyPredicateSupplier<KeyIn, ValueIn, ValueOut>(keyPredicate, chainedSupplier);
    }

    private static class PredicateSupplier<KeyIn, ValueIn, ValueOut>
            extends Supplier<KeyIn, ValueIn, ValueOut> {

        private final Predicate<KeyIn, ValueIn> predicate;
        private final Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier;

        private PredicateSupplier(Predicate<KeyIn, ValueIn> predicate) {
            this(predicate, null);
        }

        private PredicateSupplier(Predicate<KeyIn, ValueIn> predicate, Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier) {
            this.predicate = predicate;
            this.chainedSupplier = chainedSupplier;
        }

        @Override
        public ValueOut apply(Map.Entry<KeyIn, ValueIn> entry) {
            if (predicate.apply(entry)) {
                ValueIn value = entry.getValue();
                return chainedSupplier != null ? chainedSupplier.apply(entry) : (ValueOut) value;
            }
            return null;
        }
    }

    private static class KeyPredicateSupplier<KeyIn, ValueIn, ValueOut>
            extends Supplier<KeyIn, ValueIn, ValueOut> {

        private final KeyPredicate<KeyIn> keyPredicate;
        private final Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier;

        private KeyPredicateSupplier(KeyPredicate<KeyIn> keyPredicate) {
            this(keyPredicate, null);
        }

        private KeyPredicateSupplier(KeyPredicate<KeyIn> keyPredicate, Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier) {
            this.keyPredicate = keyPredicate;
            this.chainedSupplier = chainedSupplier;
        }

        @Override
        public ValueOut apply(Map.Entry<KeyIn, ValueIn> entry) {
            if (keyPredicate.evaluate(entry.getKey())) {
                ValueIn value = entry.getValue();
                return chainedSupplier != null ? chainedSupplier.apply(entry) : (ValueOut) value;
            }
            return null;
        }
    }
}
