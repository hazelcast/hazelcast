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
import com.hazelcast.mapreduce.aggregation.impl.KeyPredicateSupplier;
import com.hazelcast.mapreduce.aggregation.impl.PredicateSupplier;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.annotation.Beta;

import java.io.Serializable;
import java.util.Map;

/**
 * The Supplier interface is used to supply values from input data structures like
 * {@link com.hazelcast.core.IMap} or {@link com.hazelcast.core.MultiMap} to an
 * aggregation. Suppliers can also be used to filter elements based on keys or values
 * using {@link com.hazelcast.mapreduce.KeyPredicate}s or
 * {@link com.hazelcast.query.Predicate}s.<br/>
 * Additionally you can provide a {@link com.hazelcast.mapreduce.aggregation.PropertyExtractor}
 * implementation to extract or transform selected values to another value type. This is necessary
 * if you don't want to aggregate the actual value type of the maps but an attribute of it.
 * <p/>
 * The following examples using Java 8 Lambda syntax but everything is fully Java 6 or 7 compatible
 * using implementations of the interfaces.
 * <pre>
 *     // Select all values, no need to transform
 *     Supplier supplier = Supplier.all();
 *
 *     // Select all values but transform to Integer
 *     Supplier supplier = Supplier.all((value) -> value.intValue());
 *
 *     // Select only values where the value is bigger than 50
 *     Supplier supplier = Supplier.fromPredicate((entry) -> entry.getValue().intValue() > 50);
 *
 *     // Select only values where the value is bigger than 50 and than chain it to a
 *     // supplier transformation to transform it into an Integer
 *     Supplier supplier = Supplier.fromPredicate((entry) -> entry.getValue().intValue() > 50,
 *                                                Supplier.all((value) -> value.intValue()));
 *
 *     // Select only values where the key starts with "Foo"
 *     Supplier supplier = Supplier.fromKeyPredicate((key) -> key.startsWith("Foo"));
 *
 *     // Select only values where the key starts with "Foo" and than chain it to a
 *     // supplier transformation to transform it into an Integer
 *     Supplier supplier = Supplier.fromKeyPredicate((key) -> key.startsWith("Foo"),
 *                                                   Supplier.all((value) -> value.intValue()));
 * </pre>
 *
 * @param <KeyIn>    the input key type
 * @param <ValueIn>  the input value type
 * @param <ValueOut> the supplied value type
 * @since 3.3
 */
@Beta
public abstract class Supplier<KeyIn, ValueIn, ValueOut>
        implements Serializable {

    /**
     * The apply method is used to apply the actual filtering or extraction / transformation
     * to the input entry.<br/>
     * If the input value should be ignored by the aggregation, the Supplier has to return
     * <pre>null</pre> as the supplied value, therefor <pre>null</pre> is not a legal value itself!<br/>
     * All custom suppliers have to be serializable, if they hold internal state remember that
     * the same supplier instance might be used from multiple threads concurrently.
     *
     * @param entry the entry key-value pair to supply to the aggregation
     * @return the supplied value or null if value should be ignored
     */
    public abstract ValueOut apply(Map.Entry<KeyIn, ValueIn> entry);

    /**
     * The predefined Supplier selects all values and does not perform any kind of data
     * transformation. Input value types need to match the aggregations expected value
     * type to make this Supplier work.
     *
     * @param <KeyIn>    the input key type
     * @param <ValueIn>  the input value type
     * @param <ValueOut> the supplied value type
     * @return all values from the underlying data structure as stored
     */
    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> all() {
        return new AcceptAllSupplier(null);
    }

    /**
     * The predefined Supplier selects all values and performs the given
     * {@link com.hazelcast.mapreduce.aggregation.PropertyExtractor}s transformation to the
     * input data. The returned value's type of the transformation needs to match the expected
     * value type of the aggregation.
     *
     * @param <KeyIn>    the input key type
     * @param <ValueIn>  the input value type
     * @param <ValueOut> the supplied value type
     * @return all values from the underlying data structure transformed using the given PropertyExtractor
     */
    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> all(
            PropertyExtractor<ValueIn, ValueOut> propertyExtractor) {

        return new AcceptAllSupplier(propertyExtractor);
    }

    /**
     * The predefined Supplier selects values using the given {@link com.hazelcast.query.Predicate}
     * and does not perform any kind of data transformation. Input value types need to match the
     * aggregations expected value type to make this Supplier work.
     *
     * @param <KeyIn>    the input key type
     * @param <ValueIn>  the input value type
     * @param <ValueOut> the supplied value type
     * @return selected values from the underlying data structure as stored
     */
    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromPredicate(
            Predicate<KeyIn, ValueIn> predicate) {

        return new PredicateSupplier<KeyIn, ValueIn, ValueOut>(predicate);
    }

    /**
     * The predefined Supplier selects values using the given {@link com.hazelcast.query.Predicate}
     * and chains the filtered value to the given Supplier which might perform data transformation.
     *
     * @param <KeyIn>    the input key type
     * @param <ValueIn>  the input value type
     * @param <ValueOut> the supplied value type
     * @return all values from the underlying data structure, possibly transformed using the chains Supplier
     */
    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromPredicate(Predicate<KeyIn, ValueIn> predicate,
                                             Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier) {

        return new PredicateSupplier<KeyIn, ValueIn, ValueOut>(predicate, chainedSupplier);
    }

    /**
     * The predefined Supplier selects values using the given {@link com.hazelcast.mapreduce.KeyPredicate}
     * and does not perform any kind of data transformation. Input value types need to match the
     * aggregations expected value type to make this Supplier work.
     *
     * @param <KeyIn>    the input key type
     * @param <ValueIn>  the input value type
     * @param <ValueOut> the supplied value type
     * @return selected values from the underlying data structure as stored
     */
    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromKeyPredicate(
            KeyPredicate<KeyIn> keyPredicate) {

        return new KeyPredicateSupplier<KeyIn, ValueIn, ValueOut>(keyPredicate);
    }

    /**
     * The predefined Supplier selects values using the given {@link com.hazelcast.mapreduce.KeyPredicate}
     * and chains the filtered value to the given Supplier which might perform data transformation.
     *
     * @param <KeyIn>    the input key type
     * @param <ValueIn>  the input value type
     * @param <ValueOut> the supplied value type
     * @return all values from the underlying data structure, possibly transformed using the chains Supplier
     */
    public static <KeyIn, ValueIn, ValueOut> Supplier<KeyIn, ValueIn, ValueOut> fromKeyPredicate(KeyPredicate<KeyIn> keyPredicate,
                                             Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier) {

        return new KeyPredicateSupplier<KeyIn, ValueIn, ValueOut>(keyPredicate, chainedSupplier);
    }
}
