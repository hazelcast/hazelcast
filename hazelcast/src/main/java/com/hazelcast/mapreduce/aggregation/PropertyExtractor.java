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

import com.hazelcast.spi.annotation.Beta;

import java.io.Serializable;

/**
 * The PropertyExtractor interface is used in suppliers to retrieve values from
 * input value types and extract or transform them to some other type to be used
 * in aggregations.
 * <p/>
 * For Java 6 and 7:
 * <pre>
 * public class EmployeeIntTransformer implements PropertyExtractor&lt;Employee, Integer> {
 *   public Integer extract(Employee value) {
 *       return value.getSalaryPerMonth();
 *   }
 * }
 *
 * Supplier supplier = Supplier.all(new EmployeeIntTransformer());
 * </pre>
 * <p/>
 * Or in Java 8:
 * <pre>
 * Supplier supplier = Supplier.all((value) -> value.getSalaryPerMonth());
 * </pre>
 *
 * @param <ValueIn>  the input value type
 * @param <ValueOut> the extracted / transformed value type
 * @since 3.3
 */
@Beta
public interface PropertyExtractor<ValueIn, ValueOut>
        extends Serializable {

    /**
     * This method is called for every value that is about to be supplied to
     * an aggregation (maybe filtered through an {@link com.hazelcast.query.Predicate}
     * or {@link com.hazelcast.mapreduce.KeyPredicate}). It is responsible to either
     * transform the input value to an type of {@link ValueOut} or to extract an
     * attribute of this type.
     *
     * @param value the input value
     * @return the extracted / transformed value
     */
    ValueOut extract(ValueIn value);
}
