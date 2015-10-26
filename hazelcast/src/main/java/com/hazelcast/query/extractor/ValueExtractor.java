/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.extractor;

/***
 * Common superclass for all extractors that enable the user to define custom attributes and extract their values.
 * The extraction logic may just extract the underlying value or group, reduce or transform it.
 * <p/>
 * How to use a ValueExtractor?
 * <p/>
 * First, extend this class and implement the @see com.hazelcast.query.extractor.ValueExtractor#extract method.
 * Then, define a new attribute with the above-mentioned extractor in the configuration of the map.
 * <p/>
 * How to define a new custom attribute?
 * <code>
 * MapAttributeConfig attributeConfig = new MapAttributeConfig();
 * extractorConfig.setName("currency");
 * extractorConfig.setExtractor("com.bank.CurrencyExtractor");
 * </code>
 * <p/>
 * How to register the newly-defined attribute in a configuration of a Map?
 * <code>
 * MapConfig mapConfig = (...);
 * mapConfig.addMapAttributeConfig(attributeConfig);
 * </code>
 * Extractors may be also defined in the XML configuration.
 * <p/>
 * Please, bear in mind that an extractor may not be added after the map has been instantiated.
 * All extractor have to be defined upfront in the map's initial configuration.
 * <p/>
 * Reflection-based extraction is the default mechanism - ValueExtractors are an alternative way of getting values
 * from objects.
 *
 * @param <T> type of the target object to extract the value from
 * @param <R> type of the result object - the extracted value
 * @see com.hazelcast.query.extractor.MultiResult
 */
public abstract class ValueExtractor<T, R> {

    /**
     * Extracts a value from the given target object.
     * <p/>
     * May return:
     * <ul>
     * <li>a single 'single-value' result (not a collection)</li>
     * <li>a single 'multi-value' result (a collection)</li>
     * <li>multiple 'single-value' or 'multi-value' results encompassed in a MultiResult</li>
     * </ul>
     * <p/>
     * MultiResult is an aggregate of results that is returned if the extractor returns multiple results
     * due to a reduce operation executed on a hierarchy of values.
     *
     * @param target object to extract the value from
     * @return extracted value
     * @see com.hazelcast.query.extractor.MultiResult
     */
    public abstract R extract(T target);

    /**
     * Factory method for the MultiResult
     *
     * @return a new instance of the MultiResult class
     * @see com.hazelcast.query.extractor.MultiResult
     */
    protected MultiResult<R> createMultiResult() {
        return new MultiResult<R>();
    }

}
