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
 * Then, define a new custom attribute with the above-mentioned extractor in the configuration of the map.
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
 * Reflection-based extraction is the default mechanism - ValueExtractors are an alternative way of extracting
 * attribute values from an object.
 *
 * @param <T> type of the target object to extract the value from
 * @param <K> type of the extraction arguments passed to the extract() method
 */
public abstract class ValueExtractor<T, K> {

    /**
     * Extracts a value from the given target object.
     * The method does not return any value since the extracted value is collected by the ValueCollector.<p/>
     * <p/>
     * In order to return multiple results from a single extraction just invoke the ValueCollector#collect method
     * multiple times, so that the collector collects all results.
     * <p/>
     * It sounds counter-intuitive, but a single extraction may return multiple values when arrays or collections are
     * involved.
     * <p/>
     * Let's have a look at the following data structure:
     * <code>
     * class Swap {
     * Leg legs[2];
     * }
     * <p/>
     * class Leg {
     * String currency;
     * }
     * </code>
     * <p/>
     * Let's assume that we want to extract currencies of all legs from a single Swap object. Each Swap has two Legs
     * so there are two currencies too. In order to return both values from the extraction operation just collect them
     * separately using the ValueCollector. Collecting multiple values in such a way allows us to operate on multiple
     * "reduced" values as if they were single-values during the evaluation of the predicates.
     * <p/>
     * If more than one value is returned from the extraction they are treated as 'multiple' single values anyway, but
     * it is enough if a single value evaluates the predicate's condition to true to return a match.
     * <p/>
     * Let's assume that we registered a custom extractor to under the name 'swapCurrency' and let's have a look at the
     * following query: 'swapCurrency = EUR'.
     * The extraction may return upp to two currencies for each Swap since each Swap has up to two Legs.
     * The default evaluation semantics of the predicates is that it is enough if a single value evaluates the condition
     * to true to return the Swap as a result of this query - so it will return a Swap if "any" of the currencies
     * matches the expression.
     *
     * @param target    object to extract the value from
     * @param collector collector of the extracted value(s)
     * @see ValueCollector
     */
    public abstract void extract(T target, Arguments<K> arguments, ValueCollector collector);

}
