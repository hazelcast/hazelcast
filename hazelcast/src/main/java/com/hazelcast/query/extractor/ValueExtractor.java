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

package com.hazelcast.query.extractor;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.impl.ArgumentParser;

/***
 * Common superclass for all extractors that enable the user to define custom attributes and extract their values.
 * The extraction logic may just extract the underlying value or group, reduce or transform it.
 * <p>
 * How to use a ValueExtractor?
 * <p>
 * First, extend this class and implement the @see com.hazelcast.query.extractor.ValueExtractor#extract method.
 * Then, define a new custom attribute referencing the implemented extractor in the configuration of the map.
 * <p>
 * How to define a new custom attribute?
 * <code>
 * AttributeConfig attributeConfig = new AttributeConfig();
 * extractorConfig.setName("currency");
 * extractorConfig.setExtractorClassName("com.bank.CurrencyExtractor");
 * </code>
 * <p>
 * How to register the newly-defined attribute in a configuration of a Map?
 * <code>
 * MapConfig mapConfig = (...);
 * mapConfig.addAttributeConfig(attributeConfig);
 * </code>
 * Extractors may be also defined in the XML configuration.
 * <pre>
 * &lt;map name="trades"&gt;
 *   &lt;attributes&gt;
 *     &lt;attribute extractor-class-name="com.bank.CurrencyExtractor"&gt;currency&lt;/attribute&gt;
 *   &lt;/attributes&gt;
 * &lt;/map&gt;
 * </pre>
 * <p>
 * Please, bear in mind that an extractor may not be added after a map has been instantiated.
 * All extractors have to be defined upfront in the map's initial configuration.
 * <p>
 * A ValueExtractor may use a custom argument if it is specified in the query.
 * The custom argument may be passed within the square brackets located after the name of the attribute
 * that uses a ValueExtractor, like: customAttribute[argumentString]
 * <p>
 * Let's have a look at the following query: 'currency[incoming] == EUR'
 * Let's assume that currency is an custom attribute that uses com.test.CurrencyExtractor for extraction.
 * The string "incoming" is an argument that will be passed to the {@link ArgumentParser} during the extraction.
 * The parser will parse the string according to the parser's custom logic and it will return a parsed object.
 * The parsed object may be a single object, array, collection, etc. It's up to the ValueExtractor implementor's
 * to understand the semantics of the parsed argument object.
 * <p>
 * Reflection-based extraction is the default mechanism - ValueExtractors are an alternative way of extracting
 * attribute values from an object.
 * <p>
 * It is also possible to use a ValueExtractor with a Portable data format. In this case the target object passed to the
 * extractor implements a {@link ValueReader} interface.
 *
 * @param <T> type of the target object to extract the value from
 * @param <A> type of the extraction argument object passed to the extract() method
 */
@FunctionalInterface
public interface ValueExtractor<T, A> {

    /**
     * Extracts a value from the given target object.
     * <p>
     * The method does not return any value since the extracted value may be collected by the ValueCollector#collect
     * method.
     * <p>
     * In order to return multiple results from a single extraction just invoke the ValueCollector#collect method
     * multiple times, so that the collector collects all results.
     * <p>
     * If the extraction is executed for an Object that implements a {@link Portable} interface the target object
     * passed to the extractor is an instance of a {@link ValueReader} enabling extraction from the Portable byte stream.
     * <p>
     * It sounds counter-intuitive, but a single extraction may return multiple values when arrays or collections are
     * involved.
     * <p>
     * Let's have a look at the following data structure:
     * <pre>
     * class Motorbike {
     *     Wheel wheel[2];
     * }
     * class Wheel {
     *     String name;
     * }
     * </pre>
     * Let's assume that we want to extract the names of all wheels from a single motorbike object. Each motorbike has
     * two wheels so there are two names too. In order to return both values from the extraction operation just collect
     * them separately using the ValueCollector. Collecting multiple values in such a way allows operating on these
     * multiple values as if they were single-values during the evaluation of the predicates.
     * <p>
     * Let's assume that we registered a custom extractor to under the name 'wheelName' and executed the following query:
     * 'wheelName = front-wheel'.
     * <p>
     * The extraction may return up to two wheel names for each Motorbike since each Motorbike has up to two wheels.
     * In such a case, it is enough if a single value evaluates the predicate's condition to true to return a match, so
     * it will return a Motorbike if 'any' of the wheels matches the expression.
     *
     * @param target    object to extract the value from. In case the target object implements {@link Portable}
     *                  interface the target object passed to this method is an instance of a {@link ValueReader}.
     * @param argument  extraction argument
     * @param collector collector of the extracted value(s)
     * @see ValueCollector
     */
    void extract(T target, A argument, ValueCollector collector);

}
