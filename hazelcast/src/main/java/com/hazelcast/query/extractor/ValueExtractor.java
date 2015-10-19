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
 * Common superclass for all extractors that enable the users to define custom attributes and extract their values.
 * The extraction logic may just extract the underlying value or group, reduce or transform it.
 * <p/>
 * How to use a ValueExtractor?
 * <p/>
 * First, extend this class and implement the com.hazelcast.query.extractorValueExtractor#extract method.
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
 * Please, bear in mind that extractor may not be added while the map has been instantiated.
 * All extractor have to be defined upfront int he map's initial configuration.
 */
public abstract class ValueExtractor {

    /**
     * No-arg constructor required for the runtime instantiation of the extractor
     */
    public ValueExtractor() {
    }

    /**
     * Extracts a value from the given target object.
     * <p/>
     * May return:
     * <ul>
     * <li>a single 'single-value' result (not a collection)</li>
     * <li>a single 'multi-value' result (a collection)</li>
     * <li>multiple 'single-value' or 'multi-value' results encompassed in
     * (@see com.hazelcast.query.extractor.MultiResult)</li>
     * </ul>
     * <p/>
     * MultiResult is an aggregate of results that is returned if the extractor returns multiple results
     * due to a reduce operation executed on a hierarchy of values.
     *
     * @param target object to extract the value from
     * @return extracted value
     */
    public abstract Object extract(Object target);

}
