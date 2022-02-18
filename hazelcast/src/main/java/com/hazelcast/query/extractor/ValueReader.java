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

/**
 * Enables reading the value of the attribute specified by the path
 *
 * <p>The path may be:<ol>
 *   <li>simple -&gt; it includes a single attribute only, like "name"
 *   <li>nested -&gt; it includes more than a single attribute separated with a dot (.), e.g. "person.address.city"
 * </ol>
 *
 * The path may also include array elements:<ol>
 *   <li>specific quantifier, like "person.leg[1]" -&gt; returns the leg with index 1
 *   <li>wildcard quantifier, like "person.leg[any]" -&gt; returns all legs
 * </ol>
 *
 * <p>The wildcard quantifier may be used multiple times, like "person.leg[any].finger[any]" which returns all fingers
 * from all legs.
 */
public interface ValueReader {

    /**
     * Read the value of the attribute specified by the path and returns the result via the callback.
     *
     * @param path     attribute to read the value from
     * @param callback callback to call with the value. May be called more than once in case of wildcards.
     * @param <T>      Type of the value to read
     * @throws ValueReadingException in case of any reading errors. If an exception occurs the callback won't
     *                               be called at all
     */
    <T> void read(String path, ValueCallback<T> callback) throws ValueReadingException;

    /**
     * Read the value of the attribute specified by the path and returns the result directly to the collector.
     *
     * @param path      attribute to read the value from
     * @param collector collector to collect the result with. May collect more than one result in case of wildcards.
     * @param <T>       Type of the value to read
     * @throws ValueReadingException in case of any reading errors. If an exception occurs the collector won't
     *                               be called at all
     */
    <T> void read(String path, ValueCollector<T> collector) throws ValueReadingException;

}
