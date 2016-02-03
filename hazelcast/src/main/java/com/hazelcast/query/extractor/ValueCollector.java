/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * Enables collecting values extracted by a {@see com.hazelcast.query.extractor.ValueExtractor}
 */
public abstract class ValueCollector {

    /**
     * Collects a value extracted by a ValueExtractor.
     * <p/>
     * More than one value may be collected in a single extraction, for more details see
     * {@link com.hazelcast.query.extractor.ValueExtractor}
     *
     * @param value value to be collected
     */
    public abstract void addObject(Object value);

}
