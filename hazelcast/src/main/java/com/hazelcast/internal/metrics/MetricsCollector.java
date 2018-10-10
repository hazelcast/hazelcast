/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics;

import com.hazelcast.spi.annotation.PrivateApi;

@PrivateApi
public interface MetricsCollector {

    /**
     * Appends the given key value pair to the output in the format specific to the
     * collector.
     *
     * @param key not null, only guaranteed to be stable throughout the call
     * @param value -1 for unknown, >= 0 otherwise (double encoded as 10000 x double
     *        value; boolean as 1/0)
     */
    void collect(CharSequence key, long value);

}
