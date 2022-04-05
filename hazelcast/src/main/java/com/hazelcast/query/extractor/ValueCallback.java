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
 * Enables notifying about values extracted in a {@link com.hazelcast.query.extractor.ValueExtractor}
 *
 * @param <T> type of the extracted value
 */
@FunctionalInterface
public interface ValueCallback<T> {

    /**
     * Notifies about a value passed as an argument
     *
     * @param value value to be notified about
     */
    void onResult(T value);

}
