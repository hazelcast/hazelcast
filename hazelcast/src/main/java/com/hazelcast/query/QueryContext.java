/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

/**
 * Provides the context for queries execution.
 */
public interface QueryContext {

    /**
     * Obtains the index available for the given attribute in this query
     * context.
     *
     * @param attribute the attribute to obtain the index for.
     * @return the obtained index or {@code null} if there is no index available
     * for the given attribute.
     */
    Index getIndex(String attribute);
}
