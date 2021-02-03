/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

/**
 * Provides query parameter metadata.
 */
public final class QueryParameterMetadata {

    public static final QueryParameterMetadata EMPTY = new QueryParameterMetadata();

    private final ParameterConverter[] parameterConverters;

    public QueryParameterMetadata(ParameterConverter... parameterConverters) {
        this.parameterConverters = parameterConverters;
    }

    /**
     * @return the number of parameters this metadata describes.
     */
    public int getParameterCount() {
        return parameterConverters.length;
    }

    public ParameterConverter getParameterConverter(int index) {
        return parameterConverters[index];
    }
}
