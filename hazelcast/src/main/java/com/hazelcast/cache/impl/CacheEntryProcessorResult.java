/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

/**
 * Implementation of {@link EntryProcessorResult} wrapping the result or exception which serves a response wrapper return from
 * {@link CacheEntryProcessorEntry}
 *
 * @param <T> the type of the return value
 * @see CacheEntryProcessorEntry
 */
public class CacheEntryProcessorResult<T>
        implements EntryProcessorResult<T> {

    private T result;
    private Throwable exception;

    public CacheEntryProcessorResult(T result) {
        this.result = result;
    }

    public CacheEntryProcessorResult(Throwable exception) {
        this.exception = exception;
    }

    @Override
    public T get()
            throws EntryProcessorException {
        if (result != null) {
            return result;
        }
        throw new EntryProcessorException(exception);
    }

}
