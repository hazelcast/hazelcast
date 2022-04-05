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

package com.hazelcast.cache;

/**
 * This exception class is thrown while creating {@link com.hazelcast.cache.impl.CacheRecordStore}
 * instances but the cache config does not exist on the node to create the instance on. This can
 * happen in either of two cases:<br>
 * <ul>
 *     <li>the cache's config is not yet distributed to the node, or</li>
 *     <li>the cache has been already destroyed.</li>
 * </ul><br>
 * For the first option, the caller can decide to just retry the operation a couple of times since
 * distribution is executed in a asynchronous way.
 */
public class CacheNotExistsException extends IllegalStateException {

    public CacheNotExistsException(String s) {
        super(s);
    }
}
