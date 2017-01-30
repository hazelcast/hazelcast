/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * <p>
 * Marker interface for indicating that key and value wrapped by
 * {@link com.hazelcast.cache.CacheEntryView} will be not converted to their original types.
 * </p>
 *
 * <p>
 * The motivation of this interface is that generally while merging cache entries, actual key and value is not checked.
 * So no need to convert them to their original types.
 * </p>
 *
 * <p>
 * At worst case, value is returned from the merge method as selected and this means that at all cases
 * value is accessed. So even the the convertion is done as lazy, it will be processed at this point.
 * But by default, they (key and value) converted to their original types
 * unless this {@link com.hazelcast.cache.StorageTypeAwareCacheMergePolicy} is used.
 * </p>
 *
 * <p>
 * Another motivation for using this interface is that at server side
 * there is no need to locate classes of stored entries.
 * It means that entries can be put from client with `BINARY` in-memory format and
 * classpath of client can be different from server.
 * So in this case, if entries are tried to convert to their original types while merging,
 * {@link java.lang.ClassNotFoundException} is thrown here.
 * </p>
 *
 * <p>
 * As a result, both of performance and {@link java.lang.ClassNotFoundException} as mentioned above,
 * it is strongly recommended to use this interface if original values of key and values are not needed.
 * </p>
 *
 * @see com.hazelcast.cache.CacheMergePolicy
 */
public interface StorageTypeAwareCacheMergePolicy extends CacheMergePolicy {

}
