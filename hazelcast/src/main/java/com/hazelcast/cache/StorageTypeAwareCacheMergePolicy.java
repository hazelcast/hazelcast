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
 * Marker interface for indicating that key and value wrapped by
 * {@link com.hazelcast.cache.CacheEntryView} will be not converted to their original types.
 * <p>
 * The motivation of this interface is that generally while merging cache entries, actual key and value are not checked.
 * So there is no need to convert them to their original types.
 * <p>
 * In the worst case, the value is returned from the merge method as selected, which means that in all cases
 * the value is accessed. So although the conversion is done lazily, it will be processed at this point.
 * But by default, the key and value are converted to their original types
 * unless this {@link com.hazelcast.cache.StorageTypeAwareCacheMergePolicy} is used.
 * <p>
 * Another motivation for using this interface is that at server side
 * there is no need to locate classes of stored entries.
 * It means that entries can be put from client with {@code BINARY} in-memory format and
 * classpath of client can be different from server.
 * So in this case, if entries are tried to convert to their original types while merging,
 * {@link java.lang.ClassNotFoundException} is thrown here.
 * <p>
 * As a result, both of performance and {@link java.lang.ClassNotFoundException} as mentioned above,
 * it is strongly recommended to use this interface if original values of key and values are not needed.
 *
 * @see com.hazelcast.cache.CacheMergePolicy
 */
public interface StorageTypeAwareCacheMergePolicy extends CacheMergePolicy {
}
