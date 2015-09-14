/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * {@link com.hazelcast.cache.CacheEntryView} will be converted to their original types.
 * </p>
 *
 * <p>
 * The motivation of this interface is that generally while merging cache entries, actual key and value is not checked.
 * So no need to convert them to their original types in general.
 * </p>
 *
 * <p>
 * At worst case value is returned from the merge method as selected and this means that at all cases
 * value is accessed. So even the the convertion is done as lazy, it will be processed at this point.
 * But by default, their (key and value) storage types are used unless this {@link com.hazelcast.cache.CacheMergePolicy} is used.
 * </p>
 *
 * @see com.hazelcast.cache.CacheMergePolicy
 */
public interface OriginalTypeAwareCacheMergePolicy extends CacheMergePolicy {

}
