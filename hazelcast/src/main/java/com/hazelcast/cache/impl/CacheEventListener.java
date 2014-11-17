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

/**
 * Cache Event Listener interface is designated for handling cache events internally such as delegating events
 * to cache event listener.
 *
 * <p>Note: This interface is for internal usage and shouldn't be used in user code.</p>
 *
 * @see com.hazelcast.cache.impl.CacheEventListenerAdaptor
 * @see AbstractInternalCacheProxy.CacheCompletionEventListener
 *
 */
public interface CacheEventListener {

    /**
     * Cache Event handling function.
     *
     * @param eventObject Event data object, can be one of {@link CacheEventSet} or {@link CacheEventData} classes.
     * @see com.hazelcast.cache.impl.CacheEventType Event Types that can be handled.
     */
    void handleEvent(Object eventObject);

}
