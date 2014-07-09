/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web;

import java.util.Map;

/**
 * Decrements the reference count for a session, returning {@code Boolean.TRUE} or {@code Boolean.FALSE} to
 * indicate whether the reference count has reached zero. If the reference count reaches zero, the session
 * ID is removed by this processor to reduce network calls.
 *
 * @since 3.3
 */
public class DestroySessionEntryProcessor extends AbstractWebDataEntryProcessor<Integer> {

    @Override
    public int getId() {
        return WebDataSerializerHook.DESTROY_SESSION_ID;
    }

    @Override
    public Object process(Map.Entry<String, Integer> entry) {
        Integer value = entry.getValue();
        if (value == null) {
            // This should never happen, but if the session entry no longer exists treat it as invalidated
            return Boolean.TRUE;
        }

        int remaining = value - 1;
        if (remaining <= 0) {
            // If this was the last reference to the entry, null it out to remove it from the
            // map and return true to trigger attribute invalidation
            entry.setValue(null);

            return Boolean.TRUE;
        }

        // Otherwise, if there are still references to the session elsewhere in the cluster,
        // just persist the decrement and return false to prevent attribute invalidation
        entry.setValue(remaining);

        return Boolean.FALSE;
    }
}
