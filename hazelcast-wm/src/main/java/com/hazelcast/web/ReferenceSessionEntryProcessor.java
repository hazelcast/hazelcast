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
 * Increments the reference count, returning {@code Boolean.FALSE} if the entry does not exist and
 * {@code Boolean.TRUE} otherwise.
 * <p/>
 * This {@code EntryProcessor} is intended to be used to check whether a session is known to the cluster
 * <i>in preparation to create a local "copy"</i>. When {@code Boolean.FALSE} is returned, it means the
 * session is not known and a new session should be created. Otherwise, if the entry already exists, the
 * reference count is incremented and {@code Boolean.TRUE} is returned to indicate a "copy" should be made.
 *
 * @since 3.3
 */
public class ReferenceSessionEntryProcessor extends AbstractWebDataEntryProcessor<Integer> {

    @Override
    public int getId() {
        return WebDataSerializerHook.REFERENCE_SESSION_ID;
    }

    @Override
    public Object process(Map.Entry<String, Integer> entry) {
        Integer value = entry.getValue();
        if (value == null) {
            // This session is not active on any node in the cluster
            return Boolean.FALSE;
        }

        // Otherwise, at least one other node in the cluster has seen this session, so it should be "copied"
        // to the requesting node. Because it's being copied, increment the reference count to ensure the
        // session does not idle out globally until all referencing nodes have timed out
        entry.setValue(value + 1);

        return Boolean.TRUE;
    }
}
