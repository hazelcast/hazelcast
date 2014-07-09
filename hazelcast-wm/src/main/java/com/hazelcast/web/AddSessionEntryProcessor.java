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
 * Sets the initial reference count to 1. If the entry already exists with a non-{@code null} value, this
 * processor does nothing.
 * <p/>
 * This {@code EntryProcessor} is only intended to be used when the <i>first node in the cluster</i> creates a
 * given session. After that, when a new node in the cluster creates a local "copy" of the clustered session,
 * {@link ReferenceSessionEntryProcessor} should be used to increment the reference count.
 *
 * @since 3.3
 */
public class AddSessionEntryProcessor extends AbstractWebDataEntryProcessor<Integer> {

    @Override
    public int getId() {
        return WebDataSerializerHook.ADD_SESSION_ID;
    }

    @Override
    public Object process(Map.Entry<String, Integer> entry) {
        Integer startingValue = entry.getValue();
        if (startingValue == null) {
            entry.setValue(1);
        }

        return startingValue;
    }
}
