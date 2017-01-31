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

package com.hazelcast.map.impl;

import com.hazelcast.map.AbstractEntryProcessor;

import java.util.Map;

public class EntryRemovingProcessor extends AbstractEntryProcessor {

    public static final EntryRemovingProcessor ENTRY_REMOVING_PROCESSOR = new EntryRemovingProcessor();

    public EntryRemovingProcessor() {
    }

    public Object process(Map.Entry entry) {
        ((LazyMapEntry) entry).remove();
        return null;
    }
}
