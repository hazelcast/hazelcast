/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.EventFilter;

public class EntryEventFilter implements EventFilter {

    boolean includeValue = false;
    Object key = null;

    public EntryEventFilter(boolean includeValue, Object key) {
        this.includeValue = includeValue;
        this.key = key;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public boolean eval(Object arg) {
        return key == null || key.equals(arg);
    }
}
