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

package com.hazelcast.internal.ascii.memcache;

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * Produced {@link MemcacheEntry} from key and values
 *
 */
public final class EntryConverter {
    private final TextCommandService textCommandService;
    private final ILogger logger;

    public EntryConverter(TextCommandService textCommandService, ILogger logger) {
        this.textCommandService = textCommandService;
        this.logger = logger;
    }

    public MemcacheEntry toEntry(String key, Object value) {
        if (value == null) {
            return null;
        }

        MemcacheEntry entry = null;
        if (value instanceof MemcacheEntry) {
            entry = (MemcacheEntry) value;
        } else if (value instanceof byte[]) {
            entry = new MemcacheEntry(key, ((byte[]) value), 0);
        } else if (value instanceof String) {
            entry = new MemcacheEntry(key, stringToBytes((String) value), 0);
        } else {
            try {
                entry = new MemcacheEntry(key, textCommandService.toByteArray(value), 0);
            } catch (Exception e) {
                logger.warning(e);
            }
        }
        return entry;
    }
}
