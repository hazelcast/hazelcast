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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.ascii.TextCommandServiceImpl;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.hazelcast.internal.ascii.TextCommandConstants.CLIENT_ERROR;
import static com.hazelcast.internal.ascii.TextCommandConstants.NOT_FOUND;
import static com.hazelcast.internal.ascii.TextCommandConstants.RETURN;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.DECREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.INCREMENT;
import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

public class IncrementCommandProcessor extends MemcacheCommandProcessor<IncrementCommand> {
    private final EntryConverter entryConverter;

    public IncrementCommandProcessor(TextCommandServiceImpl textCommandService, EntryConverter entryConverter) {
        super(textCommandService);
        this.entryConverter = entryConverter;
    }

    @Override
    public void handle(IncrementCommand incrementCommand) {
        String key;
        try {
            key = URLDecoder.decode(incrementCommand.getKey(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HazelcastException(e);
        }
        String mapName = DEFAULT_MAP_NAME;
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = MAP_NAME_PREFIX + key.substring(0, index);
            key = key.substring(index + 1);
        }
        try {
            textCommandService.lock(mapName, key);
        } catch (Exception e) {
            incrementCommand.setResponse(NOT_FOUND);
            if (incrementCommand.shouldReply()) {
                textCommandService.sendResponse(incrementCommand);
            }
            return;
        }
        incrementUnderLock(incrementCommand, key, mapName);
        textCommandService.unlock(mapName, key);
        if (incrementCommand.shouldReply()) {
            textCommandService.sendResponse(incrementCommand);
        }

    }

    private void incrementUnderLock(IncrementCommand incrementCommand, String key, String mapName) {
        Object value = textCommandService.get(mapName, key);

        if (value != null) {
            MemcacheEntry entry = entryConverter.toEntry(incrementCommand.getKey(), value);

            String currentCachedValue = bytesToString(entry.getValue());

            try {
                String newCachedValue = doIncrement(incrementCommand, currentCachedValue);
                updateHitCount(incrementCommand);

                byte[] newCachedValueBytes = stringToBytes(newCachedValue);
                MemcacheEntry newValue = new MemcacheEntry(key, newCachedValueBytes, entry.getFlag());
                textCommandService.put(mapName, key, newValue);
                incrementCommand.setResponse(concatenate(newCachedValueBytes, RETURN));
            } catch (NumberFormatException e) {
                incrementCommand.setResponse(concatenate(concatenate(CLIENT_ERROR, stringToBytes(e.getMessage())), RETURN));
            }
        } else {
            if (incrementCommand.getType() == INCREMENT) {
                textCommandService.incrementIncMissCount();
            } else {
                textCommandService.incrementDecrMissCount();
            }
            incrementCommand.setResponse(NOT_FOUND);
        }
    }

    @Override
    public void handleRejection(IncrementCommand incrementCommand) {
        incrementCommand.setResponse(NOT_FOUND);
        if (incrementCommand.shouldReply()) {
            textCommandService.sendResponse(incrementCommand);
        }
    }

    private String doIncrement(IncrementCommand incrementCommand, String currentValue) {
        // overflows and underflows are handled differently for `incr` and `decr` commands.
        // see https://github.com/memcached/memcached/blob/4b23988/doc/protocol.txt#L347-L390
        // for details.

        long current = Long.parseUnsignedLong(currentValue);
        long incrementAmount = incrementCommand.getValue();

        if (incrementCommand.getType() == INCREMENT) {
            return Long.toUnsignedString(current + incrementAmount);
        } else if (incrementCommand.getType() == DECREMENT) {
            if (Long.compareUnsigned(current, incrementAmount) < 0) {
                // underflow
                return "0";
            } else {
                return Long.toUnsignedString(current - incrementAmount);
            }
        } else {
            throw new IllegalStateException("Unexpected command type");
        }
    }

    private void updateHitCount(IncrementCommand incrementCommand) {
        if (incrementCommand.getType() == INCREMENT) {
            textCommandService.incrementIncHitCount();
        } else if (incrementCommand.getType() == DECREMENT) {
            textCommandService.incrementDecrHitCount();
        }
    }
}
