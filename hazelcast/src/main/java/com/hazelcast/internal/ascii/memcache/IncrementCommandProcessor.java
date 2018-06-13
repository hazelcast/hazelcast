/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.ExceptionUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.hazelcast.internal.ascii.TextCommandConstants.NOT_FOUND;
import static com.hazelcast.internal.ascii.TextCommandConstants.RETURN;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.DECREMENT;
import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.INCREMENT;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class IncrementCommandProcessor extends MemcacheCommandProcessor<IncrementCommand> {

    public IncrementCommandProcessor(TextCommandServiceImpl textCommandService) {
        super(textCommandService);
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
        MemcacheEntry entry;

        if (value != null) {
            if (value instanceof MemcacheEntry) {
                entry = (MemcacheEntry) value;
            } else if (value instanceof byte[]) {
                entry = new MemcacheEntry(incrementCommand.getKey(), (byte[]) value, 0);
            } else if (value instanceof String) {
                entry = new MemcacheEntry(incrementCommand.getKey(), stringToBytes((String) value), 0);
            } else {
                try {
                    entry = new MemcacheEntry(incrementCommand.getKey(), textCommandService.toByteArray(value), 0);
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }

            final byte[] value1 = entry.getValue();
            final long current = (value1 == null || value1.length == 0) ? 0 : byteArrayToLong(value1);
            long result = -1;
            result = incrementCommandTypeCheck(incrementCommand, result, current);

            incrementCommand.setResponse(concatenate(stringToBytes(String.valueOf(result)), RETURN));
            MemcacheEntry newValue = new MemcacheEntry(key, longToByteArray(result), entry.getFlag());
            textCommandService.put(mapName, key, newValue);
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

    private long incrementCommandTypeCheck(IncrementCommand incrementCommand, long result, long current) {
        long paramResult = result;
        if (incrementCommand.getType() == INCREMENT) {
            paramResult = current + incrementCommand.getValue();
            paramResult = 0 > paramResult ? Long.MAX_VALUE : paramResult;
            textCommandService.incrementIncHitCount();
        } else if (incrementCommand.getType() == DECREMENT) {
            paramResult = current - incrementCommand.getValue();
            paramResult = 0 > paramResult ? 0 : paramResult;
            textCommandService.incrementDecrHitCount();
        }

        return paramResult;
    }
}
