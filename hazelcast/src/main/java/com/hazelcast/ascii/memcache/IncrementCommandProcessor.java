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

package com.hazelcast.ascii.memcache;

import com.hazelcast.ascii.TextCommandServiceImpl;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ByteUtil;
import com.hazelcast.util.ExceptionUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * User: sancar
 * Date: 3/8/13
 * Time: 3:51 PM
 */
public class IncrementCommandProcessor extends MemcacheCommandProcessor<IncrementCommand> {

    private final ILogger logger;

    public IncrementCommandProcessor(TextCommandServiceImpl textCommandService) {
        super(textCommandService);
        logger = textCommandService.getNode().getLogger(this.getClass().getName());
    }

    public void handle(IncrementCommand incrementCommand) {
        String key = null;
        try {
            key = URLDecoder.decode(incrementCommand.getKey(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HazelcastException(e);
        }
        String mapName = DefaultMapName;
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = MapNamePreceder + key.substring(0, index);
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
        Object value = textCommandService.get(mapName, key);
        MemcacheEntry entry = null;
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
            if (incrementCommand.getType() == TextCommandType.INCREMENT) {
                result = current + incrementCommand.getValue();
                result = 0 > result ? Long.MAX_VALUE : result;
                textCommandService.incrementIncHitCount();
            } else if (incrementCommand.getType() == TextCommandType.DECREMENT) {
                result = current - incrementCommand.getValue();
                result = 0 > result ? 0 : result;
                textCommandService.incrementDecrHitCount();
            }
            incrementCommand.setResponse(ByteUtil.concatenate(stringToBytes(String.valueOf(result)), RETURN));
            MemcacheEntry newValue = new MemcacheEntry(key, longToByteArray(result), entry.getFlag());
            textCommandService.put(mapName, key, newValue);
        } else {
            if (incrementCommand.getType() == TextCommandType.INCREMENT)
                textCommandService.incrementIncMissCount();
            else
                textCommandService.incrementDecrMissCount();
            incrementCommand.setResponse(NOT_FOUND);
        }
        textCommandService.unlock(mapName, key);

        if (incrementCommand.shouldReply()) {
            textCommandService.sendResponse(incrementCommand);
        }
    }

    public void handleRejection(IncrementCommand incrementCommand) {
        incrementCommand.setResponse(NOT_FOUND);
        if (incrementCommand.shouldReply()) {
            textCommandService.sendResponse(incrementCommand);
        }
    }
}
