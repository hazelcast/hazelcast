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

import com.hazelcast.ascii.TextCommandConstants;
import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ByteUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.PREPEND;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.SET;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.ADD;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.APPEND;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.REPLACE;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class SetCommandProcessor extends MemcacheCommandProcessor<SetCommand> {

    private final ILogger logger;

    public SetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
        logger = textCommandService.getNode().getLogger(this.getClass().getName());
    }

    /**
     * "set" means "store this data".
     * <p/>
     * "add" means "store this data, but only if the server *doesn't* already
     * hold data for this key".
     * <p/>
     * "replace" means "store this data, but only if the server *does*
     * already hold data for this key".
     * <p/>
     * <p/>
     * After sending the command line and the data block the client awaits
     * the reply, which may be:
     * <p/>
     * - "STORED\r\n", to indicate success.
     * <p/>
     * - "NOT_STORED\r\n" to indicate the data was not stored, but not
     * because of an error. This normally means that either that the
     * condition for an "add" or a "replace" command wasn't met, or that the
     * item is in a delete queue (see the "delete" command below).
     */
    public void handle(SetCommand setCommand) {
        String key = null;
        try {
            key = URLDecoder.decode(setCommand.getKey(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HazelcastException(e);
        }
        String mapName = DEFAULT_MAP_NAME;
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = MAP_NAME_PRECEDER + key.substring(0, index);
            key = key.substring(index + 1);
        }
        Object value = new MemcacheEntry(setCommand.getKey(), setCommand.getValue(), setCommand.getFlag());
        int ttl = textCommandService.getAdjustedTTLSeconds(setCommand.getExpiration());
        textCommandService.incrementSetCount();
        if (SET == setCommand.getType()) {
            textCommandService.put(mapName, key, value, ttl);
            setCommand.setResponse(TextCommandConstants.STORED);
        } else if (ADD == setCommand.getType()) {

            addCommandType(setCommand, mapName, key, value, ttl);

        } else if (REPLACE == setCommand.getType()) {

            replaceCommandType(setCommand, mapName, key, value, ttl);

        } else if (APPEND == setCommand.getType()) {

            appendCommandType(setCommand, mapName, key, ttl);

        } else if (PREPEND == setCommand.getType()) {

            prependCommandType(setCommand, mapName, key, ttl);

        }
        if (setCommand.shouldReply()) {
            textCommandService.sendResponse(setCommand);
        }
    }

    private void replaceCommandType(SetCommand setCommand, String mapName, String key, Object value, int ttl) {
        boolean replaced = (textCommandService.replace(mapName, key, value) != null);
        if (replaced) {
            setCommand.setResponse(TextCommandConstants.STORED);
        } else {
            setCommand.setResponse(TextCommandConstants.NOT_STORED);
        }
    }

    private void addCommandType(SetCommand setCommand, String mapName, String key, Object value, int ttl) {
        boolean added = (textCommandService.putIfAbsent(mapName, key, value, ttl) == null);
        if (added) {
            setCommand.setResponse(TextCommandConstants.STORED);
        } else {
            setCommand.setResponse(TextCommandConstants.NOT_STORED);
        }
    }

    private void prependCommandType(SetCommand setCommand, String mapName, String key, int ttl) {
        try {
            textCommandService.lock(mapName, key);
        } catch (Exception e) {
            setCommand.setResponse(TextCommandConstants.NOT_STORED);
            if (setCommand.shouldReply()) {
                textCommandService.sendResponse(setCommand);
            }
            return;
        }
        Object oldValue = textCommandService.get(mapName, key);
        MemcacheEntry entry = null;
        if (oldValue != null) {
            if (oldValue instanceof MemcacheEntry) {
                final MemcacheEntry oldEntry = (MemcacheEntry) oldValue;
                entry = new MemcacheEntry(setCommand.getKey(),
                        ByteUtil.concatenate(setCommand.getValue(), oldEntry.getValue()), oldEntry.getFlag());
            } else if (oldValue instanceof byte[]) {
                entry = new MemcacheEntry(setCommand.getKey(),
                        ByteUtil.concatenate(setCommand.getValue(), ((byte[]) oldValue)), 0);
            } else if (oldValue instanceof String) {
                entry = new MemcacheEntry(setCommand.getKey(),
                        ByteUtil.concatenate(setCommand.getValue(), stringToBytes((String) oldValue)), 0);
            } else {
                try {
                    entry = new MemcacheEntry(setCommand.getKey(),
                            ByteUtil.concatenate(setCommand.getValue(), textCommandService.toByteArray(oldValue)), 0);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
            textCommandService.put(mapName, key, entry, ttl);
            setCommand.setResponse(TextCommandConstants.STORED);
        } else {
            setCommand.setResponse(TextCommandConstants.NOT_STORED);
        }
        textCommandService.unlock(mapName, key);
    }

    private void appendCommandType(SetCommand setCommand, String mapName, String key, int ttl) {
        try {
            textCommandService.lock(mapName, key);
        } catch (Exception e) {
            setCommand.setResponse(TextCommandConstants.NOT_STORED);
            if (setCommand.shouldReply()) {
                textCommandService.sendResponse(setCommand);
            }
            return;
        }
        Object oldValue = textCommandService.get(mapName, key);
        MemcacheEntry entry = null;
        if (oldValue != null) {
            if (oldValue instanceof MemcacheEntry) {
                final MemcacheEntry oldEntry = (MemcacheEntry) oldValue;
                entry = new MemcacheEntry(setCommand.getKey(),
                        ByteUtil.concatenate(oldEntry.getValue(), setCommand.getValue()), 0);
            } else if (oldValue instanceof byte[]) {
                entry = new MemcacheEntry(setCommand.getKey(),
                        ByteUtil.concatenate(((byte[]) oldValue), setCommand.getValue()), 0);
            } else if (oldValue instanceof String) {
                entry = new MemcacheEntry(setCommand.getKey(),
                        ByteUtil.concatenate(stringToBytes((String) oldValue), setCommand.getValue()), 0);
            } else {
                try {
                    entry = new MemcacheEntry(setCommand.getKey(),
                            ByteUtil.concatenate(textCommandService.toByteArray(oldValue), setCommand.getValue()), 0);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
            textCommandService.put(mapName, key, entry, ttl);
            setCommand.setResponse(TextCommandConstants.STORED);
        } else {
            setCommand.setResponse(TextCommandConstants.NOT_STORED);
        }
        textCommandService.unlock(mapName, key);

    }

    public void handleRejection(SetCommand request) {
        request.setResponse(TextCommandConstants.NOT_STORED);
        if (request.shouldReply()) {
            textCommandService.sendResponse(request);
        }
    }
}
