/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.ascii.TextCommandConstants;
import com.hazelcast.internal.ascii.TextCommandServiceImpl;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * User: sancar
 * Date: 3/8/13
 * Time: 2:03 PM
 */
public class TouchCommandProcessor extends MemcacheCommandProcessor<TouchCommand> {

    private final ILogger logger;

    public TouchCommandProcessor(TextCommandServiceImpl textCommandService) {
        super(textCommandService);
        logger = textCommandService.getNode().getLogger(this.getClass().getName());
    }

    public void handle(TouchCommand touchCommand) {
        String key = null;
        try {
            key = URLDecoder.decode(touchCommand.getKey(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HazelcastException(e);
        }
        String mapName = DEFAULT_MAP_NAME;
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = MAP_NAME_PRECEDER + key.substring(0, index);
            key = key.substring(index + 1);
        }
        int ttl = textCommandService.getAdjustedTTLSeconds(touchCommand.getExpiration());
        try {
            textCommandService.lock(mapName, key);
        } catch (Exception e) {
            touchCommand.setResponse(TextCommandConstants.NOT_STORED);
            if (touchCommand.shouldReply()) {
                textCommandService.sendResponse(touchCommand);
            }
            return;
        }
        final Object value = textCommandService.get(mapName, key);
        textCommandService.incrementTouchCount();
        if (value != null) {
            textCommandService.put(mapName, key, value, ttl);
            touchCommand.setResponse(TextCommandConstants.TOUCHED);
        } else {
            touchCommand.setResponse(TextCommandConstants.NOT_STORED);
        }
        textCommandService.unlock(mapName, key);

        if (touchCommand.shouldReply()) {
            textCommandService.sendResponse(touchCommand);
        }
    }

    public void handleRejection(TouchCommand request) {
        request.setResponse(TextCommandConstants.NOT_STORED);
        if (request.shouldReply()) {
            textCommandService.sendResponse(request);
        }
    }
}
