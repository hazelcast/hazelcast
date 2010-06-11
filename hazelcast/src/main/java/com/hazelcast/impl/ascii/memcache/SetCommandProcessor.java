/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.ascii.memcache;

import com.hazelcast.impl.ascii.AbstractTextCommandProcessor;
import com.hazelcast.impl.ascii.TextCommandService;

import static com.hazelcast.impl.ascii.TextCommandConstants.TextCommandType.*;

public class SetCommandProcessor extends AbstractTextCommandProcessor<SetCommand> {

    public SetCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
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
    public void handle(SetCommand request) {
//            System.out.println("Processing " + request);
        String key = request.getKey();
        String mapName = "default";
        int index = key.indexOf(':');
        if (index != -1) {
            mapName = key.substring(0, index);
            key = key.substring(index + 1);
        }
        byte[] value = request.getValue();
        int ttl = textCommandService.getAdjustedTTLSeconds(request.getExpiration());
        textCommandService.incrementSetCount();
        if (SET == request.getType()) {
            request.setResponse(STORED);
            if (request.shouldReply()) {
                textCommandService.sendResponse(request);
            }
            textCommandService.put(mapName, key, value, ttl);
        } else if (ADD == request.getType()) {
            boolean added = (textCommandService.putIfAbsent(mapName, key, value, ttl) == null);
            if (added) {
                request.setResponse(STORED);
            } else {
                request.setResponse(NOT_STORED);
            }
            if (request.shouldReply()) {
                textCommandService.sendResponse(request);
            }
        } else if (REPLACE == request.getType()) {
            boolean replaced = (textCommandService.replace(mapName, key, value) != null);
            if (replaced) {
                request.setResponse(STORED);
            } else {
                request.setResponse(NOT_STORED);
            }
            if (request.shouldReply()) {
                textCommandService.sendResponse(request);
            }
        }
    }

    public void handleRejection(SetCommand request) {
        request.setResponse(NOT_STORED);
        if (request.shouldReply()) {
            textCommandService.sendResponse(request);
        }
    }
}
