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


import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.logging.ILogger;


import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.ascii.TextCommandConstants.TextCommandType.UNKNOWN;

public class SimpleCommandProcessor extends MemcacheCommandProcessor<SimpleCommand> {

    private final ILogger logger;

    public SimpleCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
        logger = textCommandService.getNode().getLogger(this.getClass().getName());
    }

    public void handle(SimpleCommand command) {
        if (command.getType() == QUIT) {
            try {
                command.getSocketTextReader().closeConnection();
            } catch (Exception e) {
                logger.warning(e);
            }
        } else if (command.getType() == UNKNOWN) {
            command.setResponse(ERROR);
            textCommandService.sendResponse(command);
        }
    }

    public void handleRejection(SimpleCommand command) {
        handle(command);
    }
}
