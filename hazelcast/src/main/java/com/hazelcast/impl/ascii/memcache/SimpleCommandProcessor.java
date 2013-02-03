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

package com.hazelcast.impl.ascii.memcache;

import com.hazelcast.impl.ascii.AbstractTextCommandProcessor;
import com.hazelcast.impl.ascii.TextCommandService;
import com.hazelcast.logging.ILogger;

import java.util.logging.Level;

import static com.hazelcast.impl.ascii.TextCommandConstants.TextCommandType.QUIT;
import static com.hazelcast.impl.ascii.TextCommandConstants.TextCommandType.UNKNOWN;

public class SimpleCommandProcessor extends AbstractTextCommandProcessor<SimpleCommand> {

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
                logger.log(Level.WARNING, e.getMessage(), e);
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