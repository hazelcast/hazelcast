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

package com.hazelcast.topic;

import com.hazelcast.core.MessageListener;
import com.hazelcast.spi.annotation.Beta;

/**
 * A {@link TopicErrorHandler} that terminates on any exception.
 *
 * In a lot of cases this is the most sensible approach for reliable topic. Since you want to process every message and
 * if a message failed to be processed, then something very bad is happening and there is no point in continuing.
 */
@Beta
public class TerminatingTopicErrorHandler implements TopicErrorHandler {

    /**
     * Singleton for the TerminatingTopicErrorHandler.
     */
    public static final TerminatingTopicErrorHandler INSTANCE = new TerminatingTopicErrorHandler();

    // we don't care if instances are created of the TerminatingTopicErrorHandler. So no need to make the constructor private
    // This makes creation of a TopicErrorHandler through XML also a lot easier.

    @Override
    public boolean terminate(Throwable t, MessageListener messageListener) {
        return true;
    }
}
