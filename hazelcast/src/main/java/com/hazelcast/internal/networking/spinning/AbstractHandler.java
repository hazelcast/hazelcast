/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.spinning;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.IOOutOfMemoryHandler;
import com.hazelcast.logging.ILogger;

public abstract class AbstractHandler {

    protected final ILogger logger;
    protected final Channel channel;
    private final IOOutOfMemoryHandler oomeHandler;

    public AbstractHandler(Channel channel, ILogger logger, IOOutOfMemoryHandler oomeHandler) {
        this.oomeHandler = oomeHandler;
        this.logger = logger;
        this.channel = channel;
    }

    public Channel getChannel() {
        return channel;
    }

    public void onFailure(Throwable e) {
        if (e instanceof OutOfMemoryError) {
            oomeHandler.handle((OutOfMemoryError) e);
        }

//        if (e instanceof EOFException) {
//            connection.close("Connection closed by the other side", e);
//        } else {
//            connection.close("Exception in " + getClass().getSimpleName(), e);
//        }
    }
}
