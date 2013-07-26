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

package com.hazelcast.partition;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;

import java.util.logging.Level;

/**
 * @author mdogan 6/6/13
 */
public final class ReplicaErrorLogger {

    public static void log(Throwable e, final ILogger logger) {
        if (e instanceof RetryableException) {
            Level level = Level.INFO;
            if (e instanceof CallerNotMemberException
                    || e instanceof WrongTargetException
                    || e instanceof TargetNotMemberException) {
                level = Level.FINEST;
            }
            logger.log(level, e.toString());
        } else {
            logger.warning(e);
        }
    }

    private ReplicaErrorLogger() {}
}
