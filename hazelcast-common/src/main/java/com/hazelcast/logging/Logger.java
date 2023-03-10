/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.logging;

import com.hazelcast.internal.util.Preconditions;

import javax.annotation.Nonnull;

public final class Logger {

    private Logger() {
    }

    /**
     * Obtains a {@link ILogger logger} for the given {@code clazz}.
     *
     * @param clazz the class to obtain the logger for.
     * @return the obtained logger.
     */
    public static ILogger getLogger(@Nonnull Class clazz) {
        Preconditions.checkNotNull(clazz, "class must not be null");
        return null;
        //return getLoggerInternal(clazz.getName());
    }

    /**
     * Obtains a {@link ILogger logger} of the given {@code name}.
     *
     * @param name the name of the logger to obtain.
     * @return the obtained logger.
     */
    public static ILogger getLogger(@Nonnull String name) {
        Preconditions.checkNotNull(name, "name must not be null");
        return null;
        //return getLoggerInternal(name);
    }
}
