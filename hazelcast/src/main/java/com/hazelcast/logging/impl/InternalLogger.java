/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.logging.impl;

import javax.annotation.Nullable;
import java.util.logging.Level;

/**
 * Defines an internal contract for loggers.
 */
public interface InternalLogger {

    /**
     * Sets the level of this logger to the given level.
     *
     * @param level the level to set, can be {@code null} if the underlying
     *              logging framework gives some special meaning to it (like
     *              inheriting the log level from some parent object).
     */
    void setLevel(@Nullable Level level);

}
