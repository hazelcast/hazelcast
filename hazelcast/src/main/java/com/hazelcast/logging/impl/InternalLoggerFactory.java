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

import javax.annotation.Nonnull;
import java.util.logging.Level;

/**
 * Defines an internal contract for logger factories.
 */
public interface InternalLoggerFactory {

    /**
     * Sets the levels of all the loggers known to this logger factory to
     * the given level. If a certain logger was already preconfigured with a more
     * verbose level than the given level, it will be kept at that more verbose
     * level.
     *
     * @param level the level to set.
     */
    void setLevel(@Nonnull Level level);

    /**
     * Resets the levels of all the loggers known to this logger factory back
     * to the default preconfigured values. Basically, undoes all the changes done
     * by the previous calls to {@link #setLevel}, if there were any.
     */
    void resetLevel();

}
