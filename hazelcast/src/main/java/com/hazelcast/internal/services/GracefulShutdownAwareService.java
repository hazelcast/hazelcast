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

package com.hazelcast.internal.services;

import java.util.concurrent.TimeUnit;

/**
 * An interface that can be implemented by SPI services to participate in graceful shutdown process, such as moving
 * their internal data to another node or releasing their allocated resources gracefully.
 */
public interface GracefulShutdownAwareService {

    /**
     * A hook method that's called during graceful shutdown to provide safety for data managed by this service.
     * Shutdown process is blocked until this method returns or shutdown timeouts. If this method does not
     * return in time, then it's considered as failed to gracefully shutdown.
     *
     * @param timeout timeout for graceful shutdown
     * @param unit time unit
     * @return true if graceful shutdown is successful, false otherwise
     */
    boolean onShutdown(long timeout, TimeUnit unit);

}
