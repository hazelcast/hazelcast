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

package com.hazelcast.spi;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializable;

/**
 * The result of a Event Registration.
 *
 */
public interface EventRegistration extends DataSerializable {

    /**
     * Returns event registration id.
     * @return event registration id
     */
    String getId();

    /**
     * Returns {@link EventFilter} attached to this registration.
     *
     * @return event filter attached to this registration
     */
    EventFilter getFilter();

    /**
     * Returns subscriber of this registration.
     *
     * @return subscriber of this registration
     */
    Address getSubscriber();

    /**
     * Returns true if this registration is for locally fired events only.
     *
     * @return true if this registration is local only, false otherwise.
     */
    boolean isLocalOnly();

}
