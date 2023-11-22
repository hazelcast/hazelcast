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

package com.hazelcast.dataconnection;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.impl.ReferenceCounter;

import javax.annotation.Nonnull;

/**
 * Base class for {@link DataConnection} implementations. Provides a {@link
 * ReferenceCounter}. When the ref count gets to 0, calls the {@link #destroy()}
 * method.
 *
 * @since 5.3
 */
public abstract class DataConnectionBase implements DataConnection {

    private final ReferenceCounter refCounter = new ReferenceCounter(this::destroy);
    private final DataConnectionConfig config;

    protected DataConnectionBase(@Nonnull DataConnectionConfig config) {
        this.config = config;
    }

    @Override
    @Nonnull
    public final String getName() {
        return config.getName();
    }

    @Override
    public final void retain() {
        refCounter.retain();
    }

    @Override
    public final void release() {
        try {
            refCounter.release();
        } catch (Exception e) {
            // Any DataConnection user might be calling the release method. There can be 3rd party
            // errors here from closing the resources, we want to shield the user from those.
            // We don't have a logger here, for that we'd need to have NodeEngine reference, but
            // the constructor takes only the config, so we just print to the console.
            e.printStackTrace();
        }
    }

    @Nonnull
    @Override
    public final DataConnectionConfig getConfig() {
        return config;
    }
}
