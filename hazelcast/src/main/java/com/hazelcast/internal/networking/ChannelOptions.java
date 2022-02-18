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

package com.hazelcast.internal.networking;

/**
 * Contains the configuration of a {@link Channel}.
 *
 * The ChannelOptions is not tied to a particular type of transport. So it could
 * contain TCP based transport, but it could also contain e.g. UDP transport
 * configuration.
 */
public interface ChannelOptions {

    /**
     * Sets an option value.
     *
     * @param option the option
     * @param value the value
     * @param <T> the type of the value
     * @return this instance for a fluent interface.
     * @throws NullPointerException if option or value is null.
     * @throws com.hazelcast.core.HazelcastException if the value could not be set.
     */
    <T> ChannelOptions setOption(ChannelOption<T> option, T value);

    /**
     * Gets an option value.
     *
     * @param option the option
     * @param <T> the type of the value
     * @return null if value is null.
     * @throws com.hazelcast.core.HazelcastException if the value could not be get.
     */
    <T> T getOption(ChannelOption<T> option);
}
