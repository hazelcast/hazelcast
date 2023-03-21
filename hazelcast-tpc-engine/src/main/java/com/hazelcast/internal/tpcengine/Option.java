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

package com.hazelcast.internal.tpcengine;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * Can be used to pass options to a socket, but in the future also files.
 * <p>
 * An option is uniquely determined based on its name.
 *
 * @param <T> the type of the option.
 */
public final class Option<T> {
    private final String name;
    private final Class<T> type;

    /**
     * Creates a Option with the provided name and type.
     *
     * @param name the name of the Option
     * @param type the type of the Option
     * @throws NullPointerException if name of type is <code>null</code>.
     */
    public Option(String name, Class<T> type) {
        this.name = checkNotNull(name, "name");
        this.type = checkNotNull(type, "type");
    }

    /**
     * Returns the name
     *
     * @return the name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the type for the value of the option.
     *
     * @return the type.
     */
    public Class<T> type() {
        return type;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Option<?> that = (Option<?>) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
