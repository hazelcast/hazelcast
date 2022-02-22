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

package com.hazelcast.collection.impl.queue.model;

import java.io.Serializable;
import java.util.Objects;

public class PriorityElement implements Serializable {
    private static final long serialVersionUID = 1L;
    private boolean highPriority;
    private int version;

    public PriorityElement(boolean highPriority, int version) {
        this.highPriority = highPriority;
        this.version = version;
    }

    public boolean isHighPriority() {
        return this.highPriority;
    }

    public void setHighPriority(boolean highPriority) {
        this.highPriority = highPriority;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PriorityElement that = (PriorityElement) o;
        return highPriority == that.highPriority
                && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(highPriority, version);
    }

    @Override
    public String toString() {
        return "PriorityElement{"
                + "highPriority=" + highPriority
                + ", version=" + version
                + '}';
    }
}
