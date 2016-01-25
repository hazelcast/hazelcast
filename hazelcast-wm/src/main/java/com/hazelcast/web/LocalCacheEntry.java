/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web;

/**
 * LocalCacheEntry which is used store cache entries inside
 * {@link WebFilter}
 */
public class LocalCacheEntry {

    private volatile boolean reload;
    private boolean removed;
    private Object value;
    private volatile boolean dirty;
    private final boolean transientEntry;

    /**
     * Instantiates a new Local cache entry.
     *
     * @param transientEntry the transient entry
     */
    public LocalCacheEntry(boolean transientEntry) {
        this.transientEntry = transientEntry;
        this.reload = true;
    }

    public LocalCacheEntry(boolean transientEntry, Object value) {
        this(transientEntry);
        this.value = value;
    }

    /**
     * Is transient.
     *
     * @return the boolean
     */
    public boolean isTransient() {
        return transientEntry;
    }

    /**
     * Is dirty.
     *
     * @return the boolean
     */
    public boolean isDirty() {
        return !transientEntry && dirty;
    }

    /**
     * Sets dirty.
     *
     * @param dirty the dirty
     */
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * Is reload.
     *
     * @return the boolean
     */
    public boolean isReload() {
        return reload;
    }

    /**
     * Is removed.
     *
     * @return the boolean
     */
    public boolean isRemoved() {
        return removed;
    }

    /**
     * Sets reload.
     *
     * @param reload the reload
     */
    public void setReload(boolean reload) {
        this.reload = reload;
    }

    /**
     * Sets removed.
     *
     * @param removed the removed
     */
    public void setRemoved(boolean removed) {
        this.removed = removed;
    }

    /**
     * Sets value.
     *
     * @param value the value
     */
    public void setValue(Object value) {
        this.value = value;
    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public Object getValue() {
        return value;
    }
}
