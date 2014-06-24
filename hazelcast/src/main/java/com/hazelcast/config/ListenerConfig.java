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

package com.hazelcast.config;

import java.util.EventListener;

import static com.hazelcast.util.ValidationUtil.hasText;
import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains the configuration for an {@link EventListener}. The configuration contains either the classname
 * of the EventListener implementation, or the actual EventListener instance.
 */
public class ListenerConfig {

    protected String className;

    protected EventListener implementation;

    private ListenerConfigReadOnly readOnly;

    /**
     * Creates a ListenerConfig without className/implementation.
     */
    public ListenerConfig() {
    }

    /**
     * Creates a ListenerConfig with the given className.
     *
     * @param className the name of the EventListener class.
     * @throws IllegalArgumentException if className is null or an empty String.
     */
    public ListenerConfig(String className) {
        setClassName(className);
    }

    public ListenerConfig(ListenerConfig config) {
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    /**
     * Creates a ListenerConfig with the given implementation.
     *
     * @param implementation the implementation to use as EventListener.
     * @throws IllegalArgumentException if the implementation is null.
     */
    public ListenerConfig(EventListener implementation) {
        this.implementation = isNotNull(implementation, "implementation");
    }

    public ListenerConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the name of the class of the EventListener. If no class is specified, null is returned.
     *
     * @return the class name of the EventListener.
     * @see #setClassName(String)
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the class name of the EventListener.
     * <p/>
     * If a implementation was set, it will be removed.
     *
     * @param className the name of the class of the EventListener.
     * @return the updated ListenerConfig.
     * @throws IllegalArgumentException if className is null or an empty String.
     * @see #setImplementation(java.util.EventListener)
     * @see #getClassName()
     */
    public ListenerConfig setClassName(String className) {
        this.className = hasText(className, "className");
        this.implementation = null;
        return this;
    }

    /**
     * Returns the EventListener implementation. If none has been specified, null is returned.
     *
     * @return the EventListener implementation.
     * @see #setImplementation(java.util.EventListener)
     */
    public EventListener getImplementation() {
        return implementation;
    }

    /**
     * Sets the EventListener implementation.
     * <p/>
     * If a className was set, it will be removed.
     *
     * @param implementation the EventListener implementation.
     * @return the updated ListenerConfig.
     * @throws IllegalArgumentException the implementation is null.
     * @see #setClassName(String)
     * @see #getImplementation()
     */
    public ListenerConfig setImplementation(EventListener implementation) {
        this.implementation = isNotNull(implementation, "implementation");
        this.className = null;
        return this;
    }

    public boolean isIncludeValue() {
        return true;
    }

    public boolean isLocal() {
        return false;
    }

    @Override
    public String toString() {
        return "ListenerConfig [className=" + className + ", implementation=" + implementation + ", includeValue="
                + isIncludeValue() + ", local=" + isLocal() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ListenerConfig that = (ListenerConfig) o;

        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return className != null ? className.hashCode() : 0;
    }
}
