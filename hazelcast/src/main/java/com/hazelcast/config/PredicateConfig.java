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

package com.hazelcast.config;

import com.hazelcast.query.Predicate;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for an {@link Predicate}. The configuration contains either the class name
 * of the Predicate implementation, or the actual Predicate instance.
 *
 * @since 3.5
 */
public class PredicateConfig {

    protected String className;

    protected String sql;

    protected Predicate implementation;

    private PredicateConfigReadOnly readOnly;

    /**
     * Creates a PredicateConfig without className/implementation.
     */
    public PredicateConfig() {
    }

    /**
     * Creates a PredicateConfig with the given className.
     *
     * @param className the name of the Predicate class.
     * @throws IllegalArgumentException if className is null or an empty String.
     */
    public PredicateConfig(String className) {
        setClassName(className);
    }

    public PredicateConfig(PredicateConfig config) {
        implementation = config.getImplementation();
        className = config.getClassName();
        sql = config.getSql();
    }

    /**
     * Creates a PredicateConfig with the given implementation.
     *
     * @param implementation the implementation to use as Predicate.
     * @throws IllegalArgumentException if the implementation is null.
     */
    public PredicateConfig(Predicate implementation) {
        this.implementation = isNotNull(implementation, "implementation");
    }

    public PredicateConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new PredicateConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the name of the class of the Predicate. If no class is specified, null is returned.
     *
     * @return the class name of the Predicate.
     * @see #setClassName(String)
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the class name of the Predicate.
     * <p/>
     * If a implementation or sql was set, it will be removed.
     *
     * @param className the name of the class of the Predicate.
     * @return the updated PredicateConfig.
     * @throws IllegalArgumentException if className is null or an empty String.
     * @see #setImplementation(Predicate)
     * @see #getClassName()
     */
    public PredicateConfig setClassName(String className) {
        this.className = checkHasText(className, "className must contain text");
        this.implementation = null;
        this.sql = null;
        return this;
    }

    /**
     * Returns the Predicate implementation. If none has been specified, null is returned.
     *
     * @return the Predicate implementation.
     * @see #setImplementation(Predicate)
     */
    public Predicate getImplementation() {
        return implementation;
    }

    /**
     * Sets the Predicate implementation.
     * <p/>
     * If a className or sql was set, it will be removed.
     *
     * @param implementation the Predicate implementation.
     * @return the updated PredicateConfig.
     * @throws IllegalArgumentException the implementation is null.
     * @see #setClassName(String)
     * @see #getImplementation()
     */
    public PredicateConfig setImplementation(Predicate implementation) {
        this.implementation = isNotNull(implementation, "implementation");
        this.className = null;
        this.sql = null;
        return this;
    }

    /**
     * Returns sql string for this config.
     *
     * @return sql string for this config.
     */
    public String getSql() {
        return sql;
    }

    /**
     * Sets sql string for this config.
     * <p/>
     * If a className or implementation was set, it will be removed.
     *
     * @param sql sql string for this config.
     */
    public void setSql(String sql) {
        this.sql = sql;
        this.className = null;
        this.implementation = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PredicateConfig)) {
            return false;
        }

        PredicateConfig that = (PredicateConfig) o;

        return equalsInternal(that);

    }

    private boolean equalsInternal(PredicateConfig that) {
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (sql != null ? !sql.equals(that.sql) : that.sql != null) {
            return false;
        }
        return !(implementation != null
                ? !implementation.equals(that.implementation) : that.implementation != null);
    }

    @Override
    public int hashCode() {
        int result = className != null ? className.hashCode() : 0;
        result = 31 * result + (sql != null ? sql.hashCode() : 0);
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PredicateConfig{"
                + "className='" + className + '\''
                + ", sql='" + sql + '\''
                + ", implementation=" + implementation
                + '}';
    }

    /**
     * Contains the configuration for a Predicate.
     *
     * @since 3.5
     */
    static class PredicateConfigReadOnly extends PredicateConfig {

        public PredicateConfigReadOnly(PredicateConfig config) {
            super(config);
        }

        @Override
        public PredicateConfig setClassName(String className) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public PredicateConfig setImplementation(Predicate implementation) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public String toString() {
            return "PredicateConfigReadOnly{ "
                    + super.toString() + '}';
        }
    }
}
