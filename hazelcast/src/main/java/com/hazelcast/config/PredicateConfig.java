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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains the configuration for an {@link Predicate}. The configuration contains either the class name
 * of the Predicate implementation, or the actual Predicate instance.
 *
 * @since 3.5
 */
public class PredicateConfig implements IdentifiedDataSerializable {

    protected String className;

    protected String sql;

    protected Predicate implementation;

    /**
     * Creates a PredicateConfig without className/implementation.
     */
    public PredicateConfig() {
    }

    /**
     * Creates a PredicateConfig with the given className.
     *
     * @param className the name of the Predicate class
     * @throws IllegalArgumentException if className is {@code null} or an empty String
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
     * @param implementation the implementation to use as Predicate
     * @throws IllegalArgumentException if the implementation is {@code null}
     */
    public PredicateConfig(Predicate implementation) {
        this.implementation = isNotNull(implementation, "implementation");
    }

    /**
     * Returns the name of the class of the Predicate. If no class is specified, {@code null} is returned.
     *
     * @return the class name of the Predicate
     * @see #setClassName(String)
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the class name of the Predicate.
     * <p>
     * If a implementation or sql was set, it will be removed.
     *
     * @param className the name of the class of the Predicate
     * @return the updated PredicateConfig
     * @throws IllegalArgumentException if className is {@code null} or an empty String
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
     * Returns the Predicate implementation. If none has been specified, {@code null} is returned.
     *
     * @return the Predicate implementation
     * @see #setImplementation(Predicate)
     */
    public Predicate getImplementation() {
        return implementation;
    }

    /**
     * Sets the Predicate implementation.
     * <p>
     * If a className or sql was set, it will be removed.
     *
     * @param implementation the Predicate implementation
     * @return the updated PredicateConfig
     * @throws IllegalArgumentException the implementation is {@code null}
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
     * @return sql string for this config
     */
    public String getSql() {
        return sql;
    }

    /**
     * Sets sql string for this config.
     * <p>
     * If a className or implementation was set, it will be removed.
     *
     * @param sql sql string for this config
     * @return this configuration
     */
    public PredicateConfig setSql(String sql) {
        this.sql = sql;
        this.className = null;
        this.implementation = null;
        return this;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PredicateConfig)) {
            return false;
        }

        PredicateConfig that = (PredicateConfig) o;
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (sql != null ? !sql.equals(that.sql) : that.sql != null) {
            return false;
        }
        return !(implementation != null ? !implementation.equals(that.implementation) : that.implementation != null);
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

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.PREDICATE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(className);
        out.writeString(sql);
        out.writeObject(implementation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readString();
        sql = in.readString();
        implementation = in.readObject();
    }
}
