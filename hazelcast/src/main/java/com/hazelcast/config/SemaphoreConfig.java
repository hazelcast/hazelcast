/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.config;

import com.hazelcast.core.SemaphoreFactory;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SemaphoreConfig implements DataSerializable {

    private String name;
    private int initialPermits;
    private boolean factoryEnabled;
    private String factoryClassName;
    private SemaphoreFactory factoryImplementation;

    public SemaphoreConfig() {
    }

    public SemaphoreConfig(String name) {
        this.name = name;
    }

    public SemaphoreConfig(String name, int initialPermits) {
        this.name = name;
        this.initialPermits = initialPermits;
    }

    public SemaphoreConfig(String name, SemaphoreConfig sc) {
        this.name = name;
        this.factoryClassName = sc.factoryClassName;
        this.initialPermits = sc.initialPermits;
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        initialPermits = in.readInt();
        factoryClassName = in.readUTF();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(initialPermits);
        out.writeUTF(factoryClassName);
    }

    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    SemaphoreConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getFactoryClassName() {
        return factoryClassName;
    }

    public SemaphoreConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
    }

    public boolean isFactoryEnabled() {
        return factoryEnabled;
    }

    public SemaphoreConfig setFactoryEnabled(boolean factoryEnabled) {
        this.factoryEnabled = factoryEnabled;
        return this;
    }

    public SemaphoreFactory getFactoryImplementation() {
        return factoryImplementation;
    }

    public SemaphoreConfig setFactoryImplementation(SemaphoreFactory factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    public int getInitialPermits() {
        return initialPermits;
    }

    public SemaphoreConfig setInitialPermits(int initialPermits) {
        this.initialPermits = initialPermits;
        return this;
    }

    @Override
    public String toString() {
        return "SemaphoreConfig [name=" + this.name
                + ", factoryEnabled="+Boolean.valueOf(factoryEnabled)
                + ", factoryClassName=" + this.factoryClassName
                + ", factoryImplementation=" + this.factoryImplementation + "]";
    }
}