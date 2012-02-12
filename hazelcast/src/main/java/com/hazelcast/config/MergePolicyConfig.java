/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.merge.MergePolicy;

public class MergePolicyConfig {
    String name;
    String className;
    MergePolicy implementation;

    public MergePolicyConfig() {
    }

    public MergePolicyConfig(String name, MergePolicy implementation) {
        this.name = name;
        this.implementation = implementation;
    }

    public MergePolicyConfig(String name, String className) {
        this.name = name;
        this.className = className;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public MergePolicy getImplementation() {
        return implementation;
    }

    public void setImplementation(MergePolicy implementation) {
        this.implementation = implementation;
    }

    @Override
    public String toString() {
        return "MergePolicyConfig{" +
                "name='" + name + '\'' +
                ", className='" + className + '\'' +
                '}';
    }
}
