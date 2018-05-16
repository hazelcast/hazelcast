/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Configuration for Serialization Filter.
 */
public class JavaSerializationFilterConfig {

    private volatile ClassFilter blacklist;
    private volatile ClassFilter whitelist;

    public ClassFilter getBlacklist() {
        if (blacklist == null) {
            blacklist = new ClassFilter();
            // default blacklist - some well-known vulnerable classes/packages
            blacklist.addClasses(
                    "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl",
                    "bsh.XThis",
                    "org.apache.commons.beanutils.BeanComparator",
                    "org.codehaus.groovy.runtime.ConvertedClosure",
                    "org.codehaus.groovy.runtime.MethodClosure",
                    "org.springframework.beans.factory.ObjectFactory",
                    "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl")
            .addPackages(
                    "org.apache.commons.collections.functors",
                    "org.apache.commons.collections4.functors");
        }
        return blacklist;
    }

    public JavaSerializationFilterConfig setBlacklist(ClassFilter blackList) {
        this.blacklist = blackList;
        return this;
    }

    public ClassFilter getWhitelist() {
        if (whitelist == null) {
            whitelist = new ClassFilter();
        }
        return whitelist;
    }

    public JavaSerializationFilterConfig setWhitelist(ClassFilter whiteList) {
        this.whitelist = whiteList;
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((blacklist == null) ? 0 : blacklist.hashCode());
        result = prime * result + ((whitelist == null) ? 0 : whitelist.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JavaSerializationFilterConfig other = (JavaSerializationFilterConfig) obj;
        return ((blacklist == null && other.blacklist == null) || (blacklist != null && blacklist.equals(other.blacklist)))
                && ((whitelist == null && other.whitelist == null) || (whitelist != null && whitelist.equals(other.whitelist)));
    }

    @Override
    public String toString() {
        return "JavaSerializationFilterConfig{ blacklist=" + blacklist + ", whitelist=" + whitelist + "}";
    }

}
