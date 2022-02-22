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

package com.hazelcast.osgi;

import org.osgi.framework.Bundle;
import org.osgi.framework.ServiceReference;

class TestServiceReference implements ServiceReference {

    private final TestBundle testBundle;
    private final Object service;
    private final Integer id;

    TestServiceReference(TestBundle testBundle, Object service, int id) {
        this.testBundle = testBundle;
        this.service = service;
        this.id = id;
    }

    @Override
    public TestBundle getBundle() {
        return testBundle;
    }

    Object getService() {
        return service;
    }

    @Override
    public Object getProperty(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getPropertyKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bundle[] getUsingBundles() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAssignableTo(Bundle bundle, String className) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Object reference) {
        if (reference instanceof TestServiceReference) {
            return id.compareTo(((TestServiceReference) reference).id);
        }
        return -1;
    }
}
