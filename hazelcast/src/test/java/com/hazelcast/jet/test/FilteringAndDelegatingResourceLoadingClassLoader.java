/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.test;

import com.hazelcast.internal.util.FilteringClassLoader;

import java.io.InputStream;
import java.net.URL;
import java.util.List;

public class FilteringAndDelegatingResourceLoadingClassLoader extends FilteringClassLoader {
    private final ClassLoader delegate;

    public FilteringAndDelegatingResourceLoadingClassLoader(List<String> excludePackages,
                                                            String enforcedSelfLoadingPackage,
                                                            ClassLoader delegate) {
        super(excludePackages, enforcedSelfLoadingPackage);
        this.delegate = delegate;
    }

    public URL getResource(String name) {
        URL resource = this.delegate.getResource(name);
        if (resource == null) {
            return super.getResource(name);
        }
        return resource;
    }

    public InputStream getResourceAsStream(String name) {
        InputStream stream = this.delegate.getResourceAsStream(name);
        if (stream == null) {
            return super.getResourceAsStream(name);
        }
        return stream;
    }

}
