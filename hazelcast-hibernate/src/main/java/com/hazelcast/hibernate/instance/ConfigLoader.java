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

package com.hazelcast.hibernate.instance;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @mdogan 4/27/12
 */
class ConfigLoader {

    public static URL locateConfig(final String path) {
        URL url = asFile(path);
        if (url == null) {
            url = asURL(path);
        }
        if (url == null) {
            url = asResource(path);
        }
        return url;
    }

    private static URL asFile(final String path) {
        File file = new File(path);
        if (file.exists()) {
            try {
                return file.toURI().toURL();
            } catch (MalformedURLException e) {
            }
        }
        return null;
    }

    private static URL asURL(final String path) {
        try {
            return new URL(path);
        } catch (MalformedURLException e) {
        }
        return null;
    }

    private static final URL asResource(final String path) {
        URL url = null;
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            url = contextClassLoader.getResource(path);
        }
        if (url == null) {
            url = ConfigLoader.class.getClassLoader().getResource(path);
        }
        if (url == null) {
            url = ClassLoader.getSystemClassLoader().getResource(path);
        }
        return url;
    }
}
