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

package classloading.domain;

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.HashMap;
import java.util.Map;

public class PersonCacheLoaderFactory implements Factory<CacheLoader<String, Person>> {
    private static final long serialVersionUID = 1L;

    @Override
    public CacheLoader<String, Person> create() {
        return new CacheLoader<String, Person>() {
            @Override
            public Person load(String k) throws CacheLoaderException {
                return new Person();
            }

            @Override
            public Map<String, Person> loadAll(Iterable<? extends String> itrbl) throws CacheLoaderException {
                Map<String, Person> rv = new HashMap<String, Person>();
                for (String it : itrbl) {
                    rv.put(it, load(it));
                }
                return rv;
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PersonCacheLoaderFactory;
    }
}
