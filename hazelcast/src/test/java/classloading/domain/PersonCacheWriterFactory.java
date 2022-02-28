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

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.Collection;

public class PersonCacheWriterFactory implements Factory<CacheWriter<String, Person>> {
    private static final long serialVersionUID = 1L;

    @Override
    public CacheWriter<String, Person> create() {
        return new CacheWriter<String, Person>() {
            @Override
            public void write(Cache.Entry<? extends String, ? extends Person> entry) throws CacheWriterException {
                throw new UnsupportedOperationException("Not supported");
            }

            @Override
            public void writeAll(Collection<Cache.Entry<? extends String, ? extends Person>> clctn) throws CacheWriterException {
                throw new UnsupportedOperationException("Not supported");
            }

            @Override
            public void delete(Object o) throws CacheWriterException {
                throw new UnsupportedOperationException("Not supported");
            }

            @Override
            public void deleteAll(Collection<?> clctn) throws CacheWriterException {
                throw new UnsupportedOperationException("Not supported");
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PersonCacheWriterFactory;
    }
}
