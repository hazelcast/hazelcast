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

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;

public class PersonCacheEntryListenerConfiguration implements CacheEntryListenerConfiguration<String, Person> {

    private static final long serialVersionUID = 1L;

    @Override
    public Factory<CacheEntryListener<? super String, ? super Person>> getCacheEntryListenerFactory() {
        return new Factory<CacheEntryListener<? super String, ? super Person>>() {
            @Override
            public CacheEntryListener<? super String, ? super Person> create() {
                return new CacheEntryListener<String, Person>() {
                };
            }

            private static final long serialVersionUID = 1L;
        };
    }

    @Override
    public boolean isOldValueRequired() {
        return false;
    }

    @Override
    public Factory<CacheEntryEventFilter<? super String, ? super Person>> getCacheEntryEventFilterFactory() {
        return new Factory<CacheEntryEventFilter<? super String, ? super Person>>() {
            @Override
            public CacheEntryEventFilter<? super String, ? super Person> create() {
                return new CacheEntryEventFilter<String, Person>() {
                    @Override
                    public boolean evaluate(CacheEntryEvent<? extends String, ? extends Person> cee)
                            throws CacheEntryListenerException {
                        return true;
                    }
                };
            }

            private static final long serialVersionUID = 1L;
        };
    }

    @Override
    public boolean isSynchronous() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PersonCacheEntryListenerConfiguration;
    }
}
