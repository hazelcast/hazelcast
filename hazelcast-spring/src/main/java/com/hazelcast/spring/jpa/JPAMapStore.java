/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.jpa;

import com.hazelcast.core.MapStore;
import org.springframework.data.repository.CrudRepository;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * JPA MapStore implementation.
 */
public class JPAMapStore implements MapStore<Serializable, Object> {
    private CrudRepository crudRepository;

    public CrudRepository getCrudRepository() {
        return crudRepository;
    }

    public void setCrudRepository(CrudRepository crudRepository) {
        this.crudRepository = crudRepository;
    }

    public void store(Serializable key, Object value) {
        crudRepository.save(value);
    }

    public void storeAll(Map map) {
        crudRepository.save(map.values());
    }

    public void delete(Serializable key) {
        crudRepository.delete(key);
    }

    public Object load(Serializable key) {
        return crudRepository.findOne(key);
    }

    // override this method after implementing deleteAll in your custom repository implementation
    public void deleteAll(Collection keys) {
        for (Object key : keys) {
            crudRepository.delete(key);
        }
    }

    // override this method after implementing findAllByIds in your custom repository implementation
    public Map loadAll(Collection collection) {
        return null;
    }

    // override this method after implementing findAllIds in your custom repository implementation
    public Set loadAllKeys() {
        return null;
    }
}
