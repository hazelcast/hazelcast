/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IndexIntegrationTest extends HazelcastTestSupport {

    @Test
    public void putRemove_withIndex_whereAttributeIsNull() {
        // GIVEN
        MapIndexConfig mapIndexConfig = new MapIndexConfig();
        mapIndexConfig.setAttribute("amount");
        mapIndexConfig.setOrdered(false);

        MapConfig mapConfig = new MapConfig().setName("map");
        mapConfig.addMapIndexConfig(mapIndexConfig);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        Trade trade = new Trade();
        trade.setCurrency("EUR");
        trade.setAmount(null);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Trade> map = instance.getMap("map");

        // WHEN
        map.put(1, trade);
        map.remove(1);

        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("amount").isNull();
        Collection<Trade> values = map.values(predicate);

        // THEN
        assertEquals(0, values.size());
        assertNull(map.get(1));
    }

    public static class Trade implements Serializable {
        private String currency;
        private Long amount;

        public Trade() {
        }

        public String getCurrency() {
            return currency;
        }

        public void setCurrency(String currency) {
            this.currency = currency;
        }

        public Long getAmount() {
            return amount;
        }

        public void setAmount(Long amount) {
            this.amount = amount;
        }
    }

}
