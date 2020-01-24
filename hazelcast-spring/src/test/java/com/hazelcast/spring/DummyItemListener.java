/*
* Copyright 2020 Hazelcast Inc.
*
* Licensed under the Hazelcast Community License (the "License"); you may not use
* this file except in compliance with the License. You may obtain a copy of the
* License at
*
* http://hazelcast.com/hazelcast-community-license
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
*/

package com.hazelcast.spring;

import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;

public class DummyItemListener implements ItemListener {

    @Override
    public void itemAdded(ItemEvent item) {
    }

    @Override
    public void itemRemoved(ItemEvent item) {
    }
}
