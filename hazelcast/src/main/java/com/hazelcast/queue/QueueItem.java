/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.nio.Data;

/**
 * @ali 12/12/12
 */
public class QueueItem {

    final Data data;

    QueueItem(Data data){
        this.data = data;
    }

    public boolean equals(Object obj) {
        if (obj instanceof QueueItem){
            QueueItem other = (QueueItem)obj;
            data.equals(other.data);
        }
        else if (obj instanceof Data){
            data.equals(obj);
        }
        return false;
    }
}
