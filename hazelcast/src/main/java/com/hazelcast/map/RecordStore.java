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

package com.hazelcast.map;/*
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

import com.hazelcast.nio.Data;

public interface RecordStore {

    boolean tryRemove(Data dataKey);

    Data remove(Data dataKey);

    boolean remove(Data dataKey, Data testValue);

    Data put(Data dataKey, Data dataValue, long ttl);

    Data replace(Data dataKey, Data dataValue);

    boolean replace(Data dataKey, Data oldValue, Data newValue);

    void set(Data dataKey, Data dataValue, long ttl);

    void putTransient(Data dataKey, Data dataValue, long ttl);

    boolean tryPut(Data dataKey, Data dataValue, long ttl);

    Data putIfAbsent(Data dataKey, Data dataValue, long ttl);

}
