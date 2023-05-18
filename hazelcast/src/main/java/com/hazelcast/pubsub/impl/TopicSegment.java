/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.OS;

@SuppressWarnings({"checkstyle:VisibilityModifier"})
public class TopicSegment {

    public static final int SIZEOF_SEGMENT_HEADER = OS.pageSize();

    public long id;
    public IOBuffer header;
    public AsyncFile file;
    public long offset = -1;
    public long fileOffset = -1;
    public IOBuffer buffer;
}
