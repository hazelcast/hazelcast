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

import java.io.File;

/**
 * A the sequence of segments for a single partition/topic.
 */
// https://strimzi.io/blog/2021/12/17/kafka-segment-retention/
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public class TopicPartition {

    public int activeBufferLength;
    public int partitionId;
    public long offset;
    public TopicSegment activeSegment;
    public long activeSegmentId = -1;
    public int maxRetainedSegments;
    public int syncInterval;
    public File dir;
    public int segmentSize;
}
