/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Contains code that processes an infinite stream by grouping items into
 * event time-based windows. Jet uses the term <strong>{@code eventSeq}
 * </strong> for a {@code long} value that determines the position of a
 * stream item on the event time axis. This is commonly referred to as the
 * <em>timestamp</em> of the event represented by the item; Jet uses this
 * term to avoid the confusion between the notions of wall-clock time (aka.
 * system time, processing time) and event time. The {@code eventSeq} value
 * is dimensionless and isn't required to have any correlation with the
 * actual notion of time except for a loose tendency to increase as the
 * position of an item in the stream increases. This way Jet can process
 * events that arrive out of order with respect to the time of their
 * occurrence, and can support timestamps in any time unit.
 */
package com.hazelcast.jet.windowing;
