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

package com.hazelcast.map.eviction;

import com.hazelcast.map.record.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * EvictionHandlerChain.
 */
public class ReachabilityHandlerChain {

    private static final Comparator<ReachabilityHandler> NICE_VALUE_COMPARATOR = new Comparator<ReachabilityHandler>() {
        @Override
        public int compare(ReachabilityHandler o1, ReachabilityHandler o2) {
            final short i1 = o1.niceNumber();
            final short i2 = o2.niceNumber();
            // we want small nice numbers first.
            return (i1 < i2) ? -1 : ((i1 == i2) ? 0 : 1);
        }
    };

    private ReachabilityHandler<Record> firstHandler;

    private ReachabilityHandler<Record> successorHandler;

    public ReachabilityHandlerChain(ReachabilityHandler<Record>... handlers) {
        // TODO eviction handler.
        for (ReachabilityHandler<Record> evictionHandler : handlers) {
            addHandler(evictionHandler);
        }
    }

    public Record isReachable(Record record, long criteria, long nowInNanos) {
        return firstHandler.process(record, criteria, nowInNanos);
    }

    public void addHandler(ReachabilityHandler<Record> handler) {
        addHandlerInternal(handler);
        final List<ReachabilityHandler> sortedHandlers = sortHandlers();
        resetHandlerChain(sortedHandlers);
        for (ReachabilityHandler sortedHandler : sortedHandlers) {
            addHandlerInternal(sortedHandler);
        }
    }

    private void addHandlerInternal(ReachabilityHandler<Record> handler) {
        if (firstHandler == null) {
            firstHandler = handler;
        } else {
            successorHandler.setSuccessorHandler(handler);
        }
        successorHandler = handler;
    }

    private List<ReachabilityHandler> sortHandlers() {
        final List<ReachabilityHandler> sortedList = new ArrayList<ReachabilityHandler>();
        ReachabilityHandler tempHandler = firstHandler;
        sortedList.add(tempHandler);
        while (tempHandler.getSuccessorHandler() != null) {
            tempHandler = tempHandler.getSuccessorHandler();
            sortedList.add(tempHandler);
        }
        Collections.sort(sortedList, NICE_VALUE_COMPARATOR);
        return sortedList;
    }


    private void resetHandlerChain(List<ReachabilityHandler> sortedHandlers) {
        firstHandler = null;
        successorHandler = null;
        for (ReachabilityHandler sortedHandler : sortedHandlers) {
            sortedHandler.resetHandler();
        }
    }

}
