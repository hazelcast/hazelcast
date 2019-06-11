/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package org.roaringbitmap.longlong;

import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.PeekableIntIterator;

import java.util.Iterator;
import java.util.Map;

public class HackedRoaringBitmap extends Roaring64NavigableMap {

    public HackedRoaringBitmap() {
        super(false, false);
    }

    public PeekableLongIterator peekableLongIterator() {
        return new PeekableLongIterator() {

            private Iterator<Map.Entry<Integer, BitmapDataProvider>> bitmaps = getHighToBitmap().entrySet().iterator();

            private int iteratorHigh;
            private PeekableIntIterator iterator;
            private long next;

            {
                advance();
            }

            @Override
            public void advanceIfNeeded(long minimum) {
                if (iterator == null || next >= minimum) {
                    return;
                }

                int high = RoaringIntPacking.high(minimum);
                int low = RoaringIntPacking.low(minimum);

                if (iteratorHigh == high) {
                    iterator.advanceIfNeeded(low);

                    if (iterator.hasNext()) {
                        next = RoaringIntPacking.pack(iteratorHigh, iterator.next());
                    } else {
                        advance();
                    }
                } else {
                    bitmaps = getHighToBitmap().tailMap(high, true).entrySet().iterator();
                    iterator = null;
                    advance();
                }
            }

            @Override
            public long peekNext() {
                return next;
            }

            @Override
            public PeekableLongIterator clone() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return iterator != null;
            }

            @Override
            public long next() {
                long next = this.next;
                advance();
                return next;
            }

            private void advance() {
                if (iterator != null && iterator.hasNext()) {
                    next = RoaringIntPacking.pack(iteratorHigh, iterator.next());
                    return;
                }

                while (bitmaps.hasNext()) {
                    Map.Entry<Integer, BitmapDataProvider> bitmap = bitmaps.next();
                    PeekableIntIterator candidateIterator = bitmap.getValue().getIntIterator();
                    if (candidateIterator.hasNext()) {
                        int candidateHigh = bitmap.getKey();

                        iterator = candidateIterator;
                        iteratorHigh = candidateHigh;
                        next = RoaringIntPacking.pack(candidateHigh, candidateIterator.next());
                        return;
                    }
                }

                iterator = null;
            }

        };
    }

}
