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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.util.QuickMath;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/**
 * Metrics dictionary storing word -> id and id -> word mappings. Used by
 * {@link MetricsCompressor}'s dictionary based algorithm.
 */
class MetricsDictionary {
    private static final int INITIAL_CAPACITY = 512;

    private String[] dictionary;
    private int size;
    private TreeMap<String, Word> orderedDictionary = new TreeMap<>(Comparator.naturalOrder());

    MetricsDictionary() {
        this.dictionary = new String[INITIAL_CAPACITY];
    }

    /**
     * Returns the dictionary id for the given word. If the word is not yet
     * stored in the dictionary, the word gets stored and a newly assigned
     * id is returned.
     *
     * @param word The word to look for
     * @return the id assigned to the given word
     */
    int getDictionaryId(String word) {
        requireNonNull(word);

        Word wordObj = orderedDictionary.get(word);
        if (wordObj != null) {
            return wordObj.id;
        }

        int nextIdx = size;
        orderedDictionary.put(word, new Word(word, nextIdx));
        ensureCapacity(nextIdx);
        dictionary[nextIdx] = word;
        size++;

        return nextIdx;
    }

    /**
     * Returns the word mapped to the given dictionary id.
     *
     * @param dictionaryId The given dictionary id
     * @return the word or {@code null} if the dictionary id is outside
     * of the mapped range, including negative ids
     */
    @Nullable
    String get(int dictionaryId) {
        if (dictionaryId < 0 || dictionaryId > size) {
            return null;
        }

        return dictionary[dictionaryId];
    }

    int size() {
        return size;
    }

    /**
     * Returns all stored word<->id mappings ordered by word.
     *
     * @return the word<->mappings
     */
    public Collection<Word> words() {
        return orderedDictionary.values();
    }

    private void ensureCapacity(int newIndex) {
        if (newIndex < dictionary.length - 1) {
            return;
        }

        int newCapacity = QuickMath.nextPowerOfTwo(dictionary.length + 1);
        String[] newDictionary = new String[newCapacity];
        System.arraycopy(dictionary, 0, newDictionary, 0, dictionary.length);
        dictionary = newDictionary;
    }

    static final class Word {
        private String word;
        private int id;

        private Word(String word, int id) {
            this.word = word;
            this.id = id;
        }

        String word() {
            return word;
        }

        int dictionaryId() {
            return id;
        }

        @Override
        public String toString() {
            return "Word{"
                    + "word='" + word + '\''
                    + ", id=" + id
                    + '}';
        }
    }
}
