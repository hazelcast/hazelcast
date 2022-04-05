/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.Comparator;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

/**
 * Metrics dictionary storing word -> id mapping. Used by {@link
 * MetricsCompressor}'s dictionary-based algorithm.
 */
class MetricsDictionary {
    /**
     * Word length is limited by {@link MetricsCompressor#writeDictionary()},
     * if there's no shared prefix, the remainder length is stored as an
     * unsigned byte.
     */
    static final int MAX_WORD_LENGTH = MetricsCompressor.UNSIGNED_BYTE_MAX_VALUE;

    private TreeMap<String, Word> orderedDictionary = new TreeMap<>(Comparator.naturalOrder());

    /**
     * Returns the dictionary id for the given word. If the word is not yet
     * stored in the dictionary, the word gets stored and a newly assigned
     * id is returned.
     *
     * @param word The word to look for
     * @return the id assigned to the given word
     */
    int getDictionaryId(String word) throws LongWordException {
        requireNonNull(word);
        if (word.length() > MAX_WORD_LENGTH) {
            throw new LongWordException("Too long value in the metric descriptor found, maximum is "
                    + MAX_WORD_LENGTH + ": " + word);
        }

        int nextWordId = orderedDictionary.size();
        return orderedDictionary
                .computeIfAbsent(word, key -> new Word(word, nextWordId))
                .id;
    }

    /**
     * Returns all stored word<->id mappings ordered by word.
     *
     * @return the word<->mappings
     */
    public Collection<Word> words() {
        return orderedDictionary.values();
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
            return "Word{word='" + word + '\'' + ", id=" + id + '}';
        }
    }
}
