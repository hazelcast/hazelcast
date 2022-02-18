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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;

/**
 * Optimizes single-attribute predicates into composite index predicates.
 * <p>
 * Given "a = 0 and b = 1" predicates and "a, b" index, the predicates may be
 * optimized into a single composite index query: "(a, b) = (0, 1)".
 */
public class CompositeIndexVisitor extends AbstractVisitor {

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodlength", "checkstyle:npathcomplexity"})
    @Override
    public Predicate visit(AndPredicate andPredicate, Indexes indexes) {
        int originalSize = andPredicate.predicates.length;
        if (originalSize < 2) {
            // can't optimize further
            return andPredicate;
        }

        InternalIndex[] compositeIndexes = indexes.getCompositeIndexes();
        if (compositeIndexes.length == 0) {
            // no composite indexes to optimize against
            return andPredicate;
        }

        // 1. Split predicates into 3 groups: (a) prefixes, that are equal
        // predicates, like a == 1; (b) comparisons, that are order comparisons
        // predicates, like a > 1; (c) output, this group contains unoptimizable
        // predicates, like a != 1; later, the group also receives optimized
        // predicates ready for output.

        Map<String, EqualPredicate> prefixes = null;
        Map<String, RangePredicate> comparisons = null;
        Output output = null;
        for (Predicate predicate : andPredicate.predicates) {
            if (predicate instanceof EqualPredicate) {
                EqualPredicate equalPredicate = (EqualPredicate) predicate;
                prefixes = obtainHashMap(prefixes, originalSize);

                EqualPredicate replaced = prefixes.put(equalPredicate.attributeName, equalPredicate);
                if (replaced != null) {
                    // If we have multiple predicates for the same attribute,
                    // that means RangeVisitor is failed to optimize them. In
                    // turn, that means the range visitor was unable to find
                    // a TypeConverter for the attribute and that means the map
                    // and/or index were empty at the moment.
                    //
                    // So we record duplicates in the output and produce an
                    // underoptimized result in the end.

                    output = obtainOutput(output, originalSize);
                    output.add(replaced);
                }
                continue;
            }

            if (predicate instanceof RangePredicate) {
                RangePredicate rangePredicate = (RangePredicate) predicate;
                comparisons = obtainHashMap(comparisons, originalSize);

                RangePredicate replaced = comparisons.put(rangePredicate.getAttribute(), rangePredicate);
                if (replaced != null) {
                    output = obtainOutput(output, originalSize);
                    output.add(replaced);
                }
                continue;
            }

            output = obtainOutput(output, originalSize);
            output.add(predicate);
        }

        if (prefixes == null || comparisons == null && prefixes.size() == 1) {
            // We have no prefixes to match; or we have only a single prefix and
            // no comparisons that can be matched to form a prefix of length 2.
            return andPredicate;
        }

        // 2. Match indexes against predicates by finding longest prefixes
        // optionally followed by a single comparison.
        //
        // Basically, we have a classic textbook problem: given a dictionary of
        // words and a set of letters, find all dictionary words that can be
        // built using the given letters. In our case, the words are index
        // components and their prefixes (since we want to match components
        // prefixes as well as the full "words"); the target set of letters is
        // attributes of the equal and comparison predicates.
        //
        // A typical solution is to sort the letters of words by alphabet and
        // insert them into a trie. Then, to find a longest word matching given
        // letters, we traverse the trie in a depth-first order, going into a
        // node if the target letters contain a letter corresponding to the node.
        // Some sources even classify this as an O(1) solution since it depends
        // only on the alphabet size: trie depth is limited by the alphabet size
        // and the maximum size of the target letter set is also limited by the
        // alphabet (attribute set is fixed and there is no attribute repetitions
        // in the index components), so we may traverse the trie in a fixed
        // amount of steps.
        //
        // The described trie solution has advantages for dense dictionaries
        // when the word count is much greater than the alphabet size and a lot
        // of words are built from the same letters. But in our case we have
        // a sparse dictionary: the index count is not that much greater than
        // the attribute count and indexes are rarely built for the same set of
        // attributes more than once. So we use a straightforward brute-force
        // approach here, which is good enough in practice mostly due to a
        // better locality of the data.

        assert !prefixes.isEmpty();
        while (!prefixes.isEmpty()) {
            int bestPrefix = 0;
            InternalIndex bestIndex = null;
            RangePredicate bestComparison = null;

            for (InternalIndex index : compositeIndexes) {
                String[] components = index.getComponents();
                if (components.length < bestPrefix || !index.isOrdered() && prefixes.size() < components.length) {
                    // Skip the index if: (a) it has fewer components than the
                    // best found prefix; (b) if index is unordered and we have
                    // fewer components to match than the index has.
                    continue;
                }

                int prefix = 0;
                while (prefix < components.length && prefixes.containsKey(components[prefix])) {
                    ++prefix;
                }
                if (prefix == 0) {
                    // no prefix found at all
                    continue;
                }

                if (index.isOrdered()) {
                    RangePredicate comparison = prefix < components.length && comparisons != null
                        ? comparisons.get(components[prefix]) : null;

                    if (comparison != null) {
                        ++prefix;
                    }

                    if (prefix > bestPrefix) {
                        bestPrefix = prefix;
                        bestIndex = index;
                        bestComparison = comparison;
                    } else if (prefix == bestPrefix) {
                        // the matched prefix is at least of length 1
                        assert bestIndex != null;
                        if (bestIndex.isOrdered() && bestIndex.getComponents().length > components.length) {
                            // prefer shorter indexes over longer ones
                            bestIndex = index;
                            bestComparison = comparison;
                        }
                    }
                } else if (prefix == components.length && prefix >= bestPrefix) {
                    // The unordered index components are fully matched and the
                    // prefix is longer or equal to the best found prefix. The
                    // later is needed to give a preference for unordered matches
                    // over ordered ones.

                    bestPrefix = prefix;
                    bestIndex = index;
                    bestComparison = null;
                }
            }

            if (bestIndex == null || bestPrefix == 1) {
                // Nothing matched or we have a single-attribute prefix which
                // should be handled by AttributeIndexRegistry.
                break;
            }

            // exclude the comparison from the prefix, just to make the math simpler later
            int equalPrefixLength = bestComparison == null ? bestPrefix : bestPrefix - 1;

            if (output == null) {
                // if we have no output yet, try to perform a cheaper fast exit
                Predicate generated = tryGenerateFast(prefixes, comparisons, equalPrefixLength, bestComparison, bestIndex);
                if (generated != null) {
                    return generated;
                }
            }

            output = obtainOutput(output, originalSize);
            addToOutput(prefixes, comparisons, output, equalPrefixLength, bestComparison, bestIndex);
        }

        // 3. Produce the final result.

        return output == null ? andPredicate : output.generate(prefixes, comparisons, andPredicate);
    }

    private static Predicate tryGenerateFast(Map<String, EqualPredicate> prefixes, Map<String, RangePredicate> comparisons,
                                             int prefixLength, RangePredicate comparison, InternalIndex index) {
        if (index.isOrdered()) {
            assert prefixLength <= index.getComponents().length;
            return tryGenerateFastOrdered(prefixes, comparisons, prefixLength, comparison, index);
        } else {
            assert comparison == null;
            assert prefixLength == index.getComponents().length;
            return tryGenerateFastUnordered(prefixes, comparisons, prefixLength, index);
        }
    }

    private static Predicate tryGenerateFastOrdered(Map<String, EqualPredicate> prefixes, Map<String, RangePredicate> comparisons,
                                                    int prefixLength, RangePredicate comparison, InternalIndex index) {
        assert index.isOrdered();
        String[] components = index.getComponents();

        if (prefixes.size() != prefixLength) {
            // some equal predicate(s) left unmatched
            return null;
        }

        if (comparison == null) {
            if (comparisons != null) {
                // some comparison(s) left unmatched
                assert !comparisons.isEmpty();
                return null;
            }

            if (prefixLength == components.length) {
                // full match
                return generateEqualPredicate(index, prefixes, true);
            } else {
                // partial match
                return generateRangePredicate(index, prefixes, prefixLength, true);
            }
        } else {
            if (comparisons.size() != 1) {
                // some comparison(s) left unmatched
                return null;
            }

            return generateRangePredicate(index, prefixes, prefixLength, comparison, true);
        }
    }

    private static Predicate tryGenerateFastUnordered(Map<String, EqualPredicate> prefixes,
                                                      Map<String, RangePredicate> comparisons, int prefixLength,
                                                      InternalIndex index) {
        assert !index.isOrdered();

        if (comparisons != null) {
            // some comparison(s) left unmatched
            assert !comparisons.isEmpty();
            return null;
        }

        if (prefixLength != prefixes.size()) {
            // some equal predicate(s) left unmatched
            return null;
        }

        return generateEqualPredicate(index, prefixes, true);
    }

    private static void addToOutput(Map<String, EqualPredicate> prefixes, Map<String, RangePredicate> comparisons, Output output,
                                    int prefixLength, RangePredicate comparison, InternalIndex index) {
        if (index.isOrdered()) {
            assert prefixLength <= index.getComponents().length;
            addToOutputOrdered(prefixes, comparisons, output, prefixLength, comparison, index);
        } else {
            assert comparison == null;
            assert prefixLength == index.getComponents().length;
            addToOutputUnordered(prefixes, output, index);
        }
    }

    private static void addToOutputOrdered(Map<String, EqualPredicate> prefixes, Map<String, RangePredicate> comparisons,
                                           Output output, int prefixLength, RangePredicate comparison, InternalIndex index) {
        assert index.isOrdered();
        String[] components = index.getComponents();

        if (prefixLength == components.length) {
            // we got a full match
            output.addGenerated(generateEqualPredicate(index, prefixes, false));
            return;
        }

        if (comparison == null) {
            output.addGenerated(generateRangePredicate(index, prefixes, prefixLength, false));
        } else {
            comparisons.remove(comparison.getAttribute());
            output.addGenerated(generateRangePredicate(index, prefixes, prefixLength, comparison, false));
        }
    }

    private static void addToOutputUnordered(Map<String, EqualPredicate> prefixes, Output output, InternalIndex index) {
        assert !index.isOrdered();
        output.addGenerated(generateEqualPredicate(index, prefixes, false));
    }

    private static Predicate generateEqualPredicate(InternalIndex index, Map<String, EqualPredicate> prefixes, boolean fast) {
        String[] components = index.getComponents();
        Comparable[] values = new Comparable[components.length];
        for (int i = 0; i < components.length; ++i) {
            String attribute = components[i];

            values[i] = fast ? prefixes.get(attribute).value : prefixes.remove(attribute).value;
        }
        return new CompositeEqualPredicate(index, new CompositeValue(values));
    }

    private static Predicate generateRangePredicate(InternalIndex index, Map<String, EqualPredicate> prefixes, int prefixLength,
                                                    boolean fast) {
        // see CompositeValue docs for more details on what is going on here

        String[] components = index.getComponents();

        Comparable[] from = new Comparable[components.length];
        Comparable[] to = new Comparable[components.length];
        for (int i = 0; i < prefixLength; ++i) {
            String attribute = components[i];

            Comparable value = fast ? prefixes.get(attribute).value : prefixes.remove(attribute).value;
            from[i] = value;
            to[i] = value;
        }
        for (int i = prefixLength; i < components.length; ++i) {
            from[i] = NEGATIVE_INFINITY;
            to[i] = POSITIVE_INFINITY;
        }
        return new CompositeRangePredicate(index, new CompositeValue(from), false, new CompositeValue(to), false, prefixLength);
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    private static Predicate generateRangePredicate(InternalIndex index, Map<String, EqualPredicate> prefixes, int prefixLength,
                                                    RangePredicate comparison, boolean fast) {
        // see CompositeValue docs for more details on what is going on here

        assert !(comparison instanceof EqualPredicate);
        assert comparison.getFrom() != NULL && comparison.getTo() != NULL;

        String[] components = index.getComponents();
        boolean fullyMatched = components.length == prefixLength + 1;
        boolean hasFrom = comparison.getFrom() != null;
        boolean hasTo = comparison.getTo() != null;
        assert hasFrom || hasTo;
        assert hasFrom || !comparison.isFromInclusive();
        assert hasTo || !comparison.isToInclusive();

        Comparable[] from = new Comparable[components.length];
        Comparable[] to = new Comparable[components.length];
        for (int i = 0; i < prefixLength; ++i) {
            String attribute = components[i];

            Comparable value = fast ? prefixes.get(attribute).value : prefixes.remove(attribute).value;
            from[i] = value;
            to[i] = value;
        }
        // NULL since we want to exclude nulls on the comparison component itself
        from[prefixLength] = hasFrom ? comparison.getFrom() : NULL;
        to[prefixLength] = hasTo ? comparison.getTo() : POSITIVE_INFINITY;
        for (int i = prefixLength + 1; i < components.length; ++i) {
            from[i] = !hasFrom || comparison.isFromInclusive() ? NEGATIVE_INFINITY : POSITIVE_INFINITY;
            to[i] = !hasTo || comparison.isToInclusive() ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }

        return new CompositeRangePredicate(index, new CompositeValue(from), fullyMatched && comparison.isFromInclusive(),
                new CompositeValue(to), fullyMatched && comparison.isToInclusive(), prefixLength);
    }

    private static <K, V> Map<K, V> obtainHashMap(Map<K, V> map, int capacity) {
        return map == null ? new HashMap<>(capacity) : map;
    }

    private static Output obtainOutput(Output output, int capacity) {
        return output == null ? new Output(capacity) : output;
    }

    @SuppressFBWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS")
    private static class Output extends ArrayList<Predicate> {

        private boolean requiresGeneration;

        Output(int capacity) {
            super(capacity);
        }

        public void addGenerated(Predicate predicate) {
            add(predicate);
            requiresGeneration = true;
        }

        @SuppressWarnings("checkstyle:npathcomplexity")
        public Predicate generate(Map<String, EqualPredicate> prefixes, Map<String, RangePredicate> comparisons,
                                  AndPredicate andPredicate) {
            if (!requiresGeneration) {
                return andPredicate;
            }

            // If we are here, that means we were unable to perform any fast
            // exits, so any further attempts to perform a fast exit here are
            // useless.

            int newSize = size() + prefixes.size() + (comparisons == null ? 0 : comparisons.size());
            assert newSize > 0;

            Predicate[] predicates = new Predicate[newSize];
            int index = 0;
            for (Predicate predicate : this) {
                predicates[index++] = predicate;
            }
            if (!prefixes.isEmpty()) {
                for (Predicate predicate : prefixes.values()) {
                    predicates[index++] = predicate;
                }
            }
            if (comparisons != null && !comparisons.isEmpty()) {
                for (Predicate predicate : comparisons.values()) {
                    predicates[index++] = predicate;
                }
            }
            assert index == newSize;
            return new AndPredicate(predicates);
        }
    }
}
