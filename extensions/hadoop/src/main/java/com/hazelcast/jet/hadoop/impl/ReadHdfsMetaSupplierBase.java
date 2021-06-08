/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.file.impl.FileProcessorMetaSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.concat;

/**
 * Base class that contains shared logic between HDFS processor meta suppliers
 * which are using the old and the new MapReduce API.
 */
public abstract class ReadHdfsMetaSupplierBase<R> implements ProcessorMetaSupplier, FileProcessorMetaSupplier<R> {

    protected transient ILogger logger;

    @Override
    public void init(@Nonnull Context context) throws Exception {
        logger = context.logger();
    }

    @Override
    public int preferredLocalParallelism() {
        return 2;
    }

    private static int indexOfMin(int[] ints) {
        return range(0, ints.length)
                .boxed()
                .min(comparingInt(i -> ints[i]))
                .orElseThrow(() -> new AssertionError("empty array"));
    }

    private static <T> T getTheOnlyItem(Collection<T> coll) {
        if (coll.size() != 1) {
            throw new AssertionError("Collection does not have exactly one item: " + coll);
        }
        return coll.iterator().next();
    }

    /**
     * Heuristically solves the following problem:
     * <ul><li>
     * HDFS stores a file by dividing it into blocks and storing each block
     * on several machines.
     * </li><li>
     * To read a file, the client asks HDFS to group the blocks into a number
     * of <em>splits</em> that can be read independently. All the blocks of
     * a given split are stored on the same set of machines. The client can
     * only request the minimum number of splits, but the exact number is up
     * to HDFS.
     * </li><li>
     * {@code ReadHdfsP} acquires some splits and must plan out which splits
     * will be read by which Jet cluster member. The first concern is data
     * locality: if a split is local to a member, that member must read it.
     * Some splits may not be on any Jet member; these can be assigned
     * arbitrarily, but overall balance across members must be maintained.
     * </li><li>
     * Since each split is stored on several machines, usually there are
     * several candidate members for each split. This results in an NP
     * constraint-solving problem.
     * </li></ul>
     * This is a high-level outline of the heuristic algorithm:
     * <ol><li>
     * Build a mapping from split to the candidate set of members that might
     * read it:
     * <ol><li>
     * for each split, form the candidate set from all members which have it
     * locally;
     * </li><li>
     * for each candidate set that is still empty, replace it with a singleton
     * set containing the member that occurs in the fewest of other candidate
     * sets.
     * </li></ol>
     * </li><li>
     * Circularly iterate over all candidate sets, removing from each
     * non-singleton set the member that occurs in the largest number of other
     * candidate sets.
     * </li></ol>
     */
    Map<Address, List<IndexedInputSplit>> assignSplitsToMembers(
            IndexedInputSplit[] indexedSplits, Address[] memberAddrs
    ) {
        Map<IndexedInputSplit, Set<Integer>> splitToCandidates = new TreeMap<>();
        int[] memberToSplitCount = new int[memberAddrs.length];

        // Each member that has the split locally is a candidate
        for (IndexedInputSplit is : indexedSplits) {
            splitToCandidates.put(is,
                    range(0, memberAddrs.length)
                            .filter(i -> {
                                try {
                                    return isSplitLocalForMember(is.getLocations(), memberAddrs[i]);
                                } catch (Exception e) {
                                    throw ExceptionUtil.rethrow(e);
                                }
                            })
                            .peek(i -> memberToSplitCount[i]++)
                            .boxed()
                            .collect(toSet())
            );
        }
        // for each split not local to any member, assign it to the member
        // with the least splits assigned so far
        splitToCandidates.entrySet().stream()
                         .filter(e -> e.getValue().isEmpty())
                         .peek(e -> logger.info(
                                 "No local member found for " + e.getKey() + ", will be read remotely."))
                         .map(Entry::getValue)
                         .forEach(memberIndexes -> {
                             int target = indexOfMin(memberToSplitCount);
                             memberIndexes.add(target);
                             memberToSplitCount[target]++;
                         });
        logger.info("Split counts per member before uniquifying: " + Arrays.toString(memberToSplitCount));

        // decide on a unique member for each split
        boolean[] foundNonUnique = new boolean[1];
        do {
            foundNonUnique[0] = false;
            splitToCandidates
                    .values().stream()
                    .filter(memberIndexes -> memberIndexes.size() > 1)
                    .peek(x -> foundNonUnique[0] = true)
                    .forEach(memberIndexes -> {
                        int memberWithMostSplits = memberIndexes
                                .stream()
                                .max(comparingInt(i -> memberToSplitCount[i]))
                                .get();
                        memberIndexes.remove(memberWithMostSplits);
                        memberToSplitCount[memberWithMostSplits]--;
                    });
        } while (foundNonUnique[0]);
        logger.info("Final split counts per member: " + Arrays.toString(memberToSplitCount));
        return splitToCandidates.entrySet().stream()
                                .map(e -> entry(e.getKey(), memberAddrs[getTheOnlyItem(e.getValue())]))
                                .collect(groupingBy(Entry::getValue, mapping(Entry::getKey, toList())));
    }

    void printAssignments(Map<Address, List<IndexedInputSplit>> assigned) {
        logger.info("Member-to-split assignment: " +
                assigned.entrySet().stream().flatMap(e -> concat(
                        Stream.of(e.getKey() + ":"),
                        Optional.of(e.getValue()).orElse(emptyList()).stream().map(Object::toString))
                ).collect(joining("\n")));
    }

    private boolean isSplitLocalForMember(List<String> splitLocations, Address memberAddr) {
        try {
            final InetAddress inetAddr = memberAddr.getInetAddress();
            return splitLocations
                    .stream()
                    .flatMap(loc -> Arrays.stream(uncheckCall(() -> InetAddress.getAllByName(loc))))
                    .anyMatch(inetAddr::equals);
        } catch (UnknownHostException e) {
            logger.warning("Failed to resolve host name for the split, " +
                    "will use host name equality to determine data locality", e);
            return isSplitLocalForMember(splitLocations, memberAddr.getScopedHost());
        }
    }

    private static boolean isSplitLocalForMember(List<String> splitLocations, String hostName) {
        if (hostName == null) {
            return false;
        }
        return splitLocations.stream().anyMatch(l -> equalsIgnoreCase(l, hostName));
    }
}
