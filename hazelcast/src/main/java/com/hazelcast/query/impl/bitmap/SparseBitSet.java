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

package com.hazelcast.query.impl.bitmap;

import static com.hazelcast.query.impl.bitmap.BitmapUtils.capacityDeltaInt;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.capacityDeltaShort;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.toUnsignedInt;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.toUnsignedLong;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.unsignedBinarySearch;
import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.System.arraycopy;
import static java.util.Arrays.copyOf;

/**
 * Stores a set of bits indexable by non-negative {@code long} indexes.
 * <p>
 * Internally, uses a {@link SparseIntArray} indexed by the high 32 bits (32-bit
 * prefix) to resolve a storage ({@link Storage32 Storage32}) for the low 32
 * bits (32-bit postfix).
 * <p>
 * {@link Storage32 Storage32} goes in two flavors:
 * <ul>
 * <li>{@link ArrayStorage32 ArrayStorage32} which manages sorted int array of
 * 32-bit postfixes.
 * <li>{@link PrefixStorage32 PrefixStorage32} which splits 32-bit postfixes
 * into 16-bit prefixes and 16-bit postfixes. The high 16 bits are stored in
 * sorted array and used to lookup a storage ({@link Storage16 Storage16}) for
 * the low 16 bits.
 * </ul>
 * <p>
 * {@link Storage16 Storage16} also goes in two flavors:
 * <ul>
 * <li>{@link ArrayStorage16 ArrayStorage16} which manages sorted short array of
 * 16-bit postfixes.
 * <li>{@link BitSetStorage16 BitSetStorage16} which manages directly indexable
 * long array of bits.
 * </ul>
 * <p>
 * The implementation (which was inspired by Roaring Bitmap) switches between
 * various storage flavors once certain thresholds on storage size are reached.
 * <p>
 * Empty storages are never stored by the implementation.
 */
final class SparseBitSet {

    /**
     * The size at which ArrayStorage32 is converted to PrefixStorage32.
     * Inferred empirically: at this size we are not penalized too much for
     * doing binary searches.
     */
    public static final int ARRAY_STORAGE_32_MAX_SIZE = 513;

    /**
     * The size at which ArrayStorage16 is converted to BitSetStorage16. At
     * this size the memory cost of having sorted short array is equal to the
     * cost of having directly indexable long array of bits.
     */
    public static final int ARRAY_STORAGE_16_MAX_SIZE = 4096;

    private static final long INT_PREFIX_MASK = 0xFFFFFFFF00000000L;
    private static final long INT_POSTFIX_MASK = 0x00000000FFFFFFFFL;
    private static final long SHORT_PREFIX_MASK = 0x00000000FFFF0000L;
    private static final long INT_PREFIX_SHORT_POSTFIX_MASK = 0xFFFFFFFF0000FFFFL;
    private static final long INT_PREFIX_SHORT_PREFIX_MASK = 0xFFFFFFFFFFFF0000L;
    private static final long SHORT_POSTFIX_MASK = 0x000000000000FFFFL;

    private final SparseIntArray<Storage32> storages = new SparseIntArray<>();

    // used for caching of the last resolved 32-bit storage
    private int lastPrefix = -1;
    private Storage32 lastStorage;

    /**
     * Adds the given member to this bit set.
     *
     * @param member the member to add.
     */
    public void add(long member) {
        assert member >= 0;
        int prefix = (int) (member >>> Integer.SIZE);

        if (prefix == lastPrefix) {
            Storage32 newStorage = lastStorage.add((int) member);
            if (newStorage != lastStorage) {
                // storage was upgraded
                lastStorage = newStorage;
                storages.set(prefix, newStorage);
            }
        } else {
            lastPrefix = prefix;
            Storage32 storage = storages.get(prefix);
            if (storage == null) {
                Storage32 createdStorage = new ArrayStorage32((int) member);
                lastStorage = createdStorage;
                storages.set(prefix, createdStorage);
            } else {
                Storage32 newStorage = storage.add((int) member);
                if (newStorage == storage) {
                    lastStorage = storage;
                } else {
                    // storage was upgraded
                    lastStorage = newStorage;
                    storages.set(prefix, newStorage);
                }
            }
        }
    }

    /**
     * Removes the given member from this bit set.
     *
     * @param member the member to remove.
     * @return {@code true} if this storage became empty as a result of the
     * member removal, {@code false} otherwise.
     */
    public boolean remove(long member) {
        assert member >= 0;
        int prefix = (int) (member >>> Integer.SIZE);

        if (prefix == lastPrefix) {
            if (lastStorage.remove((int) member)) {
                lastPrefix = -1;
                lastStorage = null;
                return storages.clear(prefix);
            } else {
                return false;
            }
        } else {
            Storage32 storage = storages.get(prefix);
            if (storage == null) {
                return false;
            }
            if (storage.remove((int) member)) {
                lastPrefix = -1;
                lastStorage = null;
                return storages.clear(prefix);
            } else {
                lastPrefix = prefix;
                lastStorage = storage;
                return false;
            }
        }
    }

    /**
     * @return an iterator that iterates over all the indexes of bits set in
     * this sparse bit set.
     */
    public AscendingLongIterator iterator() {
        return new IteratorImpl(storages);
    }

    /**
     * Defines internal contract of storages responsible for storing of 32-bit
     * postfixes.
     */
    private interface Storage32 {

        /**
         * Adds the given member to this storage.
         *
         * @param member the member to add.
         * @return a new storage instance if this storage was converted to
         * another storage flavor; this storage otherwise.
         */
        Storage32 add(int member);

        /**
         * Removes the given member from this storage.
         *
         * @param member the member to remove.
         * @return {@code true} if this storage became empty as a result of the
         * member removal, {@code false} otherwise.
         */
        boolean remove(int member);

        /**
         * Starts iteration on this storage using the given iterator.
         * <p>
         * Always succeeds since we never keep empty storages.
         *
         * @param iterator the iterator to iterate with.
         */
        void iterate(IteratorImpl iterator);

        /**
         * Advances the given iterator on this storage.
         *
         * @param iterator the iterator to advance.
         * @return {@code true} if the iterator is advanced to the next member,
         * {@code false} if no members to iterate are left in this storage.
         */
        boolean advance(IteratorImpl iterator);

        /**
         * Starts iteration on this storage starting from the given member using
         * the given iterator.
         *
         * @param member   the member to start the iteration from.
         * @param iterator the iterator to iterate with.
         * @return {@code true} if the iterator is positioned to the given
         * member; or, if the member is not present in this storage, to a member
         * immediately following it and present in this storage or {@code false}
         * if no such member exists in this storage.
         */
        boolean iterateAtLeastFrom(int member, IteratorImpl iterator);

        /**
         * Advances the given iterator to the given member; or, if the member is
         * not present in this storage, to a member immediately following it
         * and present in this storage.
         *
         * @param member   the member to advance at least to. The member must be
         *                 greater than the member this iterator is currently at.
         * @param iterator the iterator to advance.
         * @return {@code true} if the iterator is advanced to the given
         * member; or, if the member is not present in this storage, to a member
         * immediately following it and present in this storage or {@code false}
         * if no such member exists in this storage.
         */
        boolean advanceAtLeastTo(int member, IteratorImpl iterator);

    }

    /**
     * Manages sorted int array of indexes of set bits.
     */
    private static final class ArrayStorage32 implements Storage32 {

        private static final int MIN_CAPACITY = 1;

        private int size;
        private int[] members;

        ArrayStorage32(int member) {
            this.size = 1;
            this.members = new int[MIN_CAPACITY];
            members[0] = member;
        }

        @Override
        public Storage32 add(int member) {
            int index = unsignedBinarySearch(members, size, toUnsignedLong(member));
            if (index >= 0) {
                // already in the array
                return this;
            }
            index = -(index + 1);

            if (size == members.length) {
                // No space left: try to grow members array.

                if (size == ARRAY_STORAGE_32_MAX_SIZE) {
                    return new PrefixStorage32(members, member, index);
                }

                int newCapacity = Math.min(ARRAY_STORAGE_32_MAX_SIZE, size + capacityDeltaInt(members.length));
                int[] newMembers = new int[newCapacity];
                arraycopy(members, 0, newMembers, 0, index);
                arraycopy(members, index, newMembers, index + 1, size - index);
                members = newMembers;
            } else {
                // shift members right to free a slot for the new member
                arraycopy(members, index, members, index + 1, size - index);
            }
            members[index] = member;
            ++size;
            return this;
        }

        @Override
        public boolean remove(int member) {
            int index = unsignedBinarySearch(members, size, toUnsignedLong(member));
            if (index < 0) {
                // not a member
                return false;
            }

            --size;
            if (size == 0) {
                // emptied
                return true;
            }

            int delta = capacityDeltaInt(members.length);
            int wasted = members.length - size;
            int newCapacity = members.length - delta;
            if (wasted >= delta && newCapacity >= MIN_CAPACITY) {
                // We are wasting too much: shrink the array.

                int[] newMembers = new int[newCapacity];
                arraycopy(members, 0, newMembers, 0, index);
                arraycopy(members, index + 1, newMembers, index, size - index);
                members = newMembers;
            } else {
                // shift members left to fill the gap
                arraycopy(members, index + 1, members, index, size - index);
            }
            return false;
        }

        @Override
        public void iterate(IteratorImpl iterator) {
            assert size > 0;
            iterator.position32 = 1;
            iterator.index = iterator.index & INT_PREFIX_MASK | toUnsignedLong(members[0]);
        }

        @Override
        public boolean advance(IteratorImpl iterator) {
            int position = iterator.position32;
            if (position < size) {
                iterator.index = iterator.index & INT_PREFIX_MASK | toUnsignedLong(members[position]);
                iterator.position32 = position + 1;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean iterateAtLeastFrom(int member, IteratorImpl iterator) {
            long unsignedMember = toUnsignedLong(member);
            int position = unsignedBinarySearch(members, size, unsignedMember);

            if (position < 0) {
                position = -(position + 1);
                if (position == size) {
                    return false;
                }
                unsignedMember = toUnsignedLong(members[position]);
            }

            iterator.index = iterator.index & INT_PREFIX_MASK | unsignedMember;
            iterator.position32 = position + 1;
            return true;
        }

        @Override
        public boolean advanceAtLeastTo(int member, IteratorImpl iterator) {
            long unsignedMember = toUnsignedLong(member);
            long current = iterator.index;
            assert (current & INT_POSTFIX_MASK) < unsignedMember;

            int position = iterator.position32;
            if (position == size) {
                return false;
            }

            position = unsignedBinarySearch(members, position, size, unsignedMember);

            if (position < 0) {
                position = -(position + 1);
                if (position == size) {
                    return false;
                }
                unsignedMember = toUnsignedLong(members[position]);
            }

            iterator.index = current & INT_PREFIX_MASK | unsignedMember;
            iterator.position32 = position + 1;
            return true;
        }

    }

    /**
     * Manages sorted array of 16-bit prefixes to lookup storages for 16-bit
     * postfixes.
     */
    private static final class PrefixStorage32 implements Storage32 {

        private static final int MIN_CAPACITY = 2;
        private static final int MAX_CAPACITY = 64 * 1024;

        private int size;
        private short[] prefixes;
        private Storage16[] storages;

        // used for caching of the last resolved 16-bit storage
        private int lastPrefix = -1;
        private Storage16 lastStorage;

        /**
         * Constructs a new prefix storage for the given sorted members array
         * and the given member to insert at the given index.
         */
        PrefixStorage32(int[] members, int member, int index) {
            this.prefixes = new short[MIN_CAPACITY];
            this.storages = new Storage16[MIN_CAPACITY];

            for (int i = 0; i < index; ++i) {
                append(members[i]);
            }
            append(member);
            for (int i = index; i < members.length; ++i) {
                append(members[i]);
            }
        }

        @Override
        public Storage32 add(int member) {
            short prefix = (short) (member >>> Short.SIZE);
            int unsignedPrefix = toUnsignedInt(prefix);

            // Try to resolve the corresponding 16-bit postfix storage by its
            // 16-bit prefix.

            if (unsignedPrefix == lastPrefix) {
                // We are lucky: just add the member to the cached storage.

                Storage16 newStorage = lastStorage.add((short) member);
                // handle potential storage upgrade
                if (newStorage != lastStorage) {
                    int index = unsignedBinarySearch(prefixes, size, unsignedPrefix);
                    assert index >= 0;
                    storages[index] = newStorage;
                    lastStorage = newStorage;
                }
                return this;
            }

            int index = unsignedBinarySearch(prefixes, size, unsignedPrefix);
            if (index >= 0) {
                // The storage already exists: just add the member to it.

                Storage16 storage = storages[index];
                Storage16 newStorage = storage.add((short) member);
                // handle potential storage upgrade
                if (newStorage != storage) {
                    storages[index] = newStorage;
                }
                lastPrefix = unsignedPrefix;
                lastStorage = newStorage;
                return this;
            }
            index = -(index + 1);

            // No storage exists yet: we need to allocate a new postfix storage
            // and insert it into this prefix storage.

            if (size == prefixes.length) {
                // Grow the arrays.

                int newCapacity = Math.min(MAX_CAPACITY, size + capacityDeltaShort(prefixes.length));

                short[] newPrefixes = new short[newCapacity];
                arraycopy(prefixes, 0, newPrefixes, 0, index);
                arraycopy(prefixes, index, newPrefixes, index + 1, size - index);
                prefixes = newPrefixes;

                Storage16[] newStorages = new Storage16[newCapacity];
                arraycopy(storages, 0, newStorages, 0, index);
                arraycopy(storages, index, newStorages, index + 1, size - index);
                storages = newStorages;
            } else {
                // Shift the arrays right to free a slot.

                arraycopy(prefixes, index, prefixes, index + 1, size - index);
                arraycopy(storages, index, storages, index + 1, size - index);
            }

            ArrayStorage16 createdStorage = new ArrayStorage16((short) member);
            prefixes[index] = prefix;
            storages[index] = createdStorage;
            lastPrefix = unsignedPrefix;
            lastStorage = createdStorage;
            ++size;
            return this;
        }

        @Override
        public boolean remove(int member) {
            short prefix = (short) (member >>> Short.SIZE);
            int unsignedPrefix = toUnsignedInt(prefix);

            // Try to resolve the corresponding 16-bit postfix storage by its
            // 16-bit prefix.

            Storage16 newStorage;
            int index;

            if (unsignedPrefix == lastPrefix) {
                // We are lucky: just remove the member from the cached storage.

                Storage16 storage = lastStorage;
                newStorage = storage.remove((short) member);
                if (newStorage == storage) {
                    return false;
                }
                // To handle the storage downgrade or removal we need to know
                // its prefix index.
                index = unsignedBinarySearch(prefixes, size, unsignedPrefix);
                assert index >= 0;
            } else {
                index = unsignedBinarySearch(prefixes, size, unsignedPrefix);
                if (index < 0) {
                    // no storage, no problems
                    return false;
                }

                Storage16 storage = storages[index];
                newStorage = storage.remove((short) member);
                if (newStorage == storage) {
                    lastStorage = storage;
                    lastPrefix = unsignedPrefix;
                    return false;
                }
            }

            // The 16-bit postfix storage is either emptied or downgraded at
            // this point.

            if (newStorage == null) {
                // The postfix storage is emptied: remove it from this prefix
                // storage.

                --size;
                lastStorage = null;
                lastPrefix = -1;
                if (size == 0) {
                    // this prefix storage is also emptied
                    return true;
                }

                int delta = capacityDeltaShort(prefixes.length);
                int wasted = prefixes.length - size;
                int newCapacity = prefixes.length - delta;
                if (wasted >= delta && newCapacity >= MIN_CAPACITY) {
                    // Wasting too much: shrink the arrays.

                    short[] newPrefixes = new short[newCapacity];
                    arraycopy(prefixes, 0, newPrefixes, 0, index);
                    arraycopy(prefixes, index + 1, newPrefixes, index, size - index);
                    prefixes = newPrefixes;

                    Storage16[] newStorages = new Storage16[newCapacity];
                    arraycopy(storages, 0, newStorages, 0, index);
                    arraycopy(storages, index + 1, newStorages, index, size - index);
                    storages = newStorages;
                } else {
                    // Shift the arrays left to fill the gap.

                    arraycopy(prefixes, index + 1, prefixes, index, size - index);
                    arraycopy(storages, index + 1, storages, index, size - index);
                }
            } else {
                // The postfix storage is downgraded: update the records.

                lastStorage = newStorage;
                lastPrefix = unsignedPrefix;
                storages[index] = newStorage;
            }
            return false;
        }

        @Override
        public void iterate(IteratorImpl iterator) {
            assert size > 0;
            iterator.position32 = 1;
            Storage16 storage = storages[0];
            iterator.storage16 = storage;
            iterator.index = iterator.index & INT_PREFIX_MASK | toUnsignedLong(prefixes[0]) << Short.SIZE;
            storage.iterate(iterator);
        }

        @Override
        public boolean advance(IteratorImpl iterator) {
            // Try advance the current postfix storage.

            if (iterator.storage16.advance(iterator)) {
                return true;
            }

            // Try to advance to the next postfix storage and iterate it.

            int position = iterator.position32;
            if (position < size) {
                Storage16 storage = storages[position];
                iterator.storage16 = storage;
                iterator.index = iterator.index & INT_PREFIX_MASK | toUnsignedLong(prefixes[position]) << Short.SIZE;
                iterator.position32 = position + 1;
                storage.iterate(iterator);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean iterateAtLeastFrom(int member, IteratorImpl iterator) {
            return iterateAtLeastFrom(member, 0, iterator);
        }

        @Override
        public boolean advanceAtLeastTo(int member, IteratorImpl iterator) {
            short prefix = (short) (member >>> Short.SIZE);
            int unsignedPrefix = toUnsignedInt(prefix);
            long current = iterator.index;
            int currentUnsignedPrefix = (int) (current & SHORT_PREFIX_MASK) >>> Short.SIZE;
            assert currentUnsignedPrefix <= unsignedPrefix;

            if (unsignedPrefix == currentUnsignedPrefix) {
                // The requested member is within the current 16-bit postfix
                // storage.

                if (iterator.storage16.advanceAtLeastTo((short) member, iterator)) {
                    // found in the current postfix storage
                    return true;
                }

                int position = iterator.position32;
                if (position == size) {
                    // the end
                    return false;
                }

                // Iterate the next prefix.

                Storage16 storage = storages[position];
                iterator.storage16 = storage;
                iterator.index = current & INT_PREFIX_MASK | toUnsignedLong(prefixes[position]) << Short.SIZE;
                iterator.position32 = position + 1;
                storage.iterate(iterator);
                return true;
            }

            // Resolve and iterate the requested prefix.

            int position = iterator.position32;
            if (position == size) {
                return false;
            }
            return iterateAtLeastFrom(member, position, iterator);
        }

        private void append(int member) {
            short prefix = (short) (member >>> Short.SIZE);

            if (size != 0 && prefix == prefixes[size - 1]) {
                ((ArrayStorage16) storages[size - 1]).append((short) member);
                return;
            }

            if (size == prefixes.length) {
                int newCapacity = Math.min(MAX_CAPACITY, size + capacityDeltaShort(prefixes.length));
                prefixes = copyOf(prefixes, newCapacity);
                storages = copyOf(storages, newCapacity);
            }

            prefixes[size] = prefix;
            storages[size] = new ArrayStorage16((short) member);
            ++size;
        }

        private boolean iterateAtLeastFrom(int member, int fromPosition, IteratorImpl iterator) {
            short prefix = (short) (member >>> Short.SIZE);
            int position = unsignedBinarySearch(prefixes, fromPosition, size, toUnsignedInt(prefix));

            if (position < 0) {
                position = -(position + 1);
                if (position == size) {
                    // no such member
                    return false;
                }
            }

            Storage16 storage = storages[position];
            if (storage.iterateAtLeastFrom((short) member, iterator)) {
                // The postfix storage corresponding to the requested member is
                // successfully advanced at least to the requested member.

                iterator.storage16 = storage;
                iterator.index =
                        iterator.index & INT_PREFIX_SHORT_POSTFIX_MASK | toUnsignedLong(prefixes[position]) << Short.SIZE;
                iterator.position32 = position + 1;
            } else {
                // The postfix storage corresponding to the requested member
                // doesn't contain the requested member or any members greater
                // than it: try to iterate from the next prefix storage.

                ++position;
                if (position == size) {
                    return false;
                }

                storage = storages[position];
                iterator.storage16 = storage;
                iterator.index = iterator.index & INT_PREFIX_MASK | toUnsignedLong(prefixes[position]) << Short.SIZE;
                iterator.position32 = position + 1;
                storage.iterate(iterator);
            }
            return true;
        }

    }

    /**
     * Defines internal contract of storages responsible for storing of 16-bit
     * postfixes.
     */
    private interface Storage16 {

        /**
         * Adds the given member to this storage.
         *
         * @param member the member to add.
         * @return a new storage instance if this storage was converted to
         * another storage flavor; this storage otherwise.
         */
        Storage16 add(short member);

        /**
         * Removes the given member from this storage.
         *
         * @param member the member to remove.
         * @return {@code null} if this storage became empty as a result of the
         * member removal; a new storage instance if this storage was converted
         * to another storage flavor; this storage otherwise.
         */
        Storage16 remove(short member);

        /**
         * Starts iteration on this storage using the given iterator.
         * <p>
         * Always succeeds since we never keep empty storages.
         *
         * @param iterator the iterator to iterate with.
         */
        void iterate(IteratorImpl iterator);

        /**
         * Advances the given iterator on this storage.
         *
         * @param iterator the iterator to advance.
         * @return {@code true} if the iterator is advanced to the next member,
         * {@code false} if no members to iterate are left in this storage.
         */
        boolean advance(IteratorImpl iterator);

        /**
         * Starts iteration on this storage starting at least from the given
         * member using the given iterator.
         *
         * @param member   the member to start the iteration from.
         * @param iterator the iterator to iterate with.
         * @return {@code true} if the iterator is positioned to the given
         * member; or, if the member is not present in this storage, to a member
         * immediately following it and present in this storage or {@code false}
         * if no such member exists in this storage.
         */
        boolean iterateAtLeastFrom(short member, IteratorImpl iterator);

        /**
         * Advances the given iterator to the given member; or, if the member is
         * not present in this storage, to a member immediately following it and
         * present in this storage.
         *
         * @param member   the member to advance at least to. The member must be
         *                 greater than the member this iterator is currently at.
         * @param iterator the iterator to advance.
         * @return {@code true} if the iterator is advanced to the given
         * member; or, if the member is not present in this storage, to a member
         * immediately following it and present in this storage or {@code false}
         * if no such member exists in this storage.
         */
        boolean advanceAtLeastTo(short member, IteratorImpl iterator);

    }

    /**
     * Manages sorted short array of indexes of set bits.
     */
    private static final class ArrayStorage16 implements Storage16 {

        private static final int MIN_CAPACITY = 2;

        private int size;
        private short[] members;

        ArrayStorage16(short member) {
            this.size = 1;
            this.members = new short[MIN_CAPACITY];
            members[0] = member;
        }

        /**
         * Constructs a new storage by downgrading from the given {@link
         * BitSetStorage16} data.
         */
        ArrayStorage16(long[] bits, int size) {
            assert size == BitSetStorage16.MIN_SIZE;
            this.size = size;

            short[] members = new short[ARRAY_STORAGE_16_MAX_SIZE];
            int index = 0;
            for (int i = 0; i < bits.length; ++i) {
                long value = bits[i];
                int base = i << BitSetStorage16.BIT_SET_LONG_SHIFT;
                while (value != 0) {
                    int offset = numberOfTrailingZeros(value);
                    members[index++] = (short) (base + offset);
                    // zero out the consumed bit
                    value &= value - 1;
                }
            }
            assert index == size;

            this.members = members;
        }

        @Override
        public Storage16 add(short member) {
            int index = unsignedBinarySearch(members, size, toUnsignedInt(member));
            if (index >= 0) {
                // already in the array
                return this;
            }
            index = -(index + 1);

            if (size == members.length) {
                // No space left: try to grow members array.

                if (size == ARRAY_STORAGE_16_MAX_SIZE) {
                    return new BitSetStorage16(members, member, index);
                }

                int newCapacity = Math.min(ARRAY_STORAGE_16_MAX_SIZE, size + capacityDeltaShort(members.length));
                short[] newMembers = new short[newCapacity];
                arraycopy(members, 0, newMembers, 0, index);
                arraycopy(members, index, newMembers, index + 1, size - index);
                members = newMembers;
            } else {
                // shift members right to free a slot for the new member
                arraycopy(members, index, members, index + 1, size - index);
            }
            members[index] = member;
            ++size;
            return this;
        }

        @Override
        public Storage16 remove(short member) {
            int index = unsignedBinarySearch(members, size, toUnsignedInt(member));
            if (index < 0) {
                // not a member
                return this;
            }

            --size;
            if (size == 0) {
                // emptied
                return null;
            }

            int delta = capacityDeltaShort(members.length);
            int wasted = members.length - size;
            int newCapacity = members.length - delta;
            if (wasted >= delta && newCapacity >= MIN_CAPACITY) {
                // We are wasting too much: shrink the array.

                short[] newMembers = new short[newCapacity];
                arraycopy(members, 0, newMembers, 0, index);
                arraycopy(members, index + 1, newMembers, index, size - index);
                members = newMembers;
            } else {
                // shift members left to fill the gap
                arraycopy(members, index + 1, members, index, size - index);
            }
            return this;
        }

        @Override
        public void iterate(IteratorImpl iterator) {
            assert size > 0;
            iterator.position16 = 1;
            iterator.index = iterator.index & INT_PREFIX_SHORT_PREFIX_MASK | toUnsignedInt(members[0]);
        }

        @Override
        public boolean advance(IteratorImpl iterator) {
            int index = iterator.position16;
            if (index < size) {
                iterator.index = iterator.index & INT_PREFIX_SHORT_PREFIX_MASK | toUnsignedInt(members[index]);
                iterator.position16 = index + 1;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean iterateAtLeastFrom(short member, IteratorImpl iterator) {
            int unsignedMember = toUnsignedInt(member);
            int index = unsignedBinarySearch(members, size, unsignedMember);

            if (index < 0) {
                index = -(index + 1);
                if (index == size) {
                    return false;
                }
                unsignedMember = toUnsignedInt(members[index]);
            }

            iterator.index = iterator.index & INT_PREFIX_SHORT_PREFIX_MASK | unsignedMember;
            iterator.position16 = index + 1;
            return true;
        }

        @Override
        public boolean advanceAtLeastTo(short member, IteratorImpl iterator) {
            int unsignedMember = toUnsignedInt(member);
            long current = iterator.index;
            assert (current & SHORT_POSTFIX_MASK) < unsignedMember;

            int position = iterator.position16;
            if (position == size) {
                return false;
            }
            position = unsignedBinarySearch(members, position, size, unsignedMember);

            if (position < 0) {
                position = -(position + 1);
                if (position == size) {
                    return false;
                }
                unsignedMember = toUnsignedInt(members[position]);
            }

            iterator.index = current & INT_PREFIX_SHORT_PREFIX_MASK | unsignedMember;
            iterator.position16 = position + 1;
            return true;
        }

        /**
         * Appends the given member to this storage. The given member must be
         * greater than any member already known by this storage.
         */
        public void append(short member) {
            if (size == members.length) {
                int newCapacity = size + capacityDeltaShort(members.length);
                assert newCapacity <= ARRAY_STORAGE_16_MAX_SIZE;
                members = copyOf(members, newCapacity);
            }
            members[size] = member;
            ++size;
        }

    }

    /**
     * Manages directly indexable long array of bits.
     */
    private static final class BitSetStorage16 implements Storage16 {

        // 2^6 = 64 = number of bits a long can store
        public static final int BIT_SET_LONG_SHIFT = 6;

        private static final int MIN_SIZE = ARRAY_STORAGE_16_MAX_SIZE - 1;
        private static final int SIZE = 1024;

        // masks lower 6 bits
        private static final long POSTFIX_MASK = 0xFFFFFFFFFFFFFFC0L;

        private final long[] members = new long[SIZE];
        private int size;

        /**
         * Constructs a new bit set storage for the given sorted members array
         * and the given member to insert at the given index.
         */
        BitSetStorage16(short[] members, short member, int index) {
            for (int i = 0; i < index; ++i) {
                append(members[i]);
            }
            append(member);
            for (int i = index; i < members.length; ++i) {
                append(members[i]);
            }
            this.size = members.length + 1;
        }

        @Override
        public Storage16 add(short member) {
            int bitIndex = toUnsignedInt(member);
            int longIndex = bitIndex >>> BIT_SET_LONG_SHIFT;

            long bitSet = members[longIndex];
            long newBitSet = bitSet | 1L << bitIndex;
            members[longIndex] = newBitSet;

            if (newBitSet != bitSet) {
                ++size;
            }
            return this;
        }

        @Override
        public Storage16 remove(short member) {
            int bitIndex = toUnsignedInt(member);
            int longIndex = bitIndex >>> BIT_SET_LONG_SHIFT;

            long bitSet = members[longIndex];
            long newBitSet = bitSet & ~(1L << bitIndex);
            members[longIndex] = newBitSet;

            if (newBitSet != bitSet) {
                --size;
                if (size == MIN_SIZE) {
                    return new ArrayStorage16(members, size);
                }
            }
            return this;
        }

        @Override
        public void iterate(IteratorImpl iterator) {
            assert size > 0;
            iterator.position16 = 0;
            iterator.bitSet16 = members[0];
            iterator.index = iterator.index & INT_PREFIX_SHORT_PREFIX_MASK;
            boolean advanced = advance(iterator);
            assert advanced;
        }

        @Override
        public boolean advance(IteratorImpl iterator) {
            // Consume bits from the current long until it's empty.

            long bitSet = iterator.bitSet16;
            if (bitSet != 0) {
                iterator.index = iterator.index & POSTFIX_MASK | numberOfTrailingZeros(bitSet);
                // zero out the consumed bit
                iterator.bitSet16 = bitSet & bitSet - 1;
                return true;
            }

            // Try to find the next non-zero long.

            int index = iterator.position16;
            do {
                ++index;
                if (index == members.length) {
                    // nothing left
                    return false;
                }
                bitSet = members[index];
            } while (bitSet == 0);

            iterator.index =
                    iterator.index & INT_PREFIX_SHORT_PREFIX_MASK | index << BIT_SET_LONG_SHIFT | numberOfTrailingZeros(bitSet);
            iterator.bitSet16 = bitSet & bitSet - 1;
            iterator.position16 = index;
            return true;
        }

        @Override
        public boolean iterateAtLeastFrom(short member, IteratorImpl iterator) {
            int bitIndex = toUnsignedInt(member);
            int longIndex = bitIndex >>> BIT_SET_LONG_SHIFT;

            iterator.position16 = longIndex;
            // consume all preceding bits by zeroing them out
            iterator.bitSet16 = members[longIndex] & -(1L << bitIndex);
            iterator.index = iterator.index & INT_PREFIX_SHORT_PREFIX_MASK | longIndex << BIT_SET_LONG_SHIFT;
            return advance(iterator);
        }

        @Override
        public boolean advanceAtLeastTo(short member, IteratorImpl iterator) {
            long current = iterator.index;
            int bitIndex = toUnsignedInt(member);
            assert (current & SHORT_POSTFIX_MASK) < bitIndex;

            int longIndex = bitIndex >>> BIT_SET_LONG_SHIFT;

            iterator.position16 = longIndex;
            // consume all preceding bits by zeroing them out
            iterator.bitSet16 = members[longIndex] & -(1L << bitIndex);
            iterator.index = current & INT_PREFIX_SHORT_PREFIX_MASK | longIndex << BIT_SET_LONG_SHIFT;
            return advance(iterator);
        }

        private void append(short member) {
            int bitIndex = toUnsignedInt(member);
            members[bitIndex >>> BIT_SET_LONG_SHIFT] |= 1L << bitIndex;
        }

    }

    /**
     * Iterates over sparse bit sets.
     */
    private static final class IteratorImpl extends SparseIntArray.Iterator<Storage32> implements AscendingLongIterator {

        // The idea: use a single iterator instance to iterate over the entire
        // bit set including all its internal storages. This way we are avoiding
        // frequent sub iterators allocation, producing no heap litter and
        // keeping the iteration state just in a few cache lines.

        // the root storage mapping 32-bit prefixes to 32-bit postfix storages
        private final SparseIntArray<Storage32> storage64;

        // the current position of the current Storage32
        private int position32;

        // the current Storage16
        private Storage16 storage16;
        // its position
        private int position16;
        // the current bit set of BitSetStorage16
        private long bitSet16;

        // the current index (member), constructed cooperatively by all storages
        private long index;

        IteratorImpl(SparseIntArray<Storage32> storage64) {
            this.storage64 = storage64;
            long prefix = storage64.iterate(this);
            if (prefix != SparseIntArray.Iterator.END) {
                index = prefix << Integer.SIZE;
                getStorage32().iterate(this);
            } else {
                index = AscendingLongIterator.END;
            }
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public long advance() {
            long current = index;
            if (current == AscendingLongIterator.END) {
                return AscendingLongIterator.END;
            }

            if (getStorage32().advance(this)) {
                return current;
            }

            long prefix = storage64.advance((int) (current >>> Integer.SIZE), this);
            if (prefix != SparseIntArray.Iterator.END) {
                index = prefix << Integer.SIZE;
                getStorage32().iterate(this);
                return current;
            }

            index = AscendingLongIterator.END;
            return current;
        }

        @SuppressWarnings("checkstyle:nestedifdepth")
        @Override
        public long advanceAtLeastTo(long member) {
            assert member >= 0;
            long current = index;
            if (current == AscendingLongIterator.END) {
                return AscendingLongIterator.END;
            }

            if (current >= member) {
                // Already at or beyond the requested member.

                return current;
            }

            int memberPrefix = (int) (member >>> Integer.SIZE);
            int currentPrefix = (int) (current >>> Integer.SIZE);

            if (memberPrefix == currentPrefix) {
                // Try to advance the current storage.

                if (getStorage32().advanceAtLeastTo((int) member, this)) {
                    return index;
                } else {
                    // Try to advance to the next storage.

                    long prefix = storage64.advance(currentPrefix, this);
                    if (prefix != SparseIntArray.Iterator.END) {
                        index = prefix << Integer.SIZE;
                        getStorage32().iterate(this);
                        return index;
                    }
                }
            } else {
                // Try to advance to the requested storage.

                long prefix = storage64.advanceAtLeastTo(memberPrefix, currentPrefix, this);
                if (prefix != SparseIntArray.Iterator.END) {
                    if (prefix == memberPrefix) {
                        // We got the storage we have requested.

                        index = prefix << Integer.SIZE;
                        if (getStorage32().iterateAtLeastFrom((int) member, this)) {
                            return index;
                        } else {
                            // The storage we got doesn't contain the requested
                            // postfix or any postfix beyond it: try to advance
                            // to the next storage (storages are guaranteed to
                            // be non-empty).

                            prefix = storage64.advance((int) prefix, this);
                            if (prefix != SparseIntArray.Iterator.END) {
                                index = prefix << Integer.SIZE;
                                getStorage32().iterate(this);
                                return index;
                            }
                        }
                    } else {
                        // We got a storage corresponding to some other prefix
                        // beyond the requested one: just iterate the storage
                        // (storages are guaranteed to be non-empty).

                        index = prefix << Integer.SIZE;
                        getStorage32().iterate(this);
                        return index;
                    }
                }
            }

            // The end.

            index = AscendingLongIterator.END;
            return AscendingLongIterator.END;
        }

        // just an alias for getValue
        private Storage32 getStorage32() {
            return getValue();
        }

    }

}
