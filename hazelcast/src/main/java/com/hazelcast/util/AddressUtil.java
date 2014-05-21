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

package com.hazelcast.util;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;

public final class AddressUtil {

    private static final int NUMBER_OF_ADDRESSES = 255;

    private AddressUtil() {
    }

    public static boolean matchAnyInterface(String address, Collection<String> interfaces) {
        if (interfaces == null || interfaces.size() == 0) {
            return false;
        }
        for (String interfaceMask : interfaces) {
            if (matchInterface(address, interfaceMask)) {
                return true;
            }
        }
        return false;
    }

    public static boolean matchInterface(String address, String interfaceMask) {
        final AddressMatcher mask;
        try {
            mask = getAddressMatcher(interfaceMask);
        } catch (Exception e) {
            return false;
        }
        return mask.match(address);
    }

    public static boolean matchAnyDomain(String name, Collection<String> patterns) {
        if (patterns == null || patterns.size() == 0) {
            return false;
        }
        for (String pattern : patterns) {
            if (matchDomain(name, pattern)) {
                return true;
            }
        }
        return false;
    }

    public static boolean matchDomain(String name, String pattern) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return name.equals(pattern);
        } else {
            String[] names = name.split("\\.");
            String[] patterns = pattern.split("\\.");
            if (patterns.length > names.length) {
                return false;
            }
            int nameIndexDiff = names.length - patterns.length;
            for (int i = patterns.length - 1; i > -1; i--) {
                if ("*".equals(patterns[i])) {
                    continue;
                }
                if (!patterns[i].equals(names[i + nameIndexDiff])) {
                    return false;
                }
            }
            return true;
        }
    }

    public static AddressHolder getAddressHolder(String address) {
        return getAddressHolder(address, -1);
    }

    public static AddressHolder getAddressHolder(final String address, int defaultPort) {
        final int indexBracketStart = address.indexOf('[');
        final int indexBracketEnd = address.indexOf(']', indexBracketStart);
        final int indexColon = address.indexOf(':');
        final int lastIndexColon = address.lastIndexOf(':');
        String host;
        int port = defaultPort;
        String scopeId = null;
        if (indexColon > -1 && lastIndexColon > indexColon) {
            // IPv6
            if (indexBracketStart == 0 && indexBracketEnd > indexBracketStart) {
                host = address.substring(indexBracketStart + 1, indexBracketEnd);
                if (lastIndexColon == indexBracketEnd + 1) {
                    port = Integer.parseInt(address.substring(lastIndexColon + 1));
                }
            } else {
                host = address;
            }
            final int indexPercent = host.indexOf('%');
            if (indexPercent != -1) {
                scopeId = host.substring(indexPercent + 1);
                host = host.substring(0, indexPercent);
            }
        } else if (indexColon > 0 && indexColon == lastIndexColon) {
            host = address.substring(0, indexColon);
            port = Integer.parseInt(address.substring(indexColon + 1));
        } else {
            host = address;
        }
        return new AddressHolder(host, port, scopeId);
    }

    public static boolean isIpAddress(String address) {
        try {
            getAddressMatcher(address);
            return true;
        } catch (InvalidAddressException e) {
            return false;
        }
    }

    public static InetAddress fixScopeIdAndGetInetAddress(final InetAddress inetAddress) throws SocketException {
        Inet6Address resultInetAddress = null;
        if (inetAddress instanceof Inet6Address &&
                (inetAddress.isLinkLocalAddress() || inetAddress.isSiteLocalAddress())) {
            final Inet6Address inet6Address = (Inet6Address) inetAddress;
            if (inet6Address.getScopeId() <= 0 && inet6Address.getScopedInterface() == null) {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface ni = interfaces.nextElement();
                    Enumeration<InetAddress> addresses = ni.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        if (address instanceof Inet6Address &&
                                Arrays.equals(address.getAddress(), inet6Address.getAddress())) {
                            if (resultInetAddress != null) {
                                throw new IllegalArgumentException("This address " + inet6Address +
                                        " is bound to more than one network interface!");
                            }
                            resultInetAddress = (Inet6Address) address;
                        }
                    }
                }
            }
        }
        return resultInetAddress == null ? inetAddress : resultInetAddress;
    }

    public static Inet6Address getInetAddressFor(final Inet6Address inetAddress, String scope)
            throws UnknownHostException, SocketException {
        if (inetAddress.isLinkLocalAddress() || inetAddress.isSiteLocalAddress()) {
            final char[] chars = scope.toCharArray();
            boolean numeric = true;
            for (char c : chars) {
                if (!Character.isDigit(c)) {
                    numeric = false;
                    break;
                }
            }
            if (numeric) {
                return Inet6Address.getByAddress(null, inetAddress.getAddress(), Integer.parseInt(scope));
            } else {
                return Inet6Address.getByAddress(null, inetAddress.getAddress(), NetworkInterface.getByName(scope));
            }
        }
        return inetAddress;
    }

    public static Collection<Inet6Address> getPossibleInetAddressesFor(final Inet6Address inet6Address) {
        if ((inet6Address.isSiteLocalAddress() || inet6Address.isLinkLocalAddress())
                && inet6Address.getScopeId() <= 0 && inet6Address.getScopedInterface() == null) {
            final LinkedList<Inet6Address> possibleAddresses = new LinkedList<Inet6Address>();
            try {
                final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface ni = interfaces.nextElement();
                    Enumeration<InetAddress> addresses = ni.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        if (address instanceof Inet4Address) {
                            continue;
                        }
                        if (inet6Address.isLinkLocalAddress() && address.isLinkLocalAddress()
                                || inet6Address.isSiteLocalAddress() && address.isSiteLocalAddress()) {
                            final Inet6Address newAddress = Inet6Address.getByAddress(null, inet6Address.getAddress(),
                                    ((Inet6Address) address).getScopeId());
                            possibleAddresses.addFirst(newAddress);
                        }
                    }
                }
            } catch (IOException ignored) {
            }
            if (possibleAddresses.isEmpty()) {
                throw new IllegalArgumentException("Could not find a proper network interface" +
                        " to connect to " + inet6Address);
            }
            return possibleAddresses;
        }
        return Collections.singleton(inet6Address);
    }

    public static Collection<String> getMatchingIpv4Addresses(final AddressMatcher addressMatcher) {
        if (addressMatcher.isIPv6()) {
            throw new IllegalArgumentException("Cannot wildcard matching for IPv6: " + addressMatcher);
        }
        final Collection<String> addresses = new HashSet<String>();
        final String first3 = addressMatcher.address[0] + "." +
                addressMatcher.address[1] + "." +
                addressMatcher.address[2]  ;
        final String lastPart = addressMatcher.address[3];
        final int dashPos ;
        if ("*".equals(lastPart)) {
            for (int j = 0; j <= NUMBER_OF_ADDRESSES; j++) {
                addresses.add(first3 + "." + j);
            }
        } else if ((dashPos = lastPart.indexOf('-')) > 0) {
            final int start = Integer.parseInt(lastPart.substring(0, dashPos));
            final int end = Integer.parseInt(lastPart.substring(dashPos + 1));
            for (int j = start; j <= end; j++) {
                addresses.add(first3 + "." + j);
            }
        } else {
            addresses.add(addressMatcher.getAddress());
        }
        return addresses;
    }

    /**
     * Gets an AddressMatcher for a given addresses.
     *
     * @param address the address
     * @return the returned AddressMatcher. The returned value will never be null.
     * @throws com.hazelcast.util.AddressUtil.InvalidAddressException if the address is not valid.
     */
    public static AddressMatcher getAddressMatcher(String address) {
        final AddressMatcher matcher;
        final int indexColon = address.indexOf(':');
        final int lastIndexColon = address.lastIndexOf(':');
        final int indexDot = address.indexOf('.');
        final int lastIndexDot = address.lastIndexOf('.');

        if (indexColon > -1 && lastIndexColon > indexColon) {
            if (indexDot == -1) {
                matcher = new Ip6AddressMatcher();
                parseIpv6(matcher, address);
            } else {
                // IPv4 mapped IPv6
                if (indexDot >= lastIndexDot) {
                    throw new InvalidAddressException(address);
                }
                final int lastIndexColon2 = address.lastIndexOf(':');
                final String host2 = address.substring(lastIndexColon2 + 1);
                matcher = new Ip4AddressMatcher();
                parseIpv4(matcher, host2);
            }
        } else if (indexDot > -1 && lastIndexDot > indexDot && indexColon == -1) {
            // IPv4
            matcher = new Ip4AddressMatcher();
            parseIpv4(matcher, address);
        } else {
            throw new InvalidAddressException(address);
        }
        return matcher;
    }

    private static void parseIpv4(AddressMatcher matcher, String address) {
        final String[] parts = address.split("\\.");
        if (parts.length != 4) {
            throw new InvalidAddressException(address);
        }
        for (String part : parts) {
            if (!isValidIpAddressPart(part, false)) {
                throw new InvalidAddressException(address);
            }
        }
        matcher.setAddress(parts);
    }

    private static boolean isValidIpAddressPart(String part, boolean ipv6) {
        if (part.length() == 1 && "*".equals(part)) {
            return true;
        }
        final int rangeIndex = part.indexOf('-');
        if (rangeIndex > -1 &&
                (rangeIndex != part.lastIndexOf('-') || rangeIndex == part.length() - 1)) {
            return false;
        }
        final String[] subParts;
        if (rangeIndex > -1) {
            subParts = part.split("\\-");
        } else {
            subParts = new String[]{part};
        }
        for (String subPart : subParts) {
            try {
                final int num;
                if (ipv6) {
                    num = Integer.parseInt(subPart, 16);
                    if (num > 65535) {
                        return false;
                    }
                } else {
                    num = Integer.parseInt(subPart);
                    if (num > 255) {
                        return false;
                    }
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    private static void parseIpv6(AddressMatcher matcher, String address) {
        if (address.indexOf('%') > -1) {
            String[] parts = address.split("\\%");
            address = parts[0];
        }
        final String[] parts = address.split("((?<=:)|(?=:))");
        final LinkedList<String> ipString = new LinkedList<String>();
        int count = 0;
        int mark = -1;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            String nextPart = i < parts.length - 1 ? parts[i + 1] : null;
            if ("".equals(part)) {
                continue;
            }
            if (":".equals(part) && ":".equals(nextPart)) {
                if (mark != -1) {
                    throw new InvalidAddressException(address);
                }
                mark = count;
            } else if (!":".equals(part)) {
                count++;
                ipString.add(part);
            }
        }
        if (mark > -1) {
            final int remaining = (8 - count);
            for (int i = 0; i < remaining; i++) {
                ipString.add((i + mark), "0");
            }
        }
        if (ipString.size() != 8) {
            throw new InvalidAddressException(address);
        }
        final String[] addressParts = ipString.toArray(new String[ipString.size()]);
        for (String part : addressParts) {
            if (!isValidIpAddressPart(part, true)) {
                throw new InvalidAddressException(address);
            }
        }
        matcher.setAddress(addressParts);
    }

    // ----------------- UTILITY CLASSES ------------------

    public static class AddressHolder {

        public final String address;
        public final String scopeId;
        public final int port;

        public AddressHolder(final String address, final int port,
                             final String scopeId) {
            this.address = address;
            this.scopeId = scopeId;
            this.port = port;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("AddressHolder ");
            sb.append('[').append(address).append("]:").append(port);
            return sb.toString();
        }
    }

    /**
     * http://docs.oracle.com/javase/1.5.0/docs/guide/net/ipv6_guide/index.html
     */

    public static abstract class AddressMatcher {

        protected final String[] address;

        protected AddressMatcher(final String[] address) {
            this.address = address;
        }

        public abstract boolean isIPv4();

        public abstract boolean isIPv6();

        public abstract void setAddress(String ip[]);

        protected final boolean match(final String[] mask, String[] input, int radix) {
            if (input != null && mask != null) {
                for (int i = 0; i < mask.length; i++) {
                    if (!doMatch(mask[i], input[i], radix)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        protected final boolean doMatch(final String mask, String input, int radix) {
            final int dashIndex = mask.indexOf('-');
            final int ipa = Integer.parseInt(input, radix);
            if ("*".equals(mask)) {
                return true;
            } else if (dashIndex != -1) {
                final int start = Integer.parseInt(mask.substring(0, dashIndex).trim(), radix);
                final int end = Integer.parseInt(mask.substring(dashIndex + 1).trim(), radix);
                if (ipa >= start && ipa <= end) {
                    return true;
                }
            } else {
                final int x = Integer.parseInt(mask, radix);
                if (x == ipa) {
                    return true;
                }
            }
            return false;
        }

        public abstract String getAddress();

        public abstract boolean match(AddressMatcher matcher);

        public boolean match(final String address) {
            try {
                return match(getAddressMatcher(address));
            } catch (Exception e) {
                return false;
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName());
            sb.append('{');
            sb.append(getAddress());
            sb.append('}');
            return sb.toString();
        }
    }

    static class Ip4AddressMatcher extends AddressMatcher {

        public Ip4AddressMatcher() {
            super(new String[4]);  // d.d.d.d
        }

        public boolean isIPv4() {
            return true;
        }

        public boolean isIPv6() {
            return false;
        }

        public void setAddress(String ip[]) {
            System.arraycopy(ip, 0, this.address, 0, ip.length);
        }

        public boolean match(final AddressMatcher matcher) {
            if (matcher.isIPv6()) return false;
            final String[] mask = this.address;
            final String[] input = ((Ip4AddressMatcher) matcher).address;
            return match(mask, input, 10);
        }

        public String getAddress() {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < address.length; i++) {
                sb.append(address[i]);
                if (i != address.length - 1) {
                    sb.append('.');
                }
            }
            return sb.toString();
        }
    }

    static class Ip6AddressMatcher extends AddressMatcher {

        public Ip6AddressMatcher() {
            super(new String[8]);  // x:x:x:x:x:x:x:x%s
        }

        public boolean isIPv4() {
            return false;
        }

        public boolean isIPv6() {
            return true;
        }

        public void setAddress(String ip[]) {
            System.arraycopy(ip, 0, this.address, 0, ip.length);
        }

        public boolean match(final AddressMatcher matcher) {
            if (matcher.isIPv4()) return false;
            final Ip6AddressMatcher a = (Ip6AddressMatcher) matcher;
            final String[] mask = this.address;
            final String[] input = a.address;
            return match(mask, input, 16);
        }

        public String getAddress() {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < address.length; i++) {
                sb.append(address[i]);
                if (i != address.length - 1) {
                    sb.append(':');
                }
            }
            return sb.toString();
        }
    }

    public static class InvalidAddressException extends IllegalArgumentException {

        public InvalidAddressException(final String s) {
            super("Illegal IP address format: " + s);
        }
    }
}
