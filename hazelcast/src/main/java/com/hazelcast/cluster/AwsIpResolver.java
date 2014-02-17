package com.hazelcast.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: sancar
 * Date: 12/02/14
 * Time: 16:39
 */
public class AwsIpResolver {

    final private Map<String, String> publicToPrivateIp = new HashMap<String, String>();
    final private Map<String, String> privateToPublicIp = new HashMap<String, String>();


    public AwsIpResolver(List<PublicPrivatePair> pairs) {
        for (PublicPrivatePair pair : pairs) {
            final String privateIp = pair.getPrivateIp();
            final String publicIp = pair.getPublicIp();
            publicToPrivateIp.put(publicIp, privateIp);
            privateToPublicIp.put(privateIp, publicIp);
        }
    }

    public String convertToPublic(String privateIp) {
        return privateToPublicIp.get(privateIp);
    }

    public String convertToPrivate(String publicIp) {
        return publicToPrivateIp.get(publicIp);
    }

    public static class PublicPrivatePair {
        final private String publicIp;

        final private String privateIp;

        public PublicPrivatePair(String publicIp, String privateIp) {
            this.privateIp = privateIp;
            this.publicIp = publicIp;
        }

        public String getPublicIp() {
            return publicIp;
        }

        public String getPrivateIp() {
            return privateIp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PublicPrivatePair that = (PublicPrivatePair) o;

            if (privateIp != null ? !privateIp.equals(that.privateIp) : that.privateIp != null) return false;
            if (publicIp != null ? !publicIp.equals(that.publicIp) : that.publicIp != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = publicIp != null ? publicIp.hashCode() : 0;
            result = 31 * result + (privateIp != null ? privateIp.hashCode() : 0);
            return result;
        }
    }

}
