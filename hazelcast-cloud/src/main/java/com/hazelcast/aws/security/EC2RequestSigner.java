package com.hazelcast.aws.security;

import com.hazelcast.aws.impl.Constants;
import com.hazelcast.aws.impl.DescribeInstances;
import com.hazelcast.aws.utility.AwsURLEncoder;
import com.hazelcast.config.AwsConfig;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by igmar on 03/11/14.
 */
public class EC2RequestSigner {
    private final static String API_SERVICE = "ec2";
    private final static String API_TERMINATOR = "aws4_request";
    private DescribeInstances request = null;
    private AwsConfig config;
    private String requestDate = null;
    private String signature = null;

    public EC2RequestSigner() {
        requestDate = getFormattedTimestamp();
    }

    public String sign(AwsConfig config, DescribeInstances request, String endpoint) {
        this.request = request;
        this.config = config;
        final String canonicalRequest = createCanonicalRequest(config, request, endpoint);
        final String stringToSign = createStringToSign(config, request, canonicalRequest);
        final byte[] signingKey = deriveSigningKey(request, config.getSecretKey());
        this.signature = createSignature(stringToSign, signingKey);

        return this.signature;
    }

    public String getAuthorization() {
        final String authorizaton = String.format("%s Credential=%s/%s SignedHeaders=%s Signature=%s", Constants.SIGNATURE_METHOD_V4, config.getAccessKey(), getCredentialScope(config, request), getSignedHeaders(config, request), this.signature);

        return authorizaton;
    }

    private String createSignature(String stringToSign, byte[] signingKey) {
        String signature = null;
        try {
            final Mac signMac = Mac.getInstance("HmacSHA256");
            final SecretKeySpec signKS = new SecretKeySpec(signingKey, "HmacSHA256");
            signMac.init(signKS);
            signature = bytesToHex(signMac.doFinal(stringToSign.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (InvalidKeyException e) {
            return null;
        }

        return signature;
    }

    private String createStringToSign(AwsConfig config, DescribeInstances request, String canonicalRequest) {
        String XAmzDateTime =  request.getAttributes().get("X-Amz-Date");

        StringBuilder sb = new StringBuilder();
        // Algorithhm
        sb.append(Constants.SIGNATURE_METHOD_V4).append("\n");
        // requestDate
        sb.append(XAmzDateTime).append("\n");
        // CredentialScope
        sb.append(getCredentialScope(config, request)).append("\n");
        // HashedCanonicalRequest
        sb.append(canonicalRequest).append("\n");

        return sb.toString();
    }

    private String getCredentialScope(AwsConfig config, DescribeInstances request) {
        String XAmzDateTime =  request.getAttributes().get("X-Amz-Date");
        String XAmsDate = XAmzDateTime.substring(0, 8);

        return String.format("%s/%s/%s/%s", XAmsDate, config.getRegion(), API_SERVICE, API_TERMINATOR);
    }

    private String getSignedHeaders(AwsConfig config, DescribeInstances request) {
        return "host";
    }

    private byte[] deriveSigningKey(final DescribeInstances request, final String signKey) {
        // This is derived from
        // http://docs.com.hazelcast.aws.security.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
        final String dateStamp = request.getAttributes().get("Datestamp");
        try {
            final String key = "AWS4" + signKey;
            final Mac mDate = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skDate = new SecretKeySpec(key.getBytes(), "HmacSHA256");
            mDate.init(skDate);
            final byte[] kDate = mDate.doFinal(dateStamp.getBytes());

            final Mac mRegion = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skRegion = new SecretKeySpec(kDate, "HmacSHA256");
            mRegion.init(skRegion);
            final byte[] kRegion = mRegion.doFinal(request.getAttributes().get("Region").getBytes());

            final Mac mService = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skService = new SecretKeySpec(kRegion, "HmacSHA256");
            mService.init(skService);
            final byte[] kService = mService.doFinal(request.getAttributes().get("Service").getBytes());

            final Mac mSigning = Mac.getInstance("HmacSHA256");
            final SecretKeySpec skSigning = new SecretKeySpec(kService, "HmacSHA256");
            mSigning.init(skSigning);
            final byte[] kSigning = mSigning.doFinal("aws4_request".getBytes());

            return kSigning;
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (InvalidKeyException e) {
            return null;
        }
    }

    private String createCanonicalRequest(AwsConfig config, DescribeInstances request, String endpoint) {
        StringBuilder sb = new StringBuilder();
        // HTTPRequestMethod
        sb.append("GET").append("\n");
        // CanonicalURI
        sb.append("/").append("\n");
        // CanonicalQueryString
        sb.append(getCanonicalizedQueryString(request)).append("\n");
        //CanonicalHeaders : Nasty one : We really don't know at this point
        sb.append("host:" + endpoint).append("\n");
        //SignedHeaders
        sb.append(getSignedHeaders(config, request)).append("\n");
        // Payload
        String payloadHash = "";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update("".getBytes("UTF-8"));
            byte[] digest = md.digest();
            payloadHash = bytesToHex(digest);
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }

        sb.append(payloadHash);

        return sb.toString();
    }

    public String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat(Constants.DATE_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(new Date());
    }

    private String bytesToHex(byte[] in) {
        final char[] hexArray = "0123456789abcdef".toCharArray();

        char[] hexChars = new char[in.length * 2];
        for ( int j = 0; j < in.length; j++ ) {
            int v = in[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    protected String getCanonicalizedQueryString(DescribeInstances request) {
        List<String> components = getListOfEntries(request.getAttributes());
        Collections.sort(components);
        return getCanonicalizedQueryString(components);
    }

    protected void addComponents(List<String> components, Map<String, String> attributes, String key) {
        components.add(AwsURLEncoder.urlEncode(key) + "=" + AwsURLEncoder.urlEncode(attributes.get(key)));
    }

    protected List<String> getListOfEntries(Map<String, String> entries) {
        List<String> components = new ArrayList<String>();
        for (String key : entries.keySet()) {
            addComponents(components, entries, key);
        }
        return components;
    }

    /**
     * @param list
     * @return
     */
    protected String getCanonicalizedQueryString(List<String> list) {
        Iterator<String> it = list.iterator();
        StringBuilder result = new StringBuilder(it.next());
        while (it.hasNext()) {
            result.append("&").append(it.next());
        }
        return result.toString();
    }
}
