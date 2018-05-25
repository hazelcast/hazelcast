package com.hazelcast.gcp;
/*
 * PRE-REQUISITES:
 * ---------------
 * 1. If not already done, enable the Compute Engine API
 *    and check the quota for your project at
 *    https://console.developers.google.com/apis/api/compute
 * 2. To install the client library on Maven or Gradle, check installation instructions at
 *    https://github.com/google/google-api-java-client.
 *    On other build systems, you can add the jar files to your project from
 *    https://developers.google.com/resources/api-libraries/download/compute/v1/java
 * 3. This sample uses Application Default Credentials for Auth.
 *    If not already done, install the gcloud CLI from
 *    https://cloud.google.com/sdk/ and run 'gcloud auth application-default login'
 */

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.NetworkInterface;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public class InstanceQuery {
  public static void main(String[] args) throws IOException, GeneralSecurityException {
    // passing in arguments for the projectid and zone
    // Project ID for this request.
    String project = "hazelcast-33";
    if(args[0]!=null)
      project = args[0];
    // The name of the zone for this request.
    String zone = "us-east1-b";
    if(args[1]!=null)
      zone = args[1];

    // Authentication is provided by the 'gcloud' tool when running locally
    // and by built-in service accounts when running on GAE, GCE, or GKE.
    GoogleCredential credential = GoogleCredential.getApplicationDefault();

    // The createScopedRequired method returns true when running on GAE or a local developer
    // machine. In that case, the desired scopes must be passed in manually. When the code is
    // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
    // For more information, see
    // https://developers.google.com/identity/protocols/application-default-credentials
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    Compute computeService = new Compute.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName("Google Cloud Platform Sample")
            .build();

    Compute.Instances.List request = computeService.instances().list(project, zone);
    InstanceList response;
            List<String> ipAddresses = new ArrayList();
        do {
            response = request.execute();
            if (response.getItems() == null) continue;
            for (Instance instance : response.getItems()) {
                for (NetworkInterface networkInterface : instance.getNetworkInterfaces()) {
                    ipAddresses.add(networkInterface.getNetworkIP());
                }
                }
                request.setPageToken(response.getNextPageToken());
            } while (response.getNextPageToken() != null);

        for (String ipAddress : ipAddresses){
            System.out.println(ipAddress);
        }
        }
    }
