package com.hazelcast.azure;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import javax.naming.ServiceUnavailableException;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.naming.ServiceUnavailableException;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import java.lang.InterruptedException;

/**
 * Authentication helper for Azure Active Directory
 */
public class AzureAuthHelper {
    /**
     * Use the ResourceManagementService factory helper method to create a client based on the management config.
     * 
     * @param properties The properties Map provided by Hazelcast
     * @return ResourceManagementClient a client to be used to make authenticated requests to the ARM REST API
     * @throws Exception all of the exceptions
     */
    public static Configuration getAzureConfiguration(Map<String, Comparable> properties) throws Exception {
        Configuration config = createConfiguration(
            (String)AzureProperties.getOrNull(AzureProperties.SUBSCRIPTION_ID, properties),
            (String)AzureProperties.getOrNull(AzureProperties.CLIENT_ID, properties),
            (String)AzureProperties.getOrNull(AzureProperties.TENANT_ID, properties),
            (String)AzureProperties.getOrNull(AzureProperties.CLIENT_SECRET, properties));
        return config;
    }

    /**
     * Create configuration builds the management configuration needed for creating the ResourceManagementService.
     * The config contains the baseURI which is the base of the ARM REST service, the subscription id as the context for
     * the ResourceManagementService and the AAD token required for the HTTP Authorization header.
     * 
     * @return Configuration the generated configuration
     * @throws URISyntaxException if the URI used to authenticate is invalid
     * @throws IOException if we cannot read the response
     * @throws ServiceUnavailableException when the azure auth service is unavailable
     * @throws InterruptedException if task is interrupted
     * @throws ExecutionException
     */
    private static Configuration createConfiguration(String subscriptionId, 
        String clientId, String tenantId, String clientSecret) throws 
        URISyntaxException, IOException, ServiceUnavailableException, 
        ExecutionException, InterruptedException {
        String baseUri = "https://management.core.windows.net";
        return ManagementConfiguration.configure(
                null,
                new URI(baseUri),
                subscriptionId,
                getAccessTokenFromServicePrincipalCredentials(clientId, 
                    tenantId, clientSecret).getAccessToken());
    }

    /**
     * Get access token from service principal credentials calls ADAL4J to get a Bearer Auth token to use for the ARM
     * REST API.
     *
     * @return AuthenticationResult the result of the request to Azure Active Directory via ADAL4J
     * @throws ServiceUnavailableException something broke when making a call to Azure Active Directory
     * @throws MalformedURLException       the url provided to AAD was not properly formed
     * @throws ExecutionException          houston we have a problem.
     * @throws InterruptedException        the request to AAD has been interrupted
     */
    private static AuthenticationResult getAccessTokenFromServicePrincipalCredentials(String clientId, 
        String tenantId, String clientSecret) throws
            ServiceUnavailableException, MalformedURLException, ExecutionException, InterruptedException {
        AuthenticationContext context;
        AuthenticationResult result = null;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            // TODO: add your tenant id
            context = new AuthenticationContext("https://login.windows.net/" + tenantId,
                    false, service);
            // TODO: add your client id and client secret
            ClientCredential cred = new ClientCredential(clientId,
                    clientSecret);
            Future<AuthenticationResult> future = context.acquireToken(
                    "https://management.azure.com/", cred, null);
            result = future.get();
        } finally {
            service.shutdown();
        }

        if (result == null) {
            throw new ServiceUnavailableException(
                    "authentication result was null");
        }
        return result;
    }
}