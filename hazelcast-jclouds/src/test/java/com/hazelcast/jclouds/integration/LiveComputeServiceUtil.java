package com.hazelcast.jclouds.integration;

import com.google.common.base.Predicates;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.predicates.NodePredicates;

import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static org.jclouds.compute.options.TemplateOptions.Builder.userMetadata;
import static org.jclouds.compute.predicates.NodePredicates.inGroup;


public class LiveComputeServiceUtil {

    private  ComputeService computeService;
    private  List<NodeMetadata> nodes = new ArrayList<NodeMetadata>();
    private  String provider;
    private  String identity;
    private  String credential;

    private static final ILogger LOGGER = Logger.getLogger("com.hazelcast.jclouds");


    public static final String GROUP_NAME1 = "livetest-group1";
    public static final String GROUP_NAME2 = "livetest-group2";

    public static final String AWS_REGION1 = "us-east-1";
    public static final String AWS_REGION2 = "us-west-2";

    public static final Map.Entry<String, String> tag1 =
            new AbstractMap.SimpleImmutableEntry<String, String>("Owner", "DbAdmin");
    public static final Map.Entry<String, String> tag2 =
            new AbstractMap.SimpleImmutableEntry<String, String>("Stack", "Test");
    public static final Map.Entry<String, String> tag3 =
            new AbstractMap.SimpleImmutableEntry<String, String>("Type", "Micro");

    public LiveComputeServiceUtil(String provider, String identity, String credential) {
        this.provider = provider;
        this.identity = identity;
        this.credential = credential;
        init();
    }

    public void init() {
        if (computeService == null) {
            checkNotNull(provider);
            checkNotNull(identity);
            checkNotNull(credential);
            computeService = initComputeService(provider, identity, credential);
        }
    }

    public void provisionNodes() {
        Map<String, String> singleTagConfig = new HashMap<String, String>();
        singleTagConfig.put(tag1.getKey(), tag1.getValue());

        Map<String, String> multiTagConfig  = new HashMap<String, String>();
        multiTagConfig.put(tag2.getKey(), tag2.getValue());
        multiTagConfig.put(tag3.getKey(), tag3.getValue());

        Template region1Template =
                getTemplateInRegion1(singleTagConfig);

        Template region2Template =
                getTemplateInRegion2(multiTagConfig);

        try {
            createNode(GROUP_NAME1, region1Template);
            createNode(GROUP_NAME1, region1Template);
            createNode(GROUP_NAME1, region2Template);
            createNode(GROUP_NAME2, region2Template);
            createNode(GROUP_NAME2, region1Template);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ComputeService initComputeService(String provider, String identity, String credential) {
        Properties properties = new Properties();
        ContextBuilder builder = ContextBuilder.newBuilder(provider)
                .credentials(identity, credential)
                .overrides(properties);
        LOGGER.info(MessageFormat.format(">> initializing Compute Service {0}", builder.getApiMetadata()));
        return builder.buildView(ComputeServiceContext.class).getComputeService();
    }


    public  Template getTemplateInRegion1(Map<String, String> tags) {
        TemplateBuilder templateBuilder = computeService.templateBuilder();
        templateBuilder.locationId(AWS_REGION1);
        templateBuilder.smallest();
        templateBuilder.options(userMetadata(tags));
        return templateBuilder.build();
    }

    public  Template getTemplateInRegion2(Map<String, String> tags) {
        TemplateBuilder templateBuilder = computeService.templateBuilder();
        templateBuilder.locationId(AWS_REGION2);
        templateBuilder.smallest();
        templateBuilder.options(userMetadata(tags));
        return templateBuilder.build();
    }

    public NodeMetadata createNode(String groupName, Template template) throws Exception{
        NodeMetadata node = getOnlyElement(computeService.createNodesInGroup(groupName, 1, template));
        LOGGER.info(MessageFormat.format("<< node created {0}: {1}", node.getId(),
                concat(node.getPrivateAddresses(), node.getPublicAddresses())));
        nodes.add(node);
        return node;
    }

    public void destroyNodes() {
        LOGGER.info(MessageFormat.format(">> destroying nodes in group {0}", GROUP_NAME1));
        Set<? extends NodeMetadata> destroyed1 = computeService.destroyNodesMatching(
                Predicates.<NodeMetadata> and(not(NodePredicates.TERMINATED), inGroup(GROUP_NAME1)));
        LOGGER.info(MessageFormat.format("<< destroyed nodes {0}", destroyed1));

        LOGGER.info(MessageFormat.format(">> destroying nodes in group {0}", GROUP_NAME2));
        Set<? extends NodeMetadata> destroyed2 = computeService.destroyNodesMatching(
                Predicates.<NodeMetadata> and(not(NodePredicates.TERMINATED), inGroup(GROUP_NAME2)));
        LOGGER.info(MessageFormat.format("<< destroyed nodes {0}", destroyed2));
    }
}
