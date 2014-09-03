package com.hazelcast.wm.test;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

public class JettyServer implements ServletContainer {

    Server server;

    public JettyServer(int port, String sourceDir, String serverXml) throws Exception {
        buildJetty(port, sourceDir, serverXml);
    }

    @Override
    public void stop() throws Exception {
        server.stop();
    }

    @Override
    public void start() throws Exception {
        server.start();
    }

    @Override
    public void restart() throws Exception {
        server.stop();
        server.start();
    }

    public void buildJetty(int port, String sourceDir, String webXmlFile) throws Exception {
        server = new Server();

        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(port);
        server.addConnector(connector);
        WebAppContext context = new WebAppContext();
        context.setResourceBase(sourceDir);
        context.setDescriptor(sourceDir + "/WEB-INF/" + webXmlFile);
        context.setLogUrlOnStart(true);
        context.setContextPath("/");
        context.setParentLoaderPriority(true);

        server.setHandler(context);

        server.start();
    }

    @Override
    public boolean isRunning() {
        if (server == null) {
            return false;
        } else {
            return server.isRunning();
        }
    }
}
