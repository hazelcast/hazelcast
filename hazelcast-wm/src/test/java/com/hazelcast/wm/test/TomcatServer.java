package com.hazelcast.wm.test;

import org.apache.catalina.Context;
import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.ContextConfig;
import org.apache.catalina.startup.Tomcat;

import java.io.File;

public class TomcatServer implements ServletContainer {
    Tomcat tomcat;
    String serverXml;
    String sourceDir;

    public TomcatServer(int port, String sourceDir, String serverXml) throws Exception {
        this.serverXml = serverXml;
        this.sourceDir = sourceDir;
        buildTomcat(port,sourceDir,serverXml);
    }

    @Override
    public void stop() throws LifecycleException {
        tomcat.stop();
    }

    @Override
    public void start() throws Exception {
        tomcat.start();
    }

    @Override
    public void restart() throws LifecycleException, InterruptedException {
        int port = tomcat.getConnector().getLocalPort();
        tomcat.stop();
        tomcat.destroy();
        Thread.sleep(5000);
        buildTomcat(port, sourceDir, serverXml);
        Thread.sleep(5000);
    }

    public void buildTomcat(int port, String sourceDir, String serverXml) throws LifecycleException {
        tomcat = new Tomcat();
        File baseDir = new File(System.getProperty("java.io.tmpdir"));
        tomcat.setPort(port);
        tomcat.setBaseDir(baseDir.getAbsolutePath());
        Context context = tomcat.addContext("/", sourceDir);
        context.getServletContext().setAttribute(Globals.ALT_DD_ATTR, sourceDir + "/WEB-INF/" + serverXml);
        ContextConfig contextConfig = new ContextConfig();
        context.addLifecycleListener(contextConfig);
        context.setCookies(true);
        context.setBackgroundProcessorDelay(1);
        context.setReloadable(true);
        tomcat.getEngine().setJvmRoute("tomcat" + port);
        tomcat.getEngine().setName("tomcat-test" + port);
        tomcat.start();

    }
}
