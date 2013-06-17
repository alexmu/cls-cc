/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.server;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.config.Configuration;
import cn.ac.iie.cls.cc.master.CCHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.*;
import org.mortbay.jetty.nio.SelectChannelConnector;

/**
 *
 * @author alexmu
 */
public class CLSCCServer {

    static Server server = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CLSCCServer.class.getName());
    }

    public static void showUsage() {
        System.out.println("Usage:java -jar ");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            init();
            startup();
        } catch (Exception ex) {
            logger.error("starting cls ccer server is failed for " + ex.getMessage(), ex);
        }
        System.exit(0);
    }

    private static void startup() throws Exception {
        logger.info("starting cls cc server...");
        server.start();
        logger.info("start cls cc server successfully");
        server.join();
    }

    private static void init() throws Exception {
        String configurationFileName = "cls-cc.properties";
        logger.info("initializing cls cc server...");
        logger.info("getting configuration from configuration file " + configurationFileName);
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        logger.info("initializng runtime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment is failed");
        }
        logger.info("initialize runtime enviroment successfully");

        String serverIP = conf.getString("jettyServerIP", "");
        if (serverIP.isEmpty()) {
            throw new Exception("definition jettyServerIP is not found in " + configurationFileName);
        }

        int serverPort = conf.getInt("jettyServerPort", -1);
        if (serverPort == -1) {
            throw new Exception("definition jettyServerPort is not found in " + configurationFileName);
        }

        Connector connector = new SelectChannelConnector();
        connector.setHost(serverIP);
        connector.setPort(serverPort);

        server = new Server();
        
        //add by zy
//        int port=8082;
//        Server server = new Server(port);
//        
//        ResourceHandler resource_handler=new ResourceHandler();
//        resource_handler.setDirectoriesListed(true);
//       // resource_handler.setWelcomeFiles(new String[]{"index.html"});
//        resource_handler.setResourceBase("D:/123");
//        //resource_handler.setResourceBase(args.length==2?args[1]:".");
//        resource_handler.setStylesheet("");
//      
//        HandlerList handlers = new HandlerList();
//        handlers.setHandlers(new Handler[]{resource_handler,new DefaultHandler()});
//        
//        server.setHandler(handlers);
//        
//        server.start();
//        server.join();
        
        server.setConnectors(new Connector[]{connector});

        ContextHandler ccContext = new ContextHandler("/resources");
        CCHandler ccHandler = CCHandler.getCCHandler();
        if (ccHandler == null) {
            throw new Exception("initializing dataDispatchHandler is failed");
        }

        ccContext.setHandler(ccHandler);

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{ccContext});
        server.setHandler(contexts);
        logger.info("intialize cls cc server successfully");
    }
}
