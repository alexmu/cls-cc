/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.mastercc;

import cn.ac.iie.cls.cc.server.CLSCCServer;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 *
 * @author mwm
 */
public class CCHandler extends AbstractHandler {

    private static CCHandler ccHandler = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CCHandler.class.getName());
    }

    public static CCHandler getCCHandler() {
        if (ccHandler != null) {
            return ccHandler;
        }
        ccHandler = new CCHandler();
        return ccHandler;
    }

    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
        System.out.println("get request:" + request.getMethod());
        Request baseRequest = (request instanceof Request) ? (Request) request : HttpConnection.getCurrentConnection().getRequest();
        baseRequest.setHandled(true);

        String url = baseRequest.getPathInfo();
        System.out.println(url);
        String[] urlItems = url.split("/");
        if (urlItems.length < 2) {
            logger.warn("bad request for nothing");
        } else {
            String slaveCCName = urlItems[1].toLowerCase();
            if (slaveCCName.equals("dataetl")) {
                logger.info("salvecc dataetl found");
                if(urlItems.length<3){
                    logger.warn("bad request for dataetl");
                    return;
                }
                String operate = urlItems[2].toLowerCase();
                if (operate.equals("execute")) {
                    logger.info("execute");
                } else if (operate.equals("checkstatus")) {
                    logger.info("checkstatus");
                }
            } else {
                logger.warn("no slave cc found for " + urlItems[0]);
            }
        }

//        response.setContentType("text/html;charset=utf-8");
//        response.setStatus(HttpServletResponse.SC_OK);
//        baseRequest.setHandled(true);
//        response.getWriter().println("<h1>ok</h1>");
    }
}
