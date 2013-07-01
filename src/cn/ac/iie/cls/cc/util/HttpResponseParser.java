/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.util;

import cn.ac.iie.cls.cc.slave.SlaveHandlerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author zhangyun
 */
public class HttpResponseParser {

    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(HttpResponseParser.class.getName());
    }

    public static String getResult(HttpResponse response) {
        String result = "";
        InputStream reponseStream = null;
        try {
            reponseStream = response.getEntity().getContent();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] bytesContent = new byte[1024];
            int bytesReadIn = 0;

            while ((bytesReadIn = reponseStream.read(bytesContent, 0, 1024)) > 0) {
                out.write(bytesContent, 0, bytesReadIn);
            }
            out.close();
            byte[] rep = out.toByteArray();

            String responseStr = new String(rep,"UTF-8");
            logger.debug("HttpResponseParser getResult " + new String(rep));
            if (responseStr.toString().contains("1")) {
                result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                        + "<response>"
                        + "<status>client has start success!</status>"
                        + "</response>";

            } else if (responseStr.toString().contains("0")) {
                result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                        + "<error>"
                        + "<message>client start err!</message>"
                        + "</error>";
            } else {
                result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                        + "<error>"
                        + "<message>client start err!</message>"
                        + "</error>";
            }

        } catch (Exception ex) {
            logger.debug("HttpResponseParser getResult err!" + ex);
            result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                        + "<error>"
                        + "<message>client start err!</message>"
                        + "</error>";
        }

        return result;
    }
}
