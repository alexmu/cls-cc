/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.linkagecmd;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.clsagent.CLSAgentDataCollectHandler;
import cn.ac.iie.cls.cc.util.HttpResponseParser;
import java.io.ByteArrayInputStream;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class ConfigExecuteHandler implements SlaveHandler {

    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(ConfigExecuteHandler.class.getName());
    }

    public String execute(String pRequestContent) {
        String result = "";
        String host = "http://10.128.125.74";//parse xml
        int port = 7080;

        try {
            HttpClient httpClient = new DefaultHttpClient();
            HttpPost httppost = new HttpPost(host + ":" + port + "/resources/clsagent/execmd");

            InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(pRequestContent.getBytes()), -1);
            reqEntity.setContentType("binary/octet-stream");
            reqEntity.setChunked(true);
            httppost.setEntity(reqEntity);

            HttpResponse response = httpClient.execute(httppost);
            result = HttpResponseParser.getResult(response);
            System.out.println(result);
            httppost.releaseConnection();

        } catch (Exception ex) {
            logger.warn(ex);
            result = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                    + "<error>"
                    + "<message>client start err!</message>"
                    + "</error>";
        } finally {
        }

        return result;
    }
}
