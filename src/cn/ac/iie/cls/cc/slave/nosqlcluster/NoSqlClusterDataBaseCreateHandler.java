/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.nosqlcluster;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.dataetl.ETLJob;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class NoSqlClusterDataBaseCreateHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<response><message>MESSAGE<message></response>";
    private static final String FAIL_RESPONSE = "<error><message>MESSAGE<message></error>";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterDataBaseCreateHandler.class.getName());
    }

    public String execute(String pRequestContent) {
        String result = "";

        String databaseName = null;
        Connection conn = null;
        try {
            Document databaseCreateDoc = DocumentHelper.parseText(pRequestContent);
            Element requestParamsElt = databaseCreateDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();
            Element commentElt = requestParamsElt.element("comment");
            String comment = commentElt == null ? "" : commentElt.getStringValue();

            if (databaseName.isEmpty()) {
                result = FAIL_RESPONSE.replace("MESSAGE", "databaseName is not defined");
            } else {
                //connect to hive
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection("jdbc:hive://192.168.120.46:10000/default", "", "");
                Statement stmt = conn.createStatement();
                //parse xml
                //then create table
                String sql = "CREATE DATABASE IF NOT EXISTS " + databaseName +(comment.isEmpty() ? "" : " COMMENT '" + comment + "'");
                logger.info(sql);
                stmt.executeQuery(sql);
                result = SUCCESS_RESPONSE.replace("MESSAGE", "create database " + databaseName + " successfully");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            result = FAIL_RESPONSE.replace("MESSAGE", "create database " + databaseName + " unsuccessfully for " + ex.getMessage());
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
            }
        }
        return result;
    }

    public static void main(String[] args) {
        File inputXml = new File("create-database-specific.xml");
        try {
            String xmlStr = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                xmlStr += line;
            }
            System.out.println(xmlStr);
            System.out.println("parse ok");
            NoSqlClusterDataBaseCreateHandler handler = new NoSqlClusterDataBaseCreateHandler();
            System.out.println(handler.execute(xmlStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
