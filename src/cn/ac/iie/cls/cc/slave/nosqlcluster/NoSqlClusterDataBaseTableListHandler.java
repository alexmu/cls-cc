/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.nosqlcluster;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author wanghh19880807
 *
 */
//获取某个数据库的列表
public class NoSqlClusterDataBaseTableListHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<response><message>MESSAGE<message></response>";
    private static final String FAIL_RESPONSE = "<error><message>MESSAGE<message></error>";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterDataBaseTableListHandler.class.getName());
    }

    public String execute(String pRequestContent) {


        if (pRequestContent == null || pRequestContent.isEmpty()) {
            return FAIL_RESPONSE.replace("MESSAGE", "get table list unsuccessfully for table list geting configuration is empty");
        }

        String databaseName = null;
        Connection conn = null;
        ResultSet rs = null;
        String tableNameList = "";
        try {
            Document databaseDropDoc = DocumentHelper.parseText(pRequestContent);
            Element requestParamsElt = databaseDropDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();

            if (databaseName.isEmpty()) {
                return FAIL_RESPONSE.replace("MESSAGE", databaseName + " is not defined");
            } else {
                //connect to hive
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
                Statement stmt = conn.createStatement();
                //parse xml
                //then create table
                String sql = "SHOW TABLES FROM " + databaseName;
                logger.info(sql);
                rs = stmt.executeQuery(sql);

                while (rs.next()) {
                    String tableName = rs.getString(1);
                    tableNameList += "<table>" + tableName + "</table>";
                }
                //将Message替换成为相应的信息


                return SUCCESS_RESPONSE.replace("MESSAGE", "<tables>" + tableNameList + "</tables>");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return FAIL_RESPONSE.replace("MESSAGE", "get table list of database " + databaseName + " unsuccessfully for " + ex.getMessage());
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException ex1) {
                        ex1.printStackTrace();
                    }
                }

                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex1) {
                        ex1.printStackTrace();
                    }
                }

            }
        }        
    }

    public static void main(String[] args) {
        File inputXml = new File("tablelist-database-specific.xml");
        try {
            String xmlStr = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                xmlStr += line;
            }
            System.out.println(xmlStr);
            System.out.println("OK");
            NoSqlClusterDataBaseTableListHandler handler = new NoSqlClusterDataBaseTableListHandler();
            System.out.println(handler.execute(xmlStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
