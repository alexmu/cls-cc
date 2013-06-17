/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.nosqlcluster;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author wanghh19880807
 */
public class NoSqlClusterTableTruncateHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<response><message>MESSAGE<message></response>";
    private static final String FAIL_RESPONSE = "<error><message>MESSAGE<message></error>";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterTableTruncateHandler.class.getName());
    }

    public String execute(String pRequestContent) {
        String result = "";//返回的结果

        String databaseName = null;
        String tableName = null;
        Connection conn = null;
        try {
            Document databaseDropDoc = DocumentHelper.parseText(pRequestContent);
            Element requestParamsElt = databaseDropDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();
            Element tableNameElt = requestParamsElt.element("name");
            tableName = tableNameElt == null ? "" : tableNameElt.getStringValue();

            if (databaseName.isEmpty()) {
                result = FAIL_RESPONSE.replace("MESSAGE", databaseName + " is not defined");
            } else if (tableName.isEmpty()) {
                result = FAIL_RESPONSE.replace("MESSAGE", tableName + " is not defined");
            } else {
                //connect to hive
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                String connectStr = "jdbc:hive://192.168.120.46:10000/" + databaseName;
                logger.debug(connectStr);
                conn = DriverManager.getConnection(connectStr, "", "");
                Statement stmt = conn.createStatement();
                
                //then create table                
                String sql = "USE " + databaseName;
                logger.info(sql);
                stmt.executeQuery(sql);
                
                String tmpTableName = tableName + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
                sql = "CREATE TABLE " + tmpTableName + " LIKE " + tableName;
                logger.info(sql);
                stmt.executeQuery(sql);

                sql = "DROP TABLE " + tableName;
                logger.info(sql);
                stmt.executeQuery(sql);

                sql = "ALTER TABLE " + tmpTableName + " RENAME TO " + tableName;
                logger.info(sql);
                stmt.executeQuery(sql);

                result = SUCCESS_RESPONSE.replace("MESSAGE", "truncate table " + databaseName + "." + tableName + " successfully");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            result = FAIL_RESPONSE.replace("MESSAGE", "truncate table " + databaseName + "." + tableName + " unsuccessfully for " + ex.getMessage());
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ex1) {
                        ex1.printStackTrace();
                    }
                }
            }
        }
        return result;
    }

    public static void main(String[] args) {
        File inputXml = new File("truncate-table-specific.xml");
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
            NoSqlClusterTableTruncateHandler handler = new NoSqlClusterTableTruncateHandler();
            System.out.println(handler.execute(xmlStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
