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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class NoSqlClusterTableColumnAlterHandler implements SlaveHandler {

    private static final String SUCCESS_RESPONSE = "<response><message>MESSAGE<message></response>";
    private static final String FAIL_RESPONSE = "<error><message>MESSAGE<message></error>";
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterTableInfoHandler.class.getName());
    }

    public String execute(String pRequestContent) {

        if (pRequestContent == null || pRequestContent.isEmpty()) {
            return FAIL_RESPONSE.replace("MESSAGE", "get table information unsuccessfully for table information getting configuration is empty");
        }

        String databaseName = null;
        String tableName = null;
        Connection conn = null;
        ResultSet rs = null;
        try {
            Document databaseDropDoc = DocumentHelper.parseText(pRequestContent);
            Element requestParamsElt = databaseDropDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();
            Element tableNameElt = requestParamsElt.element("tableName");
            tableName = tableNameElt == null ? "" : tableNameElt.getStringValue();

            if (databaseName.isEmpty()) {
                return FAIL_RESPONSE.replace("MESSAGE", "databaseName  is not defined");
            } else if (tableName.isEmpty()) {
                return FAIL_RESPONSE.replace("MESSAGE", "tableName is not defined");
            } else {
                //connect to hive
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
                Statement stmt = conn.createStatement();
                //parse xml
                //then create table
                String sql = "USE " + databaseName;
                logger.info(sql);
                stmt.executeQuery(sql);

                sql = "Describe " + tableName;
                logger.info(sql);
                rs = stmt.executeQuery(sql);

                Map<String, String> tableColumnSet = new HashMap<String, String>();
                while (rs.next()) {
                    tableColumnSet.put(rs.getString(1), rs.getString(2));
                }
                
                //add columns
                Iterator addColumnItor = requestParamsElt.element("columnAlter").element("add") == null ? null : requestParamsElt.element("columnAlter").element("add").element("columns").elements("column").iterator();
                if (addColumnItor != null) {
                    sql = "";
                    while (addColumnItor.hasNext()) {
                        Element addColumnElt = (Element) addColumnItor.next();
                        if (sql.isEmpty()) {
                            sql = "ALTER TABLE " + tableName + " ADD COLUMNS(";
                            sql += addColumnElt.elementText("name") + " " + getColumnType(addColumnElt.elementText("type"));
                        } else {
                            sql += "," + addColumnElt.elementText("name") + " " + getColumnType(addColumnElt.elementText("type"));
                        }
                    }
                    if (!sql.isEmpty()) {
                        sql += ")";
                        logger.info(sql);
                        stmt.executeQuery(sql);
                    }
                }

                //rename columns
                Iterator renameColumnItor = requestParamsElt.element("columnAlter").element("rename") == null ? null : requestParamsElt.element("columnAlter").element("rename").element("columns").elements("column").iterator();
                if (renameColumnItor != null) {
                    sql = "";
                    while (renameColumnItor.hasNext()) {
                        Element renameColumnElt = (Element) renameColumnItor.next();
                        sql = "ALTER TABLE " + tableName + " CHANGE " + renameColumnElt.elementText("oldName") + " " + renameColumnElt.elementText("newName") + " " + tableColumnSet.get(renameColumnElt.elementText("oldName").toLowerCase());
                        logger.info(sql);
                        stmt.executeQuery(sql);
                    }
                }

                //alter columns
                Iterator alterColumnTypeItor = requestParamsElt.element("columnAlter").element("alterType") == null ? null : requestParamsElt.element("columnAlter").element("alterType").element("columns").elements("column").iterator();
                if (alterColumnTypeItor != null) {
                    sql = "";
                    while (alterColumnTypeItor.hasNext()) {
                        Element alterColumnTypeElt = (Element) alterColumnTypeItor.next();
                        sql = "ALTER TABLE " + tableName + " CHANGE " + alterColumnTypeElt.elementText("name") + " " + alterColumnTypeElt.elementText("name") + " " + alterColumnTypeElt.elementText("newType");
                        logger.info(sql);
                        stmt.executeQuery(sql);
                    }
                }

                //drop columns
                Iterator dropColumnItor = requestParamsElt.element("columnAlter").element("drop") == null ? null : requestParamsElt.element("columnAlter").element("drop").element("columns").elements("column").iterator();
                if (dropColumnItor != null) {
                    sql = "";
                    while (dropColumnItor.hasNext()) {
                        Element dropColumnElt = (Element) dropColumnItor.next();
                        sql = "ALTER TABLE " + tableName + " DROP " + dropColumnElt.elementText("name");
                        logger.info(sql);
                        stmt.executeQuery(sql);
                    }
                }
                return SUCCESS_RESPONSE.replace("MESSAGE", "alter column of table " + databaseName + "." + tableName + " successfully");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return FAIL_RESPONSE.replace("MESSAGE", "alter column of table " + databaseName + "." + tableName + " unsuccessfully for " + ex.getMessage());
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
    }

    public String getColumnType(String oldType) {

        if ("CHAR".equalsIgnoreCase(oldType) || "clob".equalsIgnoreCase(oldType)) {
            return "STRING";
        } else if (oldType.toUpperCase().startsWith("VARCHAR2")) {
            return "STRING";
        } else if ("LONG".equalsIgnoreCase(oldType)) {
            return "BIGINT";
        } else if ("NUMBER".equalsIgnoreCase(oldType)) {
            return "DOUBLE";
        } else if ("INTEGER".equalsIgnoreCase(oldType)) {
            return "INT";
        } else if ("FLOAT".equalsIgnoreCase(oldType)) {
            return "FLOAT";
        } else if ("DATE".equalsIgnoreCase(oldType)) {
            return "TIMESTAMP";
        } else if ("BLOB".equalsIgnoreCase(oldType)) {
            return "BINARY";
        } else {
            return oldType;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("table-cloumn-alter-spec.xml");
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
            NoSqlClusterTableColumnAlterHandler handler = new NoSqlClusterTableColumnAlterHandler();
            System.out.println(handler.execute(xmlStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
