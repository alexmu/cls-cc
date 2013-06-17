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
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author wanghh19880807
 */
public class NoSqlClusterTableCreateHandler implements SlaveHandler{
   
    private static final String SUCCESS_RESPONSE = "<response><message>MESSAGE<message></response>";
    private static final String FAIL_RESPONSE = "<error><message>MESSAGE<message></error>";
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(NoSqlClusterTableCreateHandler.class.getName());
    }

    public String execute(String pRequestContent) {
        String result = "";//返回的结果

        String databaseName = null;
        String tableName = null;
        String comment = null;
        Connection conn = null;
        
        try {
            Document databaseDropDoc = DocumentHelper.parseText(pRequestContent);
            Element requestParamsElt = databaseDropDoc.getRootElement();
            Element databaseNameElt = requestParamsElt.element("databaseName");
            databaseName = databaseNameElt == null ? "" : databaseNameElt.getStringValue();
            Element tableNameElt = requestParamsElt.element("name"); 
            tableName = tableNameElt == null ? "" : tableNameElt.getStringValue();
            Element commentElt = requestParamsElt.element("comment");
            comment = commentElt == null ? "" : commentElt.getStringValue();
            
            Element columnsNameElt = requestParamsElt.element("columns");
            
            
            //获取column对象
            List<Element> columnNameElts =columnsNameElt.elements("column");
            //共读取到了多少列
            int columnCount = columnNameElts.size();
            //用来存放name与type的内容
            String[] columnNames = new String[columnCount];
            String[] columnTypes = new String[columnCount];
            int index = 0;
            for(Element columnNameElt:columnNameElts){
                //获取name对象的内容
                columnNames[index] = columnNameElt.element("name").getStringValue();
                //获取type对象的内容,并将其进行类型转换
                columnTypes[index]=this.getColumnType(columnNameElt.element("type").getStringValue());
                index++;
            }
            
            
            if (databaseName.isEmpty()) {
                result = FAIL_RESPONSE.replace("MESSAGE", databaseName +" is not defined");
            }else if(tableName.isEmpty()){
                result = FAIL_RESPONSE.replace("MESSAGE", tableName + " is not defined");
            }else if(columnsNameElt == null){
                result = FAIL_RESPONSE.replace("MESSAGE",  "columns is not defined");
            }else {
                //connect to hive
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection("jdbc:hive://192.168.120.46:10000/default", "", "");
                Statement stmt = conn.createStatement();
                //parse xml
                //then create table
                //将获取的数据进行拼接，编写创建表的SQL语句
                String sql = "CREATE TABLE IF NOT EXISTS " + databaseName+"."+ tableName+"(" ;
                for(int i = 0;i<index;i++){
                    if(i<index-1){
                        sql += columnNames[i]+" "+columnTypes[i]+",";
                    }else{
                        sql += columnNames[i]+" "+columnTypes[i]+")"+"comment "+"'"+comment+"'";
                    }
                }
             
                logger.info(sql);
                stmt.executeQuery(sql);
     
                result = SUCCESS_RESPONSE.replace("MESSAGE", "create table " + databaseName +"."+ tableName + " successfully");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            result = FAIL_RESPONSE.replace("MESSAGE", "create table " + databaseName +"."+  tableName + " unsuccessfully for " + ex.getMessage());
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                if(conn!=null){
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
    
    public String getColumnType(String oldType){
       
        if("CHAR".equalsIgnoreCase(oldType) ||"clob".equalsIgnoreCase(oldType)){
            return "STRING";
        }else if(oldType.toUpperCase().startsWith("VARCHAR2")){
            return "STRING";
        }else if("LONG".equalsIgnoreCase(oldType)){
            return "BIGINT";
        }else if("NUMBER".equalsIgnoreCase(oldType)){
            return "DOUBLE";
        }else if("INTEGER".equalsIgnoreCase(oldType)){
            return "INT";
        }else if("FLOAT".equalsIgnoreCase(oldType)){
            return "FLOAT";
        }else if("DATE".equalsIgnoreCase(oldType)){
            return "TIMESTAMP";
        }else if("BLOB".equalsIgnoreCase(oldType)){
            return "BINARY";
        }else{
            return oldType;
        }
        
    }
    
    
    public static void main(String[] args) {
        File inputXml = new File("create-table-specific.xml");
        try {
            String xmlStr = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                xmlStr += line;
            }
            System.out.println(xmlStr);
            System.out.println("OK");
            NoSqlClusterTableCreateHandler handler = new NoSqlClusterTableCreateHandler();
            System.out.println(handler.execute(xmlStr));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    
}
