/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.nosqlcluster;

import cn.ac.iie.cls.cc.slavecc.SlaveHandler;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 *
 * @author alexmu
 */
public class NoSqlClusterDataBaseCreateHandler implements SlaveHandler {

    public String execute(String pRequestContent) {
        String result = null;
        try {
            //connect to hive
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
            Connection conn = DriverManager.getConnection("jdbc:hive://192.168.120.46:10000/default", "", "");
            Statement stmt = conn.createStatement();
            //parse xml
            //then create table
            stmt.executeQuery("create database sampledatabase");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        NoSqlClusterDataBaseCreateHandler handler = new NoSqlClusterDataBaseCreateHandler();
        handler.execute("xml");
    }
}
