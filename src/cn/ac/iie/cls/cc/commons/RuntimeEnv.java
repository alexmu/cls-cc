/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.commons;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.ict.ncic.util.dao.util.ClusterInfoOP;
import cn.ac.iie.cls.cc.config.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class RuntimeEnv {

    private static Configuration conf = null;
    private static final String DB_CLUSTERS = "dbClusters";
    public static final String METADB_CLUSTER = "metaDBCluster";
    public static final String HDFS_CONN_STR = "hdfsConnStr";
    public static final String HIVE_CONN_STR = "hiveConnStr";
    private static Map<String, Object> dynamicParams = new HashMap<String, Object>();
    //logger
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(RuntimeEnv.class.getName());
    }

    public static boolean initialize(Configuration pConf) {

        if (pConf == null) {
            logger.error("configuration object is null");
            return false;
        }

        conf = pConf;

        String dbCluster = conf.getString(DB_CLUSTERS, "");
        if (dbCluster.isEmpty()) {
            logger.error("parameter dbcluster does not exist or is not defined");
            return false;
        }

        try {
            DaoPool.putDao(ClusterInfoOP.getDBClusters(dbCluster));//need check
        } catch (Exception ex) {
            logger.error("init dao is failed for " + ex);
            return false;
        }

        String hdfsConnStr = conf.getString(HDFS_CONN_STR, "");
        if (hdfsConnStr.isEmpty()) {
            logger.error("parameter hdfsConnStr does not exist or is not defined");
            return false;
        }

        addParam(HDFS_CONN_STR, hdfsConnStr);


        String hiveConnStr = conf.getString(HIVE_CONN_STR, "");
        if (hiveConnStr.isEmpty()) {
            logger.error("parameter hiveConnStr does not exist or is not defined");
            return false;
        }

        addParam(HIVE_CONN_STR, hiveConnStr);
        return true;
    }

    public static void dumpEnvironment() {
        conf.dumpConfiguration();
    }

    public static void addParam(String pParamName, Object pValue) {
        synchronized (dynamicParams) {
            dynamicParams.put(pParamName, pValue);
        }
    }

    public static Object getParam(String pParamName) {
        return dynamicParams.get(pParamName);
    }
}
