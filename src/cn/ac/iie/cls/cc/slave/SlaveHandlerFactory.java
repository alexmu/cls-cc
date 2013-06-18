/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave;

import cn.ac.iie.cls.cc.slave.clsagent.CLSAgentDataCollectHandler;
import cn.ac.iie.cls.cc.slave.clsagent.CLSAgentDownloadHandler;
import cn.ac.iie.cls.cc.slave.clsagent.CLSAgentReportHandler;
import cn.ac.iie.cls.cc.slave.clsagent.ClsAgentStatusHandler;
import cn.ac.iie.cls.cc.slave.datacachecluster.DataCacheClusterStatusHandler;
import cn.ac.iie.cls.cc.slave.datadispatch.DataDispatchExecuteHandler;
import cn.ac.iie.cls.cc.slave.dataetl.DataETLCheckStatusHandler;
import cn.ac.iie.cls.cc.slave.dataetl.DataETLExecuteHandler;
import cn.ac.iie.cls.cc.slave.dataetl.DataETLTaskReportHandler;
import cn.ac.iie.cls.cc.slave.dataetlcluster.DataEtlClusterStatusHandler;
import cn.ac.iie.cls.cc.slave.linkagecmd.ConfigExecuteHandler;
import cn.ac.iie.cls.cc.slave.linkagecmd.QueryExecuteHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterDataBaseCreateHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterDataBaseDropHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterDataBaseTableListHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterSessionHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterStatusHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterTableCreateHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterTableDropHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterTableInfoHandler;
import cn.ac.iie.cls.cc.slave.nosqlcluster.NoSqlClusterTableTruncateHandler;
import cn.ac.iie.cls.cc.slave.raccluster.RacClusterSessionHandler;
import cn.ac.iie.cls.cc.slave.raccluster.RacClusterStatusHandler;
import cn.ac.iie.cls.cc.slave.test.DataCollectJobExecTestHandler;
import cn.ac.iie.cls.cc.slave.welcome.WelcomeHandler;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class SlaveHandlerFactory {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(SlaveHandlerFactory.class.getName());
    }
    private static Map<String, Class> slaveClassSet = new HashMap<String, Class>() {
        {
            //dataetl
            put("/welcome", WelcomeHandler.class);
            //dataetl
            put("/dataetl/execute", DataETLExecuteHandler.class);
            put("/dataetl/checkstatus", DataETLCheckStatusHandler.class);
            put("/dataetl/etltaskreport", DataETLTaskReportHandler.class);
            //linkagecmd
            put("/linkagecmd/query/execute", QueryExecuteHandler.class);
            put("/linkagecmd/config/execute", ConfigExecuteHandler.class);
            //datadispatch
            put("/datadispatch/execute", DataDispatchExecuteHandler.class);
            //clsagent
            put("/clsagent/download", CLSAgentDownloadHandler.class);
            put("/clsagent/datacollect", CLSAgentDataCollectHandler.class);
            put("/clsagent/status", ClsAgentStatusHandler.class);
            put("/clsagent/report", CLSAgentReportHandler.class);
            //dataetlcluster
            put("/dataetlcluster/status", DataEtlClusterStatusHandler.class);
            //datacachecluster
            put("/datacachecluster/status", DataCacheClusterStatusHandler.class);
            //nosqlcluster
            put("/nosqlcluster/database/create", NoSqlClusterDataBaseCreateHandler.class);
            put("/nosqlcluster/database/drop", NoSqlClusterDataBaseDropHandler.class);
            put("/nosqlcluster/database/tablelist", NoSqlClusterDataBaseTableListHandler.class);
            put("/nosqlcluster/table/create", NoSqlClusterTableCreateHandler.class);
            put("/nosqlcluster/table/drop", NoSqlClusterTableDropHandler.class);
            put("/nosqlcluster/table/truncate", NoSqlClusterTableTruncateHandler.class);
            put("/nosqlcluster/table/info", NoSqlClusterTableInfoHandler.class);
            put("/nosqlcluster/status", NoSqlClusterStatusHandler.class);
            put("/nosqlcluster/session", NoSqlClusterSessionHandler.class);
            //raccluster
            put("/raccluster/status", RacClusterStatusHandler.class);
            put("/raccluster/session", RacClusterSessionHandler.class);
            //raccluster
            put("/test/dcjetest", DataCollectJobExecTestHandler.class);
        }
    };
    private static Map<String, SlaveHandler> slaveObjectSet = new HashMap<String, SlaveHandler>();

    public static SlaveHandler getSlaveHandler(String pRequestPath) throws Exception {
        SlaveHandler slaveHandler = null;

        slaveHandler = slaveObjectSet.get(pRequestPath);

        if (slaveHandler == null) {
            Class slaveHandlerClass = slaveClassSet.get(pRequestPath);
            if (slaveHandlerClass != null) {
                try {
                    slaveHandler = (SlaveHandler) (slaveHandlerClass.newInstance());
                    slaveObjectSet.put(pRequestPath, slaveHandler);
                } catch (Exception ex) {
                    slaveHandler = null;
                    throw new Exception("initializing slave handler for " + pRequestPath + " is failed for " + ex.getMessage(), ex);
                }
            } else {
                throw new Exception("no slave handler for " + pRequestPath + " is found ");
            }
        }
        return slaveHandler;
    }
}
