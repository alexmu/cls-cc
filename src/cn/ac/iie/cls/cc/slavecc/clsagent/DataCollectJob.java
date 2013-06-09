/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.clsagent;

import cn.ac.iie.cls.cc.slavecc.dataetl.ETLJob;
import cn.ac.iie.cls.cc.slavecc.dataetl.ETLJobTracker;
import cn.ac.iie.cls.cc.slavecc.dataetl.ETLTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class DataCollectJob {
    
    private String processJobInstanceID = "";
    private String dataProcessDescriptor = "";
    private Map<String, DataCollectTask> dataCollectTaskSet = new HashMap<String, DataCollectTask>();
    private Map<String, DataCollectTask> succeededDataCollectTaskSet = new HashMap<String, DataCollectTask>();
    private Map<String, DataCollectTask> failedDataCollectTaskSet = new HashMap<String, DataCollectTask>();
    static Logger logger = null;
    
    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DataCollectJob.class.getName());
    }
    
    public DataCollectJob(String pProcessJobInstanceID, String pDataProcessDescriptor) {
        processJobInstanceID = pProcessJobInstanceID;
        dataProcessDescriptor = pDataProcessDescriptor;
    }
    
    public String getProcessJobInstanceID() {
        return processJobInstanceID;
    }
    
    public String getDataProcessDescriptor() {
        return dataProcessDescriptor;
    }
    
    public void appendTask(List<DataCollectTask> pDataCollectTaskList) {
        for (DataCollectTask dataCollectTask : pDataCollectTaskList) {
            dataCollectTaskSet.put(dataCollectTask.fileName, dataCollectTask);
        }
        ETLJob etlJob = ETLJobTracker.getJob(processJobInstanceID);
        if (etlJob != null) {
            etlJob.setTask2doNum(pDataCollectTaskList.size());
        }
    }
    
    public void resposeTask(List<DataCollectTask> pDataCollectTaskList) {
        List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
        for (DataCollectTask dataCollectTask : pDataCollectTaskList) {
            switch (dataCollectTask.taskStatus) {
                case DataCollectTask.SUCCEEDED:
                    succeededDataCollectTaskSet.put(dataCollectTask.fileName, dataCollectTask);
                    etlTaskList.add(new ETLTask(dataCollectTask.fileName));
                    dataCollectTaskSet.remove(dataCollectTask.fileName);
                    break;
                case DataCollectTask.FAILED:
                    failedDataCollectTaskSet.put(dataCollectTask.fileName, dataCollectTask);
                    dataCollectTaskSet.remove(dataCollectTask.fileName);
                    break;
                default:
                    logger.warn("unknown task status " + dataCollectTask.taskStatus + " for data collect task of " + dataCollectTask.fileName);
            }
        }
        //add list
        ETLJob etlJob = ETLJobTracker.getJob(processJobInstanceID);
        if (etlJob != null) {
            ETLJobTracker.appendTask(processJobInstanceID, etlTaskList);
        }
        
        if (dataCollectTaskSet.size() < 1) {
            if (succeededDataCollectTaskSet.size() < 1) {
                logger.error("data collect job for data process job " + processJobInstanceID + " is finished unsuccessfully");
            } else if (failedDataCollectTaskSet.size() > 0) {
                logger.warn("data collect job for data process job " + processJobInstanceID + " is finished partially successfully");
            } else {
                logger.info("data collect job for data process job " + processJobInstanceID + " is finished successfully");
            }
            DataCollectJobTracker.removeJob(this);
        }
    }
}
