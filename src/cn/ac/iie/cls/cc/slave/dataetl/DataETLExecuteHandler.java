/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJob;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJobTracker;

/**
 *
 * @author alexmu
 */
public class DataETLExecuteHandler implements SlaveHandler {   
    
    @Override
    public String execute(String pRequestContent) {
        String result = null;

        
        ETLJob etlJob = ETLJob.getETLJob(pRequestContent);

        if (etlJob != null) {
            String clsAgentDataCollectDescriptor = etlJob.getDataProcessDescriptor().get(ETLJob.CLS_AGENT_DATA_COLLECT_DESC);
            if (clsAgentDataCollectDescriptor != null) {
                DataCollectJob dataCollectJob = new DataCollectJob(etlJob.getProcessJobInstanceID(), clsAgentDataCollectDescriptor);
                DataCollectJobTracker.getDataCollectJobTracker().appendJob(dataCollectJob);
            }
           
            ETLJobTracker.getETLJobTracker().appendJob(etlJob);
            result = "succeeded";
        } else {
            result = "failed";
        }

        return result;
    }

    public static void main(String[] args) {
    }
}
