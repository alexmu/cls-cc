/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.clsagent;

import cn.ac.iie.cls.cc.slave.SlaveHandler;

/**
 *
 * @author alexmu
 */
public class CLSAgentDataCollectHandler implements SlaveHandler {

//    private 
    public String execute(String pRequestContent) {

        DataCollectJob dataCollectJob = DataCollectJob.getDataCollectJob(pRequestContent);
        if (dataCollectJob != null) {
            DataCollectJobTracker.getDataCollectJobTracker().appendJob(dataCollectJob);
            return "ok";
        } else {
            return "failed";
        }
    }
}
