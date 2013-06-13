/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.clsagent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author alexmu
 */
public class DataCollectJobTracker implements Runnable {

    private static BlockingQueue<DataCollectJob> dataCollectJobWaitingList = new LinkedBlockingQueue<DataCollectJob>();
    private static Map<String, DataCollectJob> executingDataCollectJobSet = new HashMap<String, DataCollectJob>();
    private static DataCollectJobTracker dataCollectJobTracker = null;

    private DataCollectJobTracker() {
    }

    public static synchronized DataCollectJobTracker getDataCollectJobTracker() {
        if (dataCollectJobTracker == null) {
            dataCollectJobTracker = new DataCollectJobTracker();
            Thread dataCollectJobTrackerRunner = new Thread(dataCollectJobTracker);
            dataCollectJobTrackerRunner.start();
        }
        return dataCollectJobTracker;
    }

    public void appendJob(DataCollectJob pDataCollectJob) {
        try {
            dataCollectJobWaitingList.put(pDataCollectJob);
        } catch (Exception ex) {
        }
    }

    public void removeJob(DataCollectJob pDataCollectJob) {
        synchronized (executingDataCollectJobSet) {
            executingDataCollectJobSet.remove(pDataCollectJob.getProcessJobInstanceID());
        }
    }

    public void appendTask(String pDataProcessInstanceId, List<DataCollectTask> pDataCollectTaskList) {
        synchronized (executingDataCollectJobSet) {
            DataCollectJob dataCollectJob = executingDataCollectJobSet.get(pDataProcessInstanceId);
            dataCollectJob.appendTask(pDataCollectTaskList);
        }
    }

    public void responseTask(String pDataProcessInstanceId, List<DataCollectTask> pDataCollectTaskList) {
        synchronized (executingDataCollectJobSet) {
            DataCollectJob dataCollectJob = executingDataCollectJobSet.get(pDataProcessInstanceId);
            dataCollectJob.resposeTask(pDataCollectTaskList);
        }
    }

    @Override
    public void run() {
        DataCollectJob dataCollectJob = null;
        while (true) {
            try {
                dataCollectJob = dataCollectJobWaitingList.take();
                boolean succeeded = false;
                //dispatch
                if (succeeded) {
                    executingDataCollectJobSet.put(dataCollectJob.getProcessJobInstanceID(), dataCollectJob);
                } else {
                    dataCollectJobWaitingList.put(dataCollectJob);
                }
            } catch (Exception ex) {
            }
        }
    }
}
