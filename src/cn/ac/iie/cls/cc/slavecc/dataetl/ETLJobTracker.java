/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.dataetl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author alexmu
 */
public class ETLJobTracker implements Runnable {

    private static BlockingQueue<ETLJob> etlJobWaitingList = new LinkedBlockingQueue<ETLJob>();
    private static Map<String, ETLJob> etlJobSet = new HashMap<String, ETLJob>();
    private static ETLJobTracker etlJobTracker = null;

    private ETLJobTracker() {
    }

    public static synchronized ETLJobTracker getETLJobTracker() {
        if (etlJobTracker == null) {
            etlJobTracker = new ETLJobTracker();
            Thread etlJobTrackerRunner = new Thread(etlJobTracker);
            etlJobTrackerRunner.start();
        }
        return etlJobTracker;
    }

    public void appendJob(ETLJob pETLJob) {
        try {
            etlJobWaitingList.put(pETLJob);
        } catch (Exception ex) {
        }
    }

    public void removeJob(ETLJob pETLJob) {
        synchronized (etlJobSet) {
            etlJobSet.remove(pETLJob.getProcessJobInstanceID());
        }
    }

    public ETLJob getJob(String pProcessJobInstanceID) {
        synchronized (etlJobSet) {
            return etlJobSet.get(pProcessJobInstanceID);
        }
    }

    public void appendTask(String pDataProcessInstanceId, List<ETLTask> pETLTaskList) {
        synchronized (etlJobSet) {
            ETLJob etlJob = etlJobSet.get(pDataProcessInstanceId);
            etlJob.appendTask(pETLTaskList);
        }
    }

    public void responseTask(String pDataProcessInstanceId, List<ETLTask> pETLTaskList) {
        synchronized (etlJobSet) {
            ETLJob etlJob = etlJobSet.get(pDataProcessInstanceId);
            etlJob.resposeTask(pETLTaskList);
        }
    }

    @Override
    public void run() {
        ETLJob etlJob = null;
        while (true) {
            try {
                etlJob = etlJobWaitingList.take();
                boolean succeeded = false;
                //dispatch
                if (succeeded) {
                    etlJobSet.put(etlJob.getProcessJobInstanceID(), etlJob);
                } else {
                    etlJobWaitingList.put(etlJob);
                }
            } catch (Exception ex) {
            }
        }
    }
}
