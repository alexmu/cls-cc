/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author alexmu
 */
public class ETLJobTracker implements Runnable {

    private static BlockingQueue<ETLJob> etlJobWaitingList = new LinkedBlockingQueue<ETLJob>();
    private static Map<String, ETLJob> etlJobSet = new HashMap<String, ETLJob>();
    private Lock etlJobSetLock = new ReentrantLock();
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
        etlJobSetLock.lock();
        etlJobSet.remove(pETLJob.getProcessJobInstanceID());
        etlJobSetLock.unlock();
    }

    public ETLJob getJob(String pProcessJobInstanceID) {
        etlJobSetLock.lock();
        ETLJob etlJob = etlJobSet.get(pProcessJobInstanceID);
        etlJobSetLock.unlock();
        return etlJob;
    }

    public void appendTask(String pDataProcessInstanceId, List<ETLTask> pETLTaskList) {
        etlJobSetLock.lock();
        ETLJob etlJob = etlJobSet.get(pDataProcessInstanceId);
        etlJobSetLock.unlock();
        etlJob.appendTask(pETLTaskList);

    }

    public void responseTask(String pDataProcessInstanceId, List<ETLTask> pETLTaskList) {
        etlJobSetLock.lock();
        ETLJob etlJob = etlJobSet.get(pDataProcessInstanceId);
        etlJobSetLock.unlock();
        etlJob.responseTask(pETLTaskList);

    }

    @Override
    public void run() {
        ETLJob etlJob = null;
        while (true) {
            try {
                etlJob = etlJobWaitingList.take();
                Thread etlJobRunner = new Thread(etlJob);
                etlJobRunner.start();
                etlJobSetLock.lock();
                etlJobSet.put(etlJob.getProcessJobInstanceID(), etlJob);
                etlJobSetLock.unlock();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
