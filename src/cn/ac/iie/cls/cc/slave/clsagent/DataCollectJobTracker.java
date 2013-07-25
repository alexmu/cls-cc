/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.clsagent;

import cn.ac.iie.cls.cc.commons.RuntimeEnv;
import cn.ac.iie.cls.cc.util.XMLReader;
import cn.ac.iie.cls.cc.util.ZooKeeperOperator;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 *
 * @author alexmu
 */
public class DataCollectJobTracker implements Runnable {

    private BlockingQueue<DataCollectJob> dataCollectJobWaitingList = new LinkedBlockingQueue<DataCollectJob>();
    private Map<String, DataCollectJob> executingDataCollectJobSet = new HashMap<String, DataCollectJob>();
    private Lock executingDataCollectJobSetLock = new ReentrantLock();
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
        executingDataCollectJobSetLock.lock();
        executingDataCollectJobSet.remove(pDataCollectJob.getProcessJobInstanceID());
        executingDataCollectJobSetLock.unlock();
    }

    public void appendTask(String pDataProcessInstanceId, List<DataCollectTask> pDataCollectTaskList) {
        executingDataCollectJobSetLock.lock();
        DataCollectJob dataCollectJob = executingDataCollectJobSet.get(pDataProcessInstanceId);
        executingDataCollectJobSetLock.unlock();
        dataCollectJob.appendTask(pDataCollectTaskList);

    }

    public void responseTask(String pDataProcessInstanceId, List<DataCollectTask> pDataCollectTaskList) {
        executingDataCollectJobSetLock.lock();
        DataCollectJob dataCollectJob = executingDataCollectJobSet.get(pDataProcessInstanceId);
        executingDataCollectJobSetLock.unlock();
        dataCollectJob.responseTask(pDataCollectTaskList);

    }

    @Override
    public void run() {
        System.out.println("dasasdasdasdasdasdasdasdasdasdasdasdasdasdasd");
        DataCollectJob dataCollectJob = null;
        while (true) {
            try {
                System.out.println("dasasdasdasdasdasdasdasdasdasdasdasdasdasdasd");
                dataCollectJob = dataCollectJobWaitingList.take();
                boolean succeeded = false;
                //dispatch
                //add by zy
                String host = "";
                int port = 7080;
                String content = dataCollectJob.getDataProcessDescriptor();
                //begin:20130717
                String agentIP = XMLReader.getValueFromStrDGText(content, "agentIP");
                if (agentIP == null || agentIP.equals("")) {
                    System.out.println("agentIP is empty!");
                    return;
                } else {
                    host = "http://" + agentIP;
                }

                ZooKeeperOperator zkoperator = new ZooKeeperOperator();
                try {
                    zkoperator.connect((String)RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER));
                    if (zkoperator.exists("/agent/" + agentIP).equals("true")) {
                        ;
                    } else {
                        System.out.println("/agent/" + agentIP + " don't exist!");
                        return;
                    }
                } catch (Exception ex) {
                    System.out.println("zookeeper err!");
                    return;
                }
                //end:2030717

                try {
                    HttpClient httpClient = new DefaultHttpClient();
                    HttpPost httppost = new HttpPost(host + ":" + port + "/resources/clsagent/execmd");

                    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(content.getBytes()), -1);
                    reqEntity.setContentType("binary/octet-stream");
                    reqEntity.setChunked(true);
                    httppost.setEntity(reqEntity);
                    executingDataCollectJobSetLock.lock();
                    HttpResponse response = httpClient.execute(httppost);
                    System.out.println(response.getStatusLine());
                    httppost.releaseConnection();
                    succeeded = true;
                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    //end
                    if (succeeded) {
                        executingDataCollectJobSet.put(dataCollectJob.getProcessJobInstanceID(), dataCollectJob);
                        executingDataCollectJobSetLock.unlock();
                    } else {
                        dataCollectJobWaitingList.put(dataCollectJob);
                    }
                }
            } catch (Exception ex) {
            }
        }
    }
}
