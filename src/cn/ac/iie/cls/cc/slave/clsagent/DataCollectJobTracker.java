/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.clsagent;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
            dataCollectJob.responseTask(pDataCollectTaskList);
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
                //add by zy
                String host = "http://192.168.111.128";
                int port = 7080;
                String content = dataCollectJob.getDataProcessDescriptor();
                try {
                    HttpClient httpClient = new DefaultHttpClient();
                    HttpPost httppost = new HttpPost(host + ":" + port + "/resources/clsagent/datacollect");

                    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(content.getBytes()), -1);
                    reqEntity.setContentType("binary/octet-stream");
                    reqEntity.setChunked(true);
                    httppost.setEntity(reqEntity);
                    HttpResponse response = httpClient.execute(httppost);
                    System.out.println(response.getStatusLine());
                    httppost.releaseConnection();
                    succeeded = true;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                //end
                if (succeeded) {
                    executingDataCollectJobSet.put(dataCollectJob.getProcessJobInstanceID(), dataCollectJob);
                } else {
                    dataCollectJobWaitingList.put(dataCollectJob);
                }
            } catch (Exception ex) {
            }
        }
    }

    public static void main(String[] args) {
        String dataProcessDesc = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
                + "<requestParams>"
                + "<processJobInstanceId>8a8081e8288b759201288b97c73a000e</processJobInstanceId>"
                + "<processConfig>"
                + "<operator name=\"operator_id_123\" class=\"GetherDataFromLocalFS\" version=\"1.0\" x=\"-1\" y=\"-1\">"
                + "<parameter name=\"name\">917mt6</parameter>"
                + "<parameter name=\"srcpath\">/home/iie</parameter>"
                + "<parameter name=\"hdfsPath\">hdfs://192.168.111.128:9000/test/123/456</parameter>"
                + "<parameter name=\"timeout\">10000000</parameter>"
                + "<parameterlist name=\"splitConfig\">"
                + "<parametermap name=\"exe\" value=\"true\"/>"
                + "<parametermap name=\"linesplitor\" value=\"\n\"/>"
                + "<parametermap name=\"num\" value=\"1000\"/>"
                + "</parameterlist>"
                + "</operator>"
                + "</processConfig>"
                + "</requestParams>";
        DataCollectJobTracker.getDataCollectJobTracker().appendJob(new DataCollectJob("8a8081e8288b759201288b97c73a000e", dataProcessDesc));
        new DataCollectJobTracker().run();
    }
}
