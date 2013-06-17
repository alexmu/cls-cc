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
                //add by zy
                String host = "http://127.0.0.1";
                int port = 7080;
                String content = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"
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
                // String content = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><requestParams><processJobInstanceId>8a8081e8288b759201288b97c73a000e</processJobInstanceId><processConfig><operator name=\"read1_filter1\" class=\"Process\" version=\"1.0\" x=\"-1\" y=\"-1\">                        <operator name=\"csv_file_reader1\" class=\"CSVFileInput\" version=\"1.0\" x=\"-1\" y=\"-1\">                <parameter name=\"csvFile\">file://./csv-sample-file.csv</parameter>                <parameter name=\"hasHeader\">true</parameter>                <parameter name=\"trimLines\">true</parameter>                <parameter name=\"fileEncoding\">UTF-8</parameter>                <parameterlist name=\"columnSet\">                    <parametermap columnindex=\"1\" columnname=\"isp \" columntype=\"String\"/>                    <parametermap columnindex=\"2\" columnname=\"time\" columntype=\"Timestamp\" frompattern=\"YY-MM-DD HH:mm:SS\" topattern=\"YYYY-MM-DD HH:mm:SS\"/>                </parameterlist>                                         </operator>                        <operator name=\"outporter1\" class=\"AlmightyOutput\" x=\"10\" y=\"10\">                            </operator>                        <connect from=\"csv_file_reader1.outport1\" to=\"outporter1.inport1\"/>        </operator></processConfig></requestParams>";
                try {
                    HttpClient httpClient = new DefaultHttpClient();
                    HttpPost httppost = new HttpPost(host+":"+port+"/resources/clsagent/datacollect");

                    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(content.getBytes()), -1);
                    reqEntity.setContentType("binary/octet-stream");
                    reqEntity.setChunked(true);
                    httppost.setEntity(reqEntity);
                    HttpResponse response = httpClient.execute(httppost);
                    httppost.releaseConnection();
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
}
