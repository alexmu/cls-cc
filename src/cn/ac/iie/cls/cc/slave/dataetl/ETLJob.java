/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.iie.cls.cc.util.XMLReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

/**
 *
 * @author alexmu
 */
public class ETLJob implements Runnable {

    private String processJobInstanceID = "";
    private Map<String, String> dataProcessDescriptor = new HashMap<String, String>();
    public static final String CLS_AGENT_DATA_COLLECT_DESC = "clsAgentETLDesc";
    public static final String DATA_ETL_DESC = "dataEtlDesc";
    private static final String PROCESS_JOB_DESC = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><requestParams><processJobInstanceId>PROCESS_JOB_ID</processJobInstanceId><processConfig>PROCESS_CONFIG</processConfig></requestParams>";
    private int task2doNum;
    private BlockingQueue<ETLTask> etlTaskWaitingList = new LinkedBlockingQueue<ETLTask>();
    private Map<String, ETLTask> etlTaskSet = new HashMap<String, ETLTask>();
    private Lock etlTaskSetLock = new ReentrantLock();
    private Map<String, ETLTask> succeededETLTaskSet = new HashMap<String, ETLTask>();
    private Map<String, ETLTask> failedETLTaskSet = new HashMap<String, ETLTask>();
    private Map<String, Map> operatorStatSet = new HashMap<String, Map>();
    private Map<String, Set> operatorRelationSet = new HashMap<String, Set>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLJob.class.getName());
    }

    private ETLJob() {
    }

    public static ETLJob getETLJob(String pProcessJobDescriptor) {
        logger.info(pProcessJobDescriptor);
        ETLJob dataProcessJob = new ETLJob();
        try {
            Document processJobDoc = DocumentHelper.parseText(pProcessJobDescriptor);
            Element processJobInstanceElt = processJobDoc.getRootElement();

            Element processJobInstanceIdElt = processJobInstanceElt.element("processJobInstanceId");
            if (processJobInstanceIdElt == null) {
                logger.error("no processJobInstanceId element found in " + pProcessJobDescriptor);
                dataProcessJob = null;
            } else {
                dataProcessJob.processJobInstanceID = processJobInstanceIdElt.getStringValue();
                Element processConfigElt = processJobInstanceElt.element("processConfig");
                if (processConfigElt == null) {
                    logger.error("no processConfig element found in " + pProcessJobDescriptor);
                    dataProcessJob = null;
                } else {
                    dataProcessJob.parse(processConfigElt.element("operator").asXML());
                }
            }
            dataProcessJob.task2doNum = -1;
        } catch (Exception ex) {
            logger.error("creating data process job instance is failed for " + ex.getMessage(), ex);
            dataProcessJob = null;
        }
        return dataProcessJob;
    }

    private String getXmlString(Element pOperatorNode) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XMLWriter xw = new XMLWriter(bos);
        xw.write(pOperatorNode);
        xw.close();
        xw = null;
        return new String(bos.toByteArray(), "UTF-8");
    }

    private void parse(String pDataProcessDescriptor) throws Exception {
        System.out.println(pDataProcessDescriptor);
        Document dataProcessDocument = DocumentHelper.parseText(pDataProcessDescriptor);
        Element operatorNode = dataProcessDocument.getRootElement();

        String operatorNodeXmlStr = getXmlString(operatorNode);
        logger.debug("data process descriptor:" + operatorNodeXmlStr);

        List<Element> elements = operatorNode.elements("operator");

        int clsAgentETLOperatorDescCnt = 0;
        String clsAgentETLOperatorName = "";
        for (Element element : elements) {
            if (element.attributeValue("class").startsWith("GetherDataFrom")) {
                dataProcessDescriptor.put(CLS_AGENT_DATA_COLLECT_DESC, PROCESS_JOB_DESC.replace("PROCESS_JOB_ID", processJobInstanceID).replace("PROCESS_CONFIG", getXmlString(element)));
                clsAgentETLOperatorName = element.attributeValue("name");
                operatorNode.remove(element);
                clsAgentETLOperatorDescCnt++;
            }
        }

//        if (clsAgentETLOperatorDescCnt > 1) {
//            throw new Exception("the descriptor'number of etlor  of cls agent in data process descriptor:" + operatorNodeXmlStr + " is " + clsAgentETLOperatorDescCnt + ",which is not correct");
//        }

        if (clsAgentETLOperatorDescCnt > 0) {
            elements = operatorNode.elements("connect");
            int clsAgentETLConnectCnt = 0;
            for (Element element : elements) {
                if (element.attributeValue("from").startsWith(clsAgentETLOperatorName)) {
                    operatorNode.remove(element);
                    clsAgentETLConnectCnt++;
                }
            }
//        if (clsAgentETLConnectCnt < 1) {
//            throw new Exception("the connector'number of etlor of cls agent in data process descriptor:" + operatorNodeXmlStr + " is less than 1");
//        }
        }

        elements = operatorNode.elements("connect");
        for (Element element : elements) {
            if (element.attributeValue("to").startsWith("parent.out")) {
                operatorNode.remove(element);
            }
        }


        dataProcessDescriptor.put(DATA_ETL_DESC, PROCESS_JOB_DESC.replace("PROCESS_JOB_ID", processJobInstanceID).replace("PROCESS_CONFIG", getXmlString(operatorNode)));
    }

    public String getProcessJobInstanceID() {
        return processJobInstanceID;
    }

    public Map<String, String> getDataProcessDescriptor() {
        return dataProcessDescriptor;
    }

    public String getInputFilePathStr() {
        String filePath = "";
        try {
            Document dataProcessDocument = DocumentHelper.parseText(dataProcessDescriptor.get(DATA_ETL_DESC));
            Element operatorNode = dataProcessDocument.getRootElement();

            String operatorNodeXmlStr = getXmlString(operatorNode);
            logger.debug("data process descriptor:" + operatorNodeXmlStr);

            List<Element> elements = operatorNode.element("processConfig").element("operator").elements("operator");

            for (Element element : elements) {
                if (element.attributeValue("class").startsWith("TXTFileInput") || element.attributeValue("class").startsWith("CSVFileInput") || element.attributeValue("class").startsWith("XMLFileInput")) {
                    List<Element> paramElts = element.elements("parameter");
                    for (Element paramElt : paramElts) {
                        if (paramElt.attributeValue("name").endsWith("File")) {
                            filePath = paramElt.getTextTrim();
                        }
                    }
                }
            }
            return filePath;
        } catch (Exception ex) {
            return null;
        }
    }

    public void appendTask(List<ETLTask> pETLTaskList) {
        for (ETLTask etlTask : pETLTaskList) {
            try {
                etlTaskWaitingList.put(etlTask);
            } catch (Exception ex) {
            }
        }
    }

    private synchronized void updateJobStat(String pTaskStat) {
        String[] operatorStatItems = pTaskStat.split("\\$");
        for (String operatorStatItem : operatorStatItems) {
            System.out.println(operatorStatItem);
            String[] statItems = operatorStatItem.split(":");
            String[] operatorDescItems = statItems[0].split(",");
            Set<String> subOperatorSet = operatorRelationSet.get(operatorDescItems[1]);
            if (subOperatorSet == null) {
                subOperatorSet = new TreeSet<String>();
                subOperatorSet.add(operatorDescItems[0]);
                operatorRelationSet.put(operatorDescItems[1], subOperatorSet);
            } else {
                subOperatorSet.add(operatorDescItems[0]);
            }

            if (statItems.length > 1) {
                String[] portMetricItems = statItems[1].split(",");
                Map<String, Integer> portMetricSet = operatorStatSet.get(operatorDescItems[0]);
                if (portMetricSet == null) {
                    portMetricSet = new HashMap<String, Integer>();
                    operatorStatSet.put(operatorDescItems[0], portMetricSet);
                }
                for (String portMetricItem : portMetricItems) {
                    String[] kvs = portMetricItem.split("\\-");
                    Integer metric = portMetricSet.get(kvs[0]);
                    if (metric == null) {
                        portMetricSet.put(kvs[0], new Integer(Integer.parseInt(kvs[1])));
                    } else {
                        int metricn = metric.intValue() + Integer.parseInt(kvs[1]);
                        portMetricSet.put(kvs[0], new Integer(metricn));
                    }
                }
            }

        }
    }

    private String getOperatorStat(String pOperatorName, String pParentOperatorUri) {
        String operatorStat = "<tracker><trackerId>TRACKER_ID</trackerId><jobInstanceId>JOB_INSTANCE_ID</jobInstanceId><operatorUri>OPERATOR_URI</operatorUri><duration></duration><runningState>SUCCEED</runningState><errorMessage></errorMessage><portCounters>PORT_COUNTERS</portCounters><children>CHILDREN</children></tracker>";
        String portCounterTemplate = "<portCounter><portName>PORT_NAME</portName><rowSetCount>0</rowSetCount><rowCount>ROW_COUNT</rowCount></portCounter>";
        operatorStat = operatorStat.replace("TRACKER_ID", processJobInstanceID + "_" + pOperatorName);
        operatorStat = operatorStat.replace("JOB_INSTANCE_ID", processJobInstanceID);
        operatorStat = operatorStat.replace("OPERATOR_URI", pParentOperatorUri + "/" + pOperatorName);

        Map<String, Integer> portMetrics = operatorStatSet.get(pOperatorName);
        String portCounters = "";
        if (portMetrics != null) {
            Iterator portMetricEntryItor = portMetrics.entrySet().iterator();
            String portCounter = "";
            while (portMetricEntryItor.hasNext()) {
                Entry portMetricEntry = (Entry) portMetricEntryItor.next();
                portCounter = portCounterTemplate.replace("PORT_NAME", (String) portMetricEntry.getKey());
                portCounter = portCounter.replace("ROW_COUNT", ((Integer) portMetricEntry.getValue()).toString());
                portCounters += portCounter;
            }
            operatorStat = operatorStat.replace("PORT_COUNTERS", portCounters);
        } else {
            operatorStat = operatorStat.replace("PORT_COUNTERS", "");
        }

        Set subOperatorSet = operatorRelationSet.get(pOperatorName);
        if (subOperatorSet != null) {
            String subOperatorStat = "";
            Iterator subOPeratorItor = subOperatorSet.iterator();
            while (subOPeratorItor.hasNext()) {
                String subOperatorName = (String) subOPeratorItor.next();
                subOperatorStat += getOperatorStat(subOperatorName, pParentOperatorUri + "/" + pOperatorName);
            }
            operatorStat = operatorStat.replace("CHILDREN", subOperatorStat);
        } else {
            operatorStat = operatorStat.replace("CHILDREN", "");
        }

        return operatorStat;
    }

    private void reportJobStat() {
        String topOperator = (String) (operatorRelationSet.get("null").iterator().next());
        String stat = getOperatorStat(topOperator, "");
        System.out.println(stat);
        String host = "http://10.128.125.69";
        int port = 8080;
        try {
            HttpClient httpClient = new DefaultHttpClient();
            HttpPost httppost = new HttpPost(host + ":" + port + "/CoLinkage/resources/processJobInstance/"+processJobInstanceID+"/operatorTracker");

            InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(stat.getBytes()), -1);
            //reqEntity.setContentType("binary/octet-stream");
            reqEntity.setContentType("application/xml");//; charset=UTF-8
//            reqEntity.setChunked(true);
            httppost.setEntity(reqEntity);
            HttpResponse response = httpClient.execute(httppost);
            System.out.println(response.getStatusLine());
            //fixme:dispose task dispatch error                        
            httppost.releaseConnection();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void responseTask(List<ETLTask> pETLTaskList) {
        synchronized (this) {
            for (ETLTask etlTask : pETLTaskList) {
                switch (etlTask.taskStatus) {
                    case ETLTask.SUCCEEDED:
                        succeededETLTaskSet.put(etlTask.filePath, etlTask);
                        updateJobStat(etlTask.taskStat);
                        etlTaskSet.remove(etlTask.filePath);
                        break;
                    case ETLTask.FAILED:
                        failedETLTaskSet.put(etlTask.filePath, etlTask);
                        updateJobStat(etlTask.taskStat);
                        etlTaskSet.remove(etlTask.filePath);
                        break;
                    case ETLTask.EXECUTING:
                        break;
                    default:
                        logger.warn("unknown task status " + etlTask.taskStatus + " for etl task of " + etlTask.filePath);
                }
            }
        }
    }

    public void setTask2doNum(int task2doNum) {
        this.task2doNum = task2doNum;
    }

    @Override
    public void run() {
        while (true) {
            try {
                ETLTask etlTask = etlTaskWaitingList.peek();
                if (etlTask != null) {

                    boolean succeeded = false;
                    //dispatch
                    String host = "http://10.128.125.73";
                    int port = 7070;
                    String content = dataProcessDescriptor.get(DATA_ETL_DESC).replace("${FILE_PATH}", etlTask.filePath);
                    try {
                        HttpClient httpClient = new DefaultHttpClient();
                        HttpPost httppost = new HttpPost(host + ":" + port + "/resources/etltask/execute");

                        InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(content.getBytes()), -1);
                        reqEntity.setContentType("binary/octet-stream");
                        reqEntity.setChunked(true);
                        httppost.setEntity(reqEntity);
                        etlTaskSetLock.lock();
                        HttpResponse response = httpClient.execute(httppost);
                        System.out.println(response.getStatusLine());
                        //fixme:dispose task dispatch error                        
                        httppost.releaseConnection();
                        succeeded = true;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        //end
                        if (succeeded) {
                            etlTaskWaitingList.take();
                            etlTaskSet.put(etlTask.filePath, etlTask);
                            etlTaskSetLock.unlock();
                        }
                    }
                }

                synchronized (this) {
                    logger.info("td:" + task2doNum + ",tt:" + etlTaskSet.size() + ",st:" + succeededETLTaskSet.size() + ",ft:" + failedETLTaskSet.size());
                    if (task2doNum == 0) {
                        logger.info("etl job for data process job " + processJobInstanceID + " is finished with no task to do");
                        break;
                    } else if (task2doNum > 0) {
                        int taskDoneNum = succeededETLTaskSet.size() + failedETLTaskSet.size();

                        if (taskDoneNum == task2doNum) {
                            if (succeededETLTaskSet.size() == 0) {
                                logger.error("etl job for data process job " + processJobInstanceID + " is finished unsuccessfully");
                            } else if (failedETLTaskSet.size() == 0) {
                                logger.warn("etl job for data process job " + processJobInstanceID + " is finished successfully");
                            } else {
                                logger.error("etl job for data process job " + processJobInstanceID + " is finished partially successfully");
                            }
                            reportJobStat();
                            ETLJobTracker.getETLJobTracker().removeJob(this);
                            break;
                        }
                    }
                }

                Thread.sleep(10000);
            } catch (Exception ex) {
                logger.warn("some error happened when doing etl task tracking" + ex.getMessage(), ex);
            }
        }
    }

    private void sendHttpRequest() {
    }

    public static void main(String[] args) {
        String dataProcessDescriptor = XMLReader.getXMLContent("simple-dataprocess-specific.xml");
        System.out.println(dataProcessDescriptor);
        ETLJob dataProcessJob = ETLJob.getETLJob(dataProcessDescriptor);
        System.out.println(dataProcessJob.getDataProcessDescriptor().get(ETLJob.DATA_ETL_DESC));
        System.out.println("parse ok");
    }
}
