/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.clsagent;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author alexmu
 */
public class CLSAgentReportHandler implements SlaveHandler {

    public String execute(String pRequestContent) {
        String xml = "";
        xml =pRequestContent;
        if(xml.equals("")){
            return "CLSAgentReportHandler's pRequestContent is empty!";
        }
        System.out.println(new Date().toString() + " xml:" + xml);

        //file which has been upload
        if (xml.startsWith("database")) {
            System.out.println("database: "+xml);
            String[] splitXml = xml.split("[|]");
            String task_id = "";
            task_id = splitXml[2];
            List<DataCollectTask> dctList = new ArrayList<DataCollectTask>();

            if (splitXml[1].equals("all")) {//all
                for (int i = 0; i < splitXml.length; i++) {
                    if (i > 4) {
                        DataCollectTask dct = new DataCollectTask(splitXml[i]);
                        dct.taskStatus = 0;
                        dctList.add(dct);
                        System.out.println("#######total: " + dct.fileName);
                    }

                }
                DataCollectJobTracker.getDataCollectJobTracker().appendTask(task_id, dctList);
            } else if (splitXml[1].equals("one")) {//one
                for (int i = 0; i < splitXml.length; i++) {
                    if (i > 4) {
                        DataCollectTask dct = new DataCollectTask(splitXml[i]);
                        dct.taskStatus = 1;
                        dctList.add(dct);
                        System.out.println("#######one: " + dct.fileName);
                    }
                }
                DataCollectJobTracker.getDataCollectJobTracker().responseTask(task_id, dctList);
            } else {
                ;
            }
        } else if(xml.startsWith("status")){
            System.out.println("status log : "+(xml.split("[|]"))[1]);
        }
        return "success!"+xml;
    }
}
