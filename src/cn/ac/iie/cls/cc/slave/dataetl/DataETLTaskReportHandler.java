/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alexmu
 */
public class DataETLTaskReportHandler implements SlaveHandler {

    public String execute(String pRequestContent) {
        String result = null;

        System.out.println(pRequestContent);
        String[] reportItems = pRequestContent.split("[|]");
        String processJobInstanceId = reportItems[2];
        String filePath = reportItems[3];
        ETLTask etlTask = new ETLTask(filePath, ETLTask.SUCCEEDED,reportItems[4]);
        List etlTaskList = new ArrayList<ETLTask>();
        etlTaskList.add(etlTask);
        ETLJobTracker.getETLJobTracker().responseTask(processJobInstanceId, etlTaskList);
        return result;
    }
}
