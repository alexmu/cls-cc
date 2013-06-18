/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.test;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJob;
import cn.ac.iie.cls.cc.slave.clsagent.DataCollectJobTracker;

/**
 *
 * @author alexmu
 */
public class DataCollectJobExecTestHandler implements SlaveHandler {

    @Override
    public String execute(String pRequestContent) {
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
        return "ok";
    }
}
