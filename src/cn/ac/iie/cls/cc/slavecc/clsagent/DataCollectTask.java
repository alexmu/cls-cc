/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.clsagent;

/**
 *
 * @author alexmu
 */
public class DataCollectTask {

    public static final int EXECUTING = 0;
    public static final int SUCCEEDED = 1;
    public static final int FAILED = 2;
    String fileName;
    int taskStatus;

    public DataCollectTask(String pFileName) {
        fileName = pFileName;
        taskStatus = EXECUTING;
    }
}
