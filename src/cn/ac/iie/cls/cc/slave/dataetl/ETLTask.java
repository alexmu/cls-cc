/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

/**
 *
 * @author alexmu
 */
public class ETLTask {

    public static final int EXECUTING = 0;
    public static final int SUCCEEDED = 1;
    public static final int FAILED = 2;
    String filePath;
    int taskStatus;

    public ETLTask(String pFileName) {
        filePath = pFileName;
        taskStatus = EXECUTING;
    }
}
