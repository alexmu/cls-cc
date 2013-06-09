/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slavecc.dataetl;

/**
 *
 * @author alexmu
 */
public class ETLTask {

    public static final int EXECUTING = 0;
    public static final int SUCCEEDED = 1;
    public static final int FAILED = 2;
    String fileName;
    int taskStatus;

    public ETLTask(String pFileName) {
        fileName = pFileName;
        taskStatus = EXECUTING;
    }
}
