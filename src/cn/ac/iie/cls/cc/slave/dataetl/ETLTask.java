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

    private ETLTask(String pFileName) {
        this(pFileName, EXECUTING);
    }

    public ETLTask(String pFileName, int pTaskStatus) {
        filePath = pFileName;
        taskStatus = pTaskStatus;
    }
}
