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
    String taskStat;

    private ETLTask(String pFileName) {
        this(pFileName, EXECUTING);
    }

    public ETLTask(String pFileName, int pTaskStatus) {
        this(pFileName, pTaskStatus, "");
    }

    public ETLTask(String pFileName, int pTaskStatus, String pTaskStat) {
        filePath = pFileName;
        taskStatus = pTaskStatus;
        taskStat = pTaskStat;
    }
}
