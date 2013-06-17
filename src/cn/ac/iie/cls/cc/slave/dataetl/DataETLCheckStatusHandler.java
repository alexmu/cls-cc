/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.dataetl;

import cn.ac.iie.cls.cc.slave.SlaveHandler;

/**
 *
 * @author alexmu
 */
public class DataETLCheckStatusHandler implements SlaveHandler {

    public String execute(String pRequestContent) {
        String result = null;

        result = pRequestContent != null && !pRequestContent.isEmpty() ? pRequestContent : this.toString();
        return result;
    }
}
