/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.cc.slave.welcome;

import cn.ac.iie.cls.cc.slave.SlaveHandler;
import java.util.Date;

/**
 *
 * @author alexmu
 */
public class WelcomeHandler implements SlaveHandler {

    @Override
    public String execute(String pRequestContent) {
        return "welcome " + new Date();
    }
}
