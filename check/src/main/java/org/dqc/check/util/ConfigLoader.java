package org.dqc.check.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ConfigLoader  {
    private String filePath="conf/settings.properties" ;

    public DefaultConfigLoader getConfigInfo()throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException {
        // write your code here
        Class c = Class.forName("org.dqc.check.util.DefaultConfigLoader");
        Constructor c1 = c.getDeclaredConstructor(String.class);
        c1.setAccessible(true);
        DefaultConfigLoader cfg = (DefaultConfigLoader) c1.newInstance(this.filePath);
        return  cfg ;
    }

}