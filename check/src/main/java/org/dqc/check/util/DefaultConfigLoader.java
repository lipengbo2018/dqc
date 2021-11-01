package org.dqc.check.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DefaultConfigLoader {

    Utils utils;
    public Properties properties = new Properties();

    public DefaultConfigLoader(String propertiesFilePath) throws IOException {

        BufferedInputStream in = new BufferedInputStream(new FileInputStream(propertiesFilePath));
        properties.load(in);
        this.utils = this.getUtils();
    }

    public Utils getUtils() {
        Utils utils = Utils.getInstance(this.properties);
        return utils;
    }

}
