package com.cobub.es.utils;

/**
 * Created by feng.wei on 2015/12/16.
 */
public final class StringUtil {

    public static boolean isEmpty(String param) {
        boolean flag = true;
        if (null != param && param.trim().length() > 0) {
            flag = false;
        }
        return flag;
    }

}
