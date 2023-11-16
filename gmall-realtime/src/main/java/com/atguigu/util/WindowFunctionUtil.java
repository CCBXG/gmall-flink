package com.atguigu.util;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.InvocationTargetException;

public class WindowFunctionUtil {

    public static <T> void setSttEdt(TimeWindow window, Iterable<T> values, Collector<T> out) throws InvocationTargetException, IllegalAccessException {
        T next = values.iterator().next();
        BeanUtils.setProperty(next, "stt", DateFormatUtil.toYmdHms(window.getStart()));
        BeanUtils.setProperty(next, "edt", DateFormatUtil.toYmdHms(window.getEnd()));
        out.collect(next);
    }

}
