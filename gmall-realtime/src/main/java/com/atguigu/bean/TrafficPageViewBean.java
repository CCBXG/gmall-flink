package com.atguigu.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficPageViewBean {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // app 版本号
    private String vc;
    // 渠道
    private String ch;
    // 地区
    private String ar;
    // 新老访客状态标记
    private String isNew;
    // 当天日期
    private String curDate;
    // 独立访客数
    private Long uvCt;
    // 会话数
    private Long svCt;
    // 页面浏览数
    private Long pvCt;
    // 累计访问时长
    private Long durSum;
    // 时间戳
    @JSONField(serialize = false) // 这个字段不需要序列化到json字符串中, 可以加这个注解
    private Long ts;
}
