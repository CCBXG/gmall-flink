package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: 用户域用户登录实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    String curDate;
    // 回流用户数
    Long backCt;
    // 独立用户数
    Long uuCt;
}
