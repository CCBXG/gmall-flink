package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Desc: 用户加购实体类
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}

