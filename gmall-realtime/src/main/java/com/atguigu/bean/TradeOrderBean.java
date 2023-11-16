package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Desc: 交易域下单实体类
 */
@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    String curDate;
    // 下单独立用户数
    Long orderUniqueUserCount;
    // 下单新用户数
    Long orderNewUserCount;
}

