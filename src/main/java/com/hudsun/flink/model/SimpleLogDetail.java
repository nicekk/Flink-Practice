package com.hudsun.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 简单的日志明细类
 *
 * @Author wangkai
 * @Time 2020/12/5 13:10
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SimpleLogDetail {

    public String msg;
    public Integer cnt;
    public long timestamp;
}
