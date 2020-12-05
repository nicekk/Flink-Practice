package com.hudsun.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @Author wangkai
 * @Time 2020/12/5 15:40
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserNumber {

    private String user;
    private Long number;
}
