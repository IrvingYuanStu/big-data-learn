package com.irving.logClean;

import lombok.Data;

/**
 * 日志对象信息类封装
 * @Author yuanyc
 * @Date 15:08 2020-02-17
 */
@Data
public class LogInfo {

    private String remoteAddr;
    private String timeStamp;
    private String request;
    private String status;
    private boolean isValid;

    @Override
    public String toString() {
        return "LogInfo{" +
                "remoteAddr='" + remoteAddr + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
