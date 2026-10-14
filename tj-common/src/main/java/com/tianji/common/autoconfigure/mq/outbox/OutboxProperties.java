package com.tianji.common.autoconfigure.mq.outbox;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "tj.mq.outbox")
public class OutboxProperties {
    private boolean pollEnabled = true;
    private int batchSize = 100;
    private long confirmTimeoutMillis = 5000;
    private long leaseSeconds = 30;
    private long retryBaseSeconds = 5;
    private long retryMaxSeconds = 300;
    private long maxBatchMillis = 30000;

    public void validate() {
        if (batchSize < 1 || batchSize > 1000 || confirmTimeoutMillis < 1 || confirmTimeoutMillis > 60000
                || leaseSeconds <= confirmTimeoutMillis / 1000 + 1
                || leaseSeconds > 86400 || maxBatchMillis < confirmTimeoutMillis
                || retryBaseSeconds < 1 || retryMaxSeconds < retryBaseSeconds || retryMaxSeconds > 86400) {
            throw new IllegalArgumentException("Outbox 批次、确认超时、租约或重试参数无效");
        }
    }
}
