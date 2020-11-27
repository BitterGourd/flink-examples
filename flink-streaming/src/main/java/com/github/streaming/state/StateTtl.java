package com.github.streaming.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

/**
 * 配置状态有效期
 * 参考文档：https://www.bookstack.cn/read/flink-1.10-zh/33cc6a389b5c5c70.md#%E4%BD%BF%E7%94%A8%20Managed%20Keyed%20State
 */
public class StateTtl {

    public static void main(String[] args) {

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
        stateDescriptor.enableTimeToLive(ttlConfig);

    }
}
