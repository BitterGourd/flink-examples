package com.github.streaming.state.event;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 事件的数据类型，包括原始IP地址和事件类型
 */
public class Event {

    private final EventType type;

    private final int sourceAddress;

    public Event(EventType type, int sourceAddress) {
        this.type = checkNotNull(type);
        this.sourceAddress = sourceAddress;
    }

    public EventType type() {
        return type;
    }

    public int sourceAddress() {
        return sourceAddress;
    }

    @Override
    public int hashCode() {
        return 31 * type.hashCode() + sourceAddress;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        } else {
            final Event that = (Event) obj;
            return this.type == that.type && this.sourceAddress == that.sourceAddress;
        }
    }

    @Override
    public String toString() {
        return "Event" + formatAddress(sourceAddress) + " : " + type.name();
    }

    /**
     * 把 32 位整数表示的 IPV4 地址转换为字符串 (127.0.0.0形式)
     * @param address The address, MSB(最高有效位) first.
     * @return The IP address string.
     */
    public static String formatAddress(int address) {
        int b1 = (address >>> 24) & 0xff;
        int b2 = (address >>> 16) & 0xff;
        int b3 = (address >>>  8) & 0xff;
        int b4 =  address         & 0xff;

        return "" + b1 + '.' + b2 + '.' + b3 + '.' + b4;
    }
}
