package com.github.streaming.serialize;

import com.github.streaming.state.event.Event;
import com.github.streaming.state.event.EventType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A serializer and deserializer for the {@link Event} type.
 */
public class EventDeSerializer implements DeserializationSchema<Event>, SerializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    @Override
    public Event deserialize(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
        int address = buffer.getInt(0);
        int typeOrdinal = buffer.getInt(4);
        return new Event(EventType.values()[typeOrdinal], address);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Event element) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(0, element.sourceAddress());
        byteBuffer.putInt(4, element.type().ordinal());
        return byteBuffer.array();
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
