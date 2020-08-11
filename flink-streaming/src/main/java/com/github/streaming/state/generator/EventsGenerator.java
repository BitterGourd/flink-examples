package com.github.streaming.state.generator;

import com.github.streaming.state.dfa.EventTypeAndState;
import com.github.streaming.state.dfa.State;
import com.github.streaming.state.event.Event;
import com.github.streaming.state.event.EventType;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 事件生成器
 * 成器在内部维护一系列状态机（地址和当前关联的状态），并从这些状态机返回转换事件
 * 每当下一个事件是生成器时，该生成器都会选择一个随机状态机并在该状态机上创建一个随机转换
 *
 * 生成器随机添加新的状态机，并在达到终端状态后立即删除它们
 * 此实现可同时维护多达1000个状态机
 */
public class EventsGenerator {

    private final Random rnd;

    /** The currently active state machines. */
    private final LinkedHashMap<Integer, State> states;

    /** Probability with this generator generates an illegal state transition. */
    private final double errorProb;

    public EventsGenerator() {
        this(0.0);
    }

    public EventsGenerator(double errorProb) {
        checkArgument(errorProb >= 0.0 && errorProb <= 1.0, "Invalid error probability");
        this.errorProb = errorProb;

        this.rnd = new Random();
        this.states = new LinkedHashMap<>();
    }

    /**
     * Creates a new random event. This method randomly pick either
     * one of its currently running state machines, or start a new state machine for
     * a random IP address.
     *
     * <p>With {@link #errorProb} probability, the generated event will be from an illegal state
     * transition of one of the currently running state machines.
     *
     * @param minIp The lower bound for the range from which a new IP address may be picked.
     * @param maxIp The upper bound for the range from which a new IP address may be picked.
     * @return A next random event.
     */
    public Event next(int minIp, int maxIp) {
        final double p = rnd.nextDouble();

        if (p * 1000 >= states.size()) {
            // create a new state machine
            final int nextIP = rnd.nextInt(maxIp - minIp) + minIp;

            if (!states.containsKey(nextIP)) {
                EventTypeAndState eventAndState = State.Initial.randomTransition(rnd);
                states.put(nextIP, eventAndState.state);
                return new Event(eventAndState.eventType, nextIP);
            } else {
                // collision on IP address, try again
                return next(minIp, maxIp);
            }
        } else {
            // pick an existing state machine

            // skip over some elements in the linked map, then take the next
            // update it, and insert it at the end

            int numToSkip = Math.min(20, rnd.nextInt(states.size()));
            Iterator<Map.Entry<Integer, State>> iter = states.entrySet().iterator();

            for (int i = numToSkip; i > 0; --i) {
                iter.next();
            }

            Map.Entry<Integer, State> entry = iter.next();
            State currentState = entry.getValue();
            int address = entry.getKey();

            iter.remove();

            if (p < errorProb) {
                EventType event = currentState.randomInvalidTransition(rnd);
                return new Event(event, address);
            } else {
                EventTypeAndState eventAndState = currentState.randomTransition(rnd);
                if (!eventAndState.state.isTerminal()) {
                    // reinsert
                    states.put(address, eventAndState.state);
                }

                return new Event(eventAndState.eventType, address);
            }
        }
    }
    /**
     * Creates an event for an illegal state transition of one of the internal
     * state machines. If the generator has not yet started any state machines
     * (for example, because no call to {@link #next(int, int)} was made, yet), this
     * will return null.
     *
     * @return An event for a illegal state transition, or null, if not possible.
     */
    @Nullable
    public Event nextInvalid() {
        final Iterator<Map.Entry<Integer, State>> iter = states.entrySet().iterator();
        if (iter.hasNext()) {
            final Map.Entry<Integer, State> entry = iter.next();

            State currentState = entry.getValue();
            int address = entry.getKey();
            iter.remove();

            EventType event = currentState.randomInvalidTransition(rnd);
            return new Event(event, address);
        } else {
            return null;
        }
    }

    public int numActiveEntries() {
        return states.size();
    }
}
