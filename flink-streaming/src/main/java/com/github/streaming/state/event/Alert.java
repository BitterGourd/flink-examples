package com.github.streaming.state.event;

import com.github.streaming.state.dfa.State;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Data type for alerts.
 */
public class Alert {

	private final int address;

	private final State state;

	private final EventType transition;

	/**
	 * Creates a new alert.
	 *
	 * @param address The originating address (think 32 bit IPv4 address).
	 * @param state The state that the event state machine found.
	 * @param transition The transition that was considered invalid.
	 */
	public Alert(int address, State state, EventType transition) {
		this.address = address;
		this.state = checkNotNull(state);
		this.transition = checkNotNull(transition);
	}

	// ------------------------------------------------------------------------

	public int address() {
		return address;
	}

	public State state() {
		return state;
	}

	public EventType transition() {
		return transition;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int code = 31 * address + state.hashCode();
		return 31 * code + transition.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		else {
			final Alert that = (Alert) obj;
			return this.address == that.address &&
					this.transition == that.transition &&
					this.state == that.state;
		}
	}

	@Override
	public String toString() {
		return "ALERT " + Event.formatAddress(address) + " : " + state.name() + " -> " + transition.name();
	}
}
