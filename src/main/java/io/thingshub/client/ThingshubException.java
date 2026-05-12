package io.thingshub.client;

public class ThingshubException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ThingshubException() {
		super();
	}

	public ThingshubException(String msg) {
		super(msg);
	}

	public ThingshubException(Throwable cause) {
		super(cause);
	}

	public ThingshubException(String msg, Throwable cause) {
		super(msg, cause);
	}

}