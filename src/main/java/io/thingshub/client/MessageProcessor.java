package io.thingshub.client;

public interface MessageProcessor<T> {

	String getMessageName();

	void process(String sn, String messageId, T payload);

	default void onError(String sn, Integer code, String error) {

	};

}