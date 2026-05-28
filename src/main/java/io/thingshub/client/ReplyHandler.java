package io.thingshub.client;

public interface ReplyHandler<T> {

	void onSuccess(T data);

	default void onError(Throwable cause) {
	}

	default void onTimeout() {
	}

	default void onComplete() {
	}

}