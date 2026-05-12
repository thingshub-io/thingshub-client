package io.thingshub.client;

import java.lang.reflect.Type;

public interface ReplyHandler<T> {

	Type getType();

	void onSuccess(T data);

	default void onFailure(Throwable cause) {
	}

	default void onTimeout() {
	}

	default void onComplete() {
	}

}