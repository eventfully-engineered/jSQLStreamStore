package com.eventfullyengineered.jsqlstreamstore;

public interface Handle<T> {

	void handle(T message);
}
