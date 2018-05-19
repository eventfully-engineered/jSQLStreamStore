package jsqlstreamstore;

public interface Handle<T> {

	void handle(T message);
}
