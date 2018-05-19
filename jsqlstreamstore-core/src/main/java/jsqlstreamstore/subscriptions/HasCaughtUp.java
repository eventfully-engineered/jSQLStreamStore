package jsqlstreamstore.subscriptions;

/**
 * A delegate that is invoked when a subscription has either caught up or fallen behind.
 *
 */
@FunctionalInterface
public interface HasCaughtUp {

    void hasCaughtUp(boolean hasCaughUp);

}
