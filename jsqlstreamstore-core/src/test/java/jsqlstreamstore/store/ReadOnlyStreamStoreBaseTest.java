package jsqlstreamstore.store;

import org.junit.jupiter.api.Test;

public class ReadOnlyStreamStoreBaseTest {

    @Test
    public void shouldThrowWhenDisposedAndReadOccurs() {
//        using(var fixture = GetFixture())
//        {
//            var store = await fixture.GetStreamStore();
//            store.Dispose();
//
//            Func<Task> act = () => store.ReadAllForwards(Position.Start, 10);
//
//            act.ShouldThrow<ObjectDisposedException>();
//        }
    }

    @Test
    public void canDisposeMoreThanOnce() {
//        using (var fixture = GetFixture())
//        {
//            var store = await fixture.GetStreamStore();
//            store.Dispose();
//
//            Action act = store.Dispose;
//
//            act.ShouldNotThrow();
//        }
    }


}
