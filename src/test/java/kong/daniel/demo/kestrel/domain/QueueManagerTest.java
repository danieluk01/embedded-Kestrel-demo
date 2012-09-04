package kong.daniel.demo.kestrel.domain;

import kong.daniel.demo.kestrel.domain.QueueManager;

import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class QueueManagerTest {

	private static final String MESSAGE_ITEM = "MESSAGE_ITEM";

	@Test
	public void shouldSendMessageToQueueAndRetriveIt(){
		
		final QueueManager manager = new QueueManager();
		
		//send first text message
		boolean succeed = manager.send(MESSAGE_ITEM);
		assertThat(succeed, is(true));
		
		
		String check = manager.peek();
		assertThat(check, is(MESSAGE_ITEM));
		check = manager.retrieve();
		assertThat(check, is(MESSAGE_ITEM));
		
	}
		
}
