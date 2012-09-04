package kong.daniel.demo.kestrel.main;

import kong.daniel.demo.kestrel.domain.QueueManager;

public class Demo {

	public static void main(String[] args) throws Exception{
		QueueManager queueManager = new QueueManager();
		produceMsg(queueManager);
		Thread.sleep(1000);
		consumeMsg(queueManager);
	}

	private static void produceMsg(final QueueManager queueManager) {
		for (int i = 0; i < 5; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					for (int j = 0; j < 3; j++) {
						queueManager.send("MSG-"
								+ Thread.currentThread().getName() + "-" + j);
					}
				}
			}).start();
		}
	}

	private static void consumeMsg(final QueueManager queueManager) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i < 15; i++) {
					System.out.println("peeked: "+queueManager.peek());
					System.out.println("retrieved: "+queueManager.retrieve());
				}
			}
		}).start();
	}

}
