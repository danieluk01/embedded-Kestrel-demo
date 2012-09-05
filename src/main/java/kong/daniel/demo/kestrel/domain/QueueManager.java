package kong.daniel.demo.kestrel.domain;

import java.io.File;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import net.lag.kestrel.PersistentQueue;
import net.lag.kestrel.config.QueueBuilder;
import net.lag.kestrel.config.QueueConfig;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import scala.Option;

import com.twitter.util.Duration;
import com.twitter.util.JavaTimer;
import com.twitter.util.StorageUnit;
import com.twitter.util.Timer;

public class QueueManager {
	
	private static PersistentQueue queue;
	private static String queueName;
	private static String queuePath;
	
	static{
		loadDefaultSettings();
		initDefaultQueue();
	}
	
	public QueueManager(){
		
	}
	
	public boolean send(String msg){
		return queue.add(msg.getBytes());
	}
	
	public String retrieve(){
		return new String(queue.remove().get().data());
	}
	
	public String peek(){
		return new String(queue.peek().get().data());
	}

	private static void initDefaultQueue() {
		
		//kestrel default settings
//		QueueConfig config = new QueueBuilder().apply();
		
		//customized kestrel settings
		QueueConfig config = getConfig();
		
		Timer timer = new JavaTimer();
		ScheduledExecutorService journalSyncScheduler = Executors.newScheduledThreadPool(10);

		//default queue
		try{
			queue = new PersistentQueue(queueName, queuePath, config, timer, journalSyncScheduler);
			queue.setup();
		}catch(Throwable cause){
			System.out.println(cause.getClass().getName()+": \n"+cause.getMessage());
			cause.printStackTrace(System.out);
		}
	}

	private static void loadDefaultSettings() {
		try {
			StringWriter writer = new StringWriter();
			IOUtils.copy(
					QueueManager.class.getClassLoader().getResourceAsStream(
							"default_queue.json"), writer, "UTF-8");
			String json =  writer.toString();
			Map<String, String> defaultSetting = new ObjectMapper().readValue(json, new TypeReference<Map<String, String>>() { });
			queueName = defaultSetting.get("defaultQueueName");
			queuePath = defaultSetting.get("defaultQueuePath");
			File dir = new File(queuePath);
			if(!dir.exists()){
				dir.mkdir();
			}
		} catch (Exception cause) {
			throw new RuntimeException(
					"Something wrong happened during loading the default queue settings!",
					cause);
		}
	}
	
	private static QueueConfig getConfig() {
		int maxItems = 10000;
		StorageUnit maxSize = new StorageUnit(1024*10000);
		StorageUnit maxItemSize = new StorageUnit(1024*100);
		
		//10 seconds
		Duration maxAge = new Duration(10*1000*1000*1000);	
		StorageUnit defaultJournalSize = new StorageUnit(1024*5000);
		StorageUnit maxMemorySize = new StorageUnit(1024*10000);
		StorageUnit maxJournalSize = new StorageUnit(1024*10000);
		Boolean discardOldWhenFull = true;
		Boolean keepJournal = true;
		
		//3 seconds
		Duration syncJournal = new Duration(3*1000*1000*1000);
		String expireToQueue = null;
		int maxExpireSweep = 5000;
		Boolean fanoutOnly = false;
		Duration maxQueueAge = null;
			
		QueueConfig config = new QueueConfig(maxItems, 
												maxSize, 
												maxItemSize, 
												Option.apply(maxAge), 
												defaultJournalSize, 
												maxMemorySize, 
												maxJournalSize, 
												discardOldWhenFull, 
												keepJournal, 
												syncJournal, 
												Option.apply(expireToQueue), 
												maxExpireSweep, 
												fanoutOnly, 
												Option.apply(maxQueueAge));
		return config;
	}

}
