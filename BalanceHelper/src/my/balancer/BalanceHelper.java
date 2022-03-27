package my.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.util.URISupport.CompositeData;

public class BalanceHelper {
	
	private final static String JS_SEPARATOR = ";";
	private final static String SQ_SEPARATOR = "@";
	private final static String QU_SEPARATOR = ",";
	private final static String PORT_SEPARATOR = ":";

	
	public static void main(String[] args) {
		BalanceHelper b = new BalanceHelper();
		HashMap<String, List<String>> sqMap = b.retrieveJobserverQueuesMap();
		
		System.out.println("QUEUES ON SERVERS");
		Iterator<String> it = sqMap.keySet().iterator();
		while (it.hasNext()) {
			String server = it.next();
			System.out.println("\t" + server);
			List<String> queues = (List<String>) sqMap.get(server);
			for (Object q : queues) {
				System.out.println("\t\t" + (String) q);
			}
		}

		// retrieve current jobserver-workload-map
		System.out.println("\nMEMORY_USAGE ON SERVERS");
		HashMap<String,HashMap<String,Long>> wlMap = b.retrieveJobserverWorkloadMap(sqMap);
		Iterator<Map.Entry<String,HashMap<String,Long>>> itWl = wlMap.entrySet().iterator();
		while (itWl.hasNext()) {
			Map.Entry<String,HashMap<String,Long>> entry = itWl.next();
			System.out.println("\t" + entry.getKey() + " -> " + entry.getValue().get("UsedMemory") + " | " + entry.getValue().get("CommittedMemory"));
		}
		
//		System.out.println("\nQUEUES WORKLOAD");
//		Iterator<String> it2 = sqMap.keySet().iterator();
//		while (it2.hasNext()) {
//			String server = it2.next();
//			System.out.println("\t" + server);
//			List<String> queues = (List<String>) sqMap.get(server);
//			HashMap<String,Long> qwlMap = b.retrieveQueueWorkload(server, queues);
//			Iterator<Map.Entry<String,Long>> itQWl = qwlMap.entrySet().iterator();
//			while (itQWl.hasNext()) {
//				Map.Entry<String,Long> entry = itQWl.next();
//				System.out.println("\t\t" + entry.getKey() + " -> " + entry.getValue());
//			}
//		}
	}
	
	
	private HashMap<String, List<String>> retrieveJobserverQueuesMap() {
		String strJsqueues = null; //configParams.getJobserverQueues();
		if (strJsqueues == null) {
			strJsqueues = "172.30.2.147:8889@GOC.EXTRACTION_FAKE1,GOC.EXTRACTION_FAKE2;172.30.2.148:8888@GOC.EXTRACTION_FAKE3,GOC.EXTRACTION_FAKE4";
		}
//		LOG.warn("QUEUES = " + strJsqueues);
		
		HashMap<String,List<String>> map = new HashMap<String,List<String>>();
		for (String js : strJsqueues.split(JS_SEPARATOR)) {
			String server = js.split(SQ_SEPARATOR)[0];
			
			ArrayList<String> list = new ArrayList<String>(); 
			for (String q : js.split(SQ_SEPARATOR)[1].split(QU_SEPARATOR)) {
				list.add(q);
			}
			
			map.put(server, list);
		}
		return map;
	}
	

	private HashMap<String,HashMap<String, Long>> retrieveJobserverWorkloadMap(HashMap<String,List<String>> sqMap) {
		HashMap<String,HashMap<String, Long>> workloadMap = new HashMap<String,HashMap<String, Long>>();
		
		HashMap<String,?> serverMap = sqMap;
		Iterator<String> it = serverMap.keySet().iterator();
		while (it.hasNext()) {
			String server = it.next();
			HashMap<String, Long> workload = retrieveServerWorkload(server);
			workloadMap.put(server, workload);
		}
		
		return workloadMap;
	}


	private HashMap<String, Long> retrieveServerWorkload(String server) {
		HashMap<String, Long> workloadMap = null;
		MemoryMXBean mbean = null;
		
		try {
			String host = server.split(PORT_SEPARATOR)[0];
			String port = server.split(PORT_SEPARATOR)[1];
			JMXServiceURL url = new JMXServiceURL("jmxmp", host, Integer.valueOf(port).intValue());
			
			JMXConnector c = JMXConnectorFactory.connect(url, null);	// add configuration map with user etc. on demand
			c.connect();
			
			MBeanServerConnection mbsc = c.getMBeanServerConnection();
			mbean = ManagementFactory.newPlatformMXBeanProxy(mbsc,ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
		} 
		catch (Exception e) {
//			LOG.error(e.getMessage());
			System.out.println(e.getMessage());
		}
		
		if (mbean != null) {
			workloadMap = new HashMap<String, Long>();
			workloadMap.put("UsedMemory", mbean.getHeapMemoryUsage().getUsed());
			workloadMap.put("CommittedMemory", mbean.getHeapMemoryUsage().getCommitted());
		}
		
		return workloadMap;
	}
	
	
	private HashMap<String, Long> retrieveQueueWorkload(String server, List<String> queues) {
		HashMap<String,Long> workloadMap = new HashMap<String,Long>();
		MemoryMXBean mbean = null;
		
		try {
			String host = server.split(PORT_SEPARATOR)[0];
			String port = server.split(PORT_SEPARATOR)[1];
			JMXServiceURL url = new JMXServiceURL("jmxmp", host, Integer.valueOf(port).intValue());
			
			JMXConnector c = JMXConnectorFactory.connect(url, null);	// add configuration map with user etc. on demand
			c.connect();
			
//			MBeanServerConnection mbsc = c.getMBeanServerConnection();
//			mbean = ManagementFactory.newPlatformMXBeanProxy(mbsc,ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
			
			
			MBeanServerConnection mbsc = c.getMBeanServerConnection();
			for (String q : queues) {
				ObjectName nameConsumers = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + host + ",destinationType=Queue,destinationName=" + q);
				DestinationViewMBean mbView = MBeanServerInvocationHandler.newProxyInstance(mbsc, nameConsumers, DestinationViewMBean.class, true);
				workloadMap.put(q, mbView.getQueueSize());
			}

		} 
		catch (Exception e) {
//			LOG.error(e.getMessage());
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		
		return workloadMap;
	}
}
