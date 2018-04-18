package org.junko.mysqlcdc;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.security.JaasUtils;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

public class KafkaTest {

	public void createToptic() {
		ZkUtils zkUtils = ZkUtils.apply("192.168.12.141:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 创建一个单分区单副本名为t1的topic
		AdminUtils.createTopic(zkUtils, "t2", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
		zkUtils.close();
	}

	public void getTopticInfo() {
		ZkUtils zkUtils = ZkUtils.apply("192.168.12.141:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 获取topic 'test'的topic属性属性
		Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "t1");
		// 查询topic-level属性
		Iterator it = props.entrySet().iterator();
		System.out.println(props.entrySet().size());
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			Object key = entry.getKey();
			Object value = entry.getValue();
			System.out.println(key + " = " + value);
		}
		zkUtils.close();
	}

	
	public void editToptic(){
		ZkUtils zkUtils = ZkUtils.apply("192.168.12.141:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
		// 增加topic级别属性
		props.put("min.cleanable.dirty.ratio", "0.3");
		// 删除topic级别属性
		props.remove("max.message.bytes");
		// 修改topic 'test'的属性
		AdminUtils.changeTopicConfig(zkUtils, "t1", props);
		zkUtils.close();
	}
	
	public void writeDataToKafka(){
		
	}
	public static void main(String[] args) {
		new KafkaTest().getTopticInfo();
	}

}
