package org.junko.mysqlcdc;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Hello world!
 *
 */
public class App  
{
	public void testBinary() throws InvalidProtocolBufferException{
		 CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.12.140",  
	                11111), "example", "", "");  
	        int batchSize = 1000;  
	        int emptyCount = 0;  
	        try {  
	            connector.connect();  
	            connector.subscribe(".*\\..*");  
	            connector.rollback();  
	            int totalEmtryCount = 1200;  
	            while (emptyCount < totalEmtryCount) {  
	                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据  
	                long batchId = message.getId();  
	                int size = message.getEntries().size();  
	                if (batchId == -1 || size == 0) {  
	                    emptyCount++;  
	                    System.out.println("empty count : " + emptyCount);  
	                    try {  
	                        Thread.sleep(1000);  
	                    } catch (InterruptedException e) {  
	                        e.printStackTrace();  
	                    }  
	                } else {  
	                    emptyCount = 0;  
	                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);  
	                    printEntry(message.getEntries());  
	                }  
	  
	                connector.ack(batchId); // 提交确认  
	                // connector.rollback(batchId); // 处理失败, 回滚数据  
	            }  
	  
	            System.out.println("empty too many times, exit");  
	        } finally {  
	            connector.disconnect();  
	        }  
	    }  
	  
	    private static void printEntry( List<Entry> entrys) {  
	        for (Entry entry : entrys) {  
	            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {  
	                continue;  
	            }  
	  
	            RowChange rowChage = null;  
	            try {  
	                rowChage = RowChange.parseFrom(entry.getStoreValue());  
	            } catch (Exception e) {  
	                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),  
	                        e);  
	            }  
	  
	            EventType eventType = rowChage.getEventType();  
	            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",  
	                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),  
	                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),  
	                    eventType));  
	  
	            for (RowData rowData : rowChage.getRowDatasList()) {  
	                if (eventType == EventType.DELETE) {  
	                    printColumn(rowData.getBeforeColumnsList());  
	                } else if (eventType == EventType.INSERT) {  
	                    printColumn(rowData.getAfterColumnsList());  
	                } else {  
	                    System.out.println("-------> before");  
	                    printColumn(rowData.getBeforeColumnsList());  
	                    System.out.println("-------> after");  
	                    printColumn(rowData.getAfterColumnsList());  
	                }  
	            }  
	        }  
	    }  
	  
	    private static void printColumn( List<Column> columns) {  
	        for (Column column : columns) {  
	            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());  
	        }  
		 
	}
	
    private void printEntries(List<Entry> entries) throws InvalidProtocolBufferException {
		// TODO Auto-generated method stub
    	for(Entry entry : entries){
    	RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
    	for (RowData rowData : rowChange.getRowDatasList()) {
    	    if (rowChange.getEventType() == EventType.INSERT) {
    	      printColumns(rowData.getAfterColumnsList());
    	    }
    	}
    	}
	}

	private void printColumns(List<Column> columns) {
		// TODO Auto-generated method stub
		String line = columns.stream()
		        .map(column -> column.getName() + "=" + column.getValue())
		        .collect(Collectors.joining(","));
		System.out.println(line);
	}

	public static void main( String[] args ) throws InvalidProtocolBufferException
    {
        new App().testBinary();
    }
}
