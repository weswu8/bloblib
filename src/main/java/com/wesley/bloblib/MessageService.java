package com.wesley.bloblib;

import java.util.ArrayList;
import java.util.UUID;

import org.pmw.tinylog.Logger;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;


// Message used for synchronize the catch state across the nodes
/**
 * @author weswu
 *
 */
public class MessageService {
	private static MessageService instance;
	private static String queueName = com.wesley.bloblib.Configuration.QUEUE_NAME;
	private static String hostID = UUID.randomUUID().toString().replace("-", "").substring(0, 7);
	static CloudQueueClient queueClient;
	static CloudQueue queueRef;
	
	@SuppressWarnings("static-access")
	private MessageService() {
		try {
			queueClient = BlobClientService.getQueueClient();
			/* check the queue, create it if does not exit */
		    queueRef = queueClient.getQueueReference(queueName);
		    queueRef.createIfNotExists();			
		} catch (Exception ex) {
			Logger.error(ex.getMessage());
		}
	}
	public static MessageService getInstance(){
        if(instance == null){
            synchronized (MessageService.class) {
                if(instance == null){
                    instance = new MessageService();
                }
            }
        }
        return instance;
    }
	
	public static String buildProcessedMsgToBeSent(String msg){
		String finalMsg = null;
		JsonObject msgJson = Json.parse(msg).asObject();
		String hosts = msgJson.get("host").asString();
		if (!hosts.contains(hostID)){
			hosts = hosts + "|" + hostID;
		}
		msgJson.set("host", hosts);
		finalMsg =  msgJson.toString();
		return finalMsg;
	}
	
	/**
	 * @param fileName, should be the full path /container/blob
	 * @return
	 */
	public static String buildNewMsgToBeSent(String fileName){
		String finalMsg = null;
		JsonObject msgJson = Json.object()
				.add("file", fileName)
				.add("host", hostID)
				.add("date", String.valueOf(System.currentTimeMillis()));
		finalMsg =  msgJson.toString();
		return finalMsg;
	}
	
	public static boolean sendMessage(String msg) throws Exception{
		try {
			if (null == msg || "" == msg.trim()){return false;}
			CloudQueueMessage cloudQueueMessage = new CloudQueueMessage(msg);
			int msgTTL = Configuration.BFS_CACHE_TTL_MS/1000;
			queueRef.addMessage(cloudQueueMessage, msgTTL, 0, null, null);
		} catch (Exception ex) {
			Logger.error("Exception occurred when sending the message :{} , {} ", msg, ex.getMessage());
		}
		return true;
	}
	
	/**
	 * get the message from the shared queue
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<String> receiveMessages() throws Exception{
		ArrayList<String> msgs = new ArrayList<>();
		try {
			CloudQueueMessage receivedMessage = null;
			String msgContent = null;
			while((receivedMessage = queueRef.retrieveMessage(2, null /*options*/, null /*opContext*/)) != null)
			{	// get the message
				// this msg was processed already
				msgContent = receivedMessage.getMessageContentAsString();
				if (null == msgContent) { 
					queueRef.deleteMessage(receivedMessage);
					continue;
				}
				JsonObject mcObject = Json.parse(msgContent).asObject();
				if (mcObject.get("host").asString().contains(hostID)){
					// this is the msg sent by this node, do nothing
					continue;
				}
				// check if the message is expired, delete it if so
				long oTimeInMs = Long.parseLong(mcObject.get("date").asString());
				if (System.currentTimeMillis() - oTimeInMs > Configuration.BFS_CACHE_TTL_MS){
					queueRef.deleteMessage(receivedMessage);
					continue;					
				}
				// delete the original message
				queueRef.deleteMessage(receivedMessage);
				// add it into the msgs list to be returned
				msgs.add(msgContent);
			}
		} catch (Exception ex) {
			Logger.error("Exception occurred when receiving the message, {} ", ex.getMessage());
		}
		return msgs;
	}
}
