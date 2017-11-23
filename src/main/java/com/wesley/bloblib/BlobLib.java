package com.wesley.bloblib;

import java.util.ArrayList;
import java.util.List;

import org.pmw.tinylog.Logger;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.microsoft.azure.storage.file.ShareListingDetails;

public class BlobLib {

	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		try {
			//BfsPath bfsPath = new BfsPath("/index.html");
			//PathProperties pathProperties = bfsPath.getBfsPathProperties();
//			MsgsToBeSentCache.getInstance().startAutoSendService();
//			MessageProcessor.getInstance().startDbCacheAutoCleanupService();
//			String finalMsg = MessageService.getInstance().buildNewMsgToBeSent("blobfs/music.log");
//			MsgsToBeSentCache.getInstance().put(UUID.randomUUID().toString(), finalMsg);
			//String xMsg = MessageService.getInstance().buildProcessedMsgToBeSent(finalMsg);
			//MsgsToBeSentCache.getInstance().put(UUID.randomUUID().toString(), xMsg);
//			 BlobReqParams reqParams = new BlobReqParams();
//             reqParams.setVirtualDirOptMode(VirtualDirOptMode.HIERARCHICAL);
//             reqParams.setVirtualDirOptMode(VirtualDirOptMode.HBLOBPROPS);
//             reqParams.setContainer("music2017");
//             reqParams.setBlob("");
//             java.util.List bfsBlobModels = BlobService.getBlobsWithinVirtualDirectory(reqParams);
//             java.util.Iterator itr = bfsBlobModels.iterator();
//             while (itr.hasNext())
//             {
//                 BfsBlobModel bfsBlobModel = (BfsBlobModel)itr.next();
//                 System.out.println(bfsBlobModel.getBlobName());
//             } 
//			OpenedFilesManager openedFilesManager = OpenedFilesManager.getInstance();
//			//openedFilesManager.startLeaseAutoRenewService();
//			BlobReqParams insParams = new BlobReqParams();
//            insParams.setContainer("music2017");
//            insParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
//            insParams.setBlob("blobfs-2017-05-26.0.log");
//            BlobBufferedIns bbIns = new BlobBufferedIns(insParams);
//            BlobReqParams ousParams = new BlobReqParams();
//            ousParams.setContainer("music2017");
//            ousParams.setBlob("music2.log");
//            /* get the blob type */
//            ousParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
//            BlobBufferedOus bbOus = new BlobBufferedOus(ousParams);
//            OpenedFileModel ofe = new OpenedFileModel(bbIns, bbOus);
//            ofe.setLeaseID(bbOus.getLeaseID());
//            ofe.setContainer("music2017");
//			ofe.setBlob("music2.log");
//            openedFilesManager.put(1L, ofe);
//            byte[] bytesReaded = new byte[1024];
//            int readResult = 0;
//            String line;
//            while ((line = bbIns.readLine()) != null)
//            {
//                System.out.println((line));
//                bbOus.writeLine(line);
//                Thread.sleep(100);
//            }
//            bbOus.close();
			/* get the blobs within the virtual directory with lazy mode */
//            BlobReqParams getBlobsReq = new BlobReqParams();
//            getBlobsReq.setContainer("music2017");
//            getBlobsReq.setBlob("");
//            ///* use flat mode here, there is no virtual directory in this mode 
//            // * safe copy/move , delete the original file only if copy/move successed*/
//            getBlobsReq.setVirtualDirOptMode(VirtualDirOptMode.HIERARCHICAL);
//            getBlobsReq.setVirtualDirOptMode(VirtualDirOptMode.HBLOBPROPS);
//            @SuppressWarnings("rawtypes")
//			List<BfsBlobModel> bfsBlobModels = new ArrayList<BfsBlobModel>();
//            long startTime = System.currentTimeMillis();
//            System.out.println("start time - nocache:" + startTime);
//            bfsBlobModels = BlobService.getBlobsWithinVirtualDirectory(getBlobsReq);
//            //String dirName = BlobService.getVirtualDirectoryPath(getBlobsReq);
//            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++=");
////            @SuppressWarnings("rawtypes")
//			java.util.Iterator itr = bfsBlobModels.iterator();
//            //List blobNameList = new ArrayList<>();
//            while (itr.hasNext())
//            {
//                final BfsBlobModel bfsBlobModel = (BfsBlobModel)itr.next();
//                System.out.println(bfsBlobModel.getBlobName());
//            }
//            System.out.println("total time:" + (System.currentTimeMillis() - startTime));
//            startTime = System.currentTimeMillis();
//            System.out.println("start time - withcache:" + startTime);
//            bfsBlobModels = BlobService.getBlobsWithinVirtualDirectory(getBlobsReq);
//            while (itr.hasNext())
//            {
//                final BfsBlobModel bfsBlobModel = (BfsBlobModel)itr.next();
//                System.out.println(bfsBlobModel.getBlobName());
//            }
//            System.out.println("total time:" + (System.currentTimeMillis() - startTime));
//			BfsPath bfsPath = new BfsPath("/newtest/sport2018/新建文件夹/$.$$");
//            PathProperties pathPropeties = bfsPath.getBfsPathProperties();
//            BfsBlobType bfsBlobType = pathPropeties.getBfsBlobType();
//            BfsPath bfsPath = new BfsPath("newtest");
//            PathProperties pathProperties = bfsPath.getBfsPathProperties();
//			  BlobReqParams delDirParams = new BlobReqParams();
//	          delDirParams.setContainer("orders");
//	          delDirParams.setBlob("wx.txt");
//	          BlobService.deleteBlob(delDirParams);
//			  final CachedFilesInMemManager cachedFilesInMemManager = CachedFilesInMemManager.getInstance();
//			  String key = cachedFilesInMemManager.getTheFormattedKey(delDirParams.getContainer(), delDirParams.getBlob());
			  //cachedFilesInMemManager.delete(key);
//			  MessageService mService =  MessageService.getInstance();
//			  JsonObject msgJson = Json.object()
//						.add("file", "wx.txt")
//						.add("host", "12334")
//						.add("date", "34223");
//			  String finalMsg =  msgJson.toString();
//			  mService.sendMessage(finalMsg);
//            java.util.List failedFilesList = new java.util.ArrayList();
//            if (BlobService.deleteVirtualDirectory(failedFilesList, delDirParams))
//            {
//               System.out.println("DELETE SUCCESSED!");
//            }
			System.out.println("You called the main method!");
			while(true)
			{
				System.out.println("main thread!");
				Thread.sleep(100);
			}
		
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			Logger.error(ex.getMessage());
		}
	}

}
