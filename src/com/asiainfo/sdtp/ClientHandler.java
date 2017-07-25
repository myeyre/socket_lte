package com.asiainfo.sdtp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ClientHandler implements Runnable {
	private Socket socket = null;
	private DataInputStream dis = null;
	private DataOutputStream out = null;
	private int sendDataInfo=0;
	private int sendflag=0;
	
	public ClientHandler(Socket socket) {
		try {
			this.socket = socket;
			this.dis = new DataInputStream(socket.getInputStream());
			this.out = new DataOutputStream(socket.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start_time=System.currentTimeMillis();
		long lastsendtime=System.currentTimeMillis();
		System.out.println("startTime=" + format.format(new Date()));
		if ((verNego() == 1) && (linkAuth() == 1) && (linkCheck() == 1)) {
			int sequenceId = 1;
			while ((sequenceId <= 100000) && (notifyEventData(sequenceId) == 1)) {
				sequenceId++;
				sendDataInfo++;
				if (sequenceId % 10000 == 0) {
					System.out.println("sequenceId=" + sequenceId);
				}
				if((System.currentTimeMillis()-lastsendtime)>=5000){
					byte result=responseLinkDataCheck(sendflag++,sendDataInfo);
					System.out.println("responseLinkDataCheck=" +result);
					sendDataInfo=0;
					lastsendtime=System.currentTimeMillis();
				}
			}
		}
		linkRel();
		close();
		System.out.println("socket.isClosed=" +socket.isClosed());
		System.out.println("socket.isConnected=" +socket.isConnected());
		System.out.println("endTime=" + format.format(new Date()));
		System.out.println("耗时"+(System.currentTimeMillis()-start_time)/1000+"s");
	}

	public byte verNego() {
		try {
			short totalLength = 11;
			short messageType = 1;
			int sequenceId = 1;
			byte totalContents = 1;
			byte version = 1;
			byte subVersion = 0;

			byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
			byte[] messageTypeArray = ConvToByte.shortToByte(messageType);
			byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

			byte[] requestArray = new byte[totalLength];
			int pos = 0;
			System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(messageTypeArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
			pos += 4;
			requestArray[pos] = totalContents;
			pos++;
			requestArray[pos] = version;
			pos++;
			requestArray[pos] = subVersion;

			this.out.write(requestArray);
			this.out.flush();
			short rsp_totalLength = this.dis.readShort();
			System.out.println("rsp_totalLength="+rsp_totalLength);
			int rsp_messageType = this.dis.readUnsignedShort();
			System.out.println("rsp_messageType="+rsp_messageType);
			int rsp_sequenceId = this.dis.readInt();
			System.out.println("rsp_sequenceId="+rsp_sequenceId);
			byte rsp_totalContents = this.dis.readByte();
			System.out.format("rsp_totalContents=%02X",rsp_totalContents);
			return this.dis.readByte();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public byte linkAuth() {
		try {
			short totalLength = 43;
			short messageType = 2;
			int sequenceId = 2;
			byte totalContents = 1;
			String loginId = "asiainfo1234";
			String passwd = "asiainfo123";
			int timestamp = 123456789;
			short rand = 12345;
			byte[] tmp=new byte[50];
			byte[] digestArray=new byte[64] ;
			
			byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
			byte[] messageTypeArray = ConvToByte.shortToByte(messageType);
			byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);
			byte[] loginIdArray = loginId.getBytes();
			
			byte[] timestampArray = ConvToByte.intToByte(timestamp);
			byte[] randArray = ConvToByte.shortToByte(rand);

			int pos = 0;
			//获取sha256编码器
			MessageDigest md = MessageDigest.getInstance("MD5");
			//计算密码对应的sha256编码值
			byte[] sha_passwd=md.digest(passwd.getBytes());
			//拼接临时字段，以计算digest，和客户端发送的值比对（Digest=SHA256(LoginID+SHA256(Shared secret)+Timestamp+"rand=”+RAND)）
//			System.arraycopy(loginIdArray, 0, tmp, pos, 12);
//			pos +=12;
//			System.arraycopy(sha_passwd, 0, tmp, pos, 32);
//			pos +=32;
//			System.arraycopy(timestampArray, 0, tmp, pos, 4);
//			pos +=4;
//			System.arraycopy(randArray, 0, tmp, pos, 2);
//			//计算digest
//			byte[] digest=md.digest(tmp);
//			System.arraycopy(digest, 0, digestArray, 0, 32);
//			for(int i=32;i<64;i++){
//				digestArray[i]=(byte)0;
//			}
			StringBuilder sb=new StringBuilder();
			for (byte b:sha_passwd)
				sb.append(String.format("%02x", b));
			String str_tmp=loginId+sb.toString()+timestamp+"rand="+rand;
			byte[] digest=md.digest(str_tmp.getBytes());
			
			byte[] requestArray = new byte[totalLength];
			pos = 0;
			System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(messageTypeArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
			pos += 4;
			requestArray[pos] = totalContents;
			pos++;
			System.arraycopy(loginIdArray, 0, requestArray, pos, 12);
			pos += 12;
			System.arraycopy(digest, 0, requestArray, pos, 16);
			pos += 16;
			System.arraycopy(timestampArray, 0, requestArray, pos, 4);
			pos += 4;
			System.arraycopy(randArray, 0, requestArray, pos, 2);

			this.out.write(requestArray);
			this.out.flush();
//			for(int i=0;i<totalLength;i++)
//				System.out.format("%02X",requestArray[i]);
			short rsp_totalLength = this.dis.readShort();
			System.out.println("rsp_totalLength="+rsp_totalLength);
			int rsp_messageType = this.dis.readUnsignedShort();
			System.out.println("rsp_messageType="+rsp_messageType);
			int rsp_sequenceId = this.dis.readInt();
			System.out.println("rsp_sequenceId="+rsp_sequenceId);
			byte rsp_totalContents = this.dis.readByte();
			System.out.format("rsp_totalContents=%02X\n",rsp_totalContents);
			byte rsp_result = this.dis.readByte();

			byte[] rsp_digestArray = new byte[64];
			this.dis.read(rsp_digestArray);
//			String rsp_digest = new String(rsp_digestArray);
			sb.delete(0, sb.length());
//			for (byte b:sha_passwd)
//				sb.append(String.format("%02x", b));
//			System.out.println(sb.toString());
			return rsp_result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public byte linkCheck() {
		try {
			short totalLength = 9;
			short messageType = 3;
			int sequenceId = 3;
			byte totalContents = 1;

			byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
			byte[] messageTypeArray = ConvToByte.shortToByte(messageType);
			byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

			byte[] requestArray = new byte[totalLength];
			int pos = 0;
			System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(messageTypeArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
			pos += 4;
			requestArray[pos] = totalContents;

			this.out.write(requestArray);
			this.out.flush();

			short rsp_totalLength = this.dis.readShort();
			System.out.println("rsp_totalLength="+rsp_totalLength);
			int rsp_messageType = this.dis.readUnsignedShort();
			System.out.println("rsp_messageType="+rsp_messageType);
			int rsp_sequenceId = this.dis.readInt();
			System.out.println("rsp_sequenceId="+rsp_sequenceId);
			byte rsp_totalContents = this.dis.readByte();

			return 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public byte linkRel() {
		try {
			short totalLength = 10;
			short messageType = 4;
			int sequenceId = 4;
			byte totalContents = 1;
			byte reason = 1;

			byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
			byte[] messageTypeArray = ConvToByte.shortToByte(messageType);
			byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

			byte[] requestArray = new byte[totalLength];
			int pos = 0;
			System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(messageTypeArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
			pos += 4;
			requestArray[pos] = totalContents;
			pos++;
			requestArray[pos] = reason;

			this.out.write(requestArray);
			this.out.flush();

			int rsp_totalLength = this.dis.readUnsignedShort();
			System.out.println("rsp_totalLength="+rsp_totalLength);
			int rsp_messageType = this.dis.readUnsignedShort();
			System.out.println("rsp_messageType="+rsp_messageType);
			int rsp_sequenceId = this.dis.readInt();
			System.out.println("rsp_sequenceId="+rsp_sequenceId);
			byte rsp_totalContents = this.dis.readByte();
			System.out.println("rsp_totalContents="+rsp_totalContents);
			return this.dis.readByte();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public byte notifyEventData(int sequenceId) {
		try {
			short messageType = 5;

			byte totalContents = 1;
			short mobileEventType = 3;
			long orderId = 1234567890L;
			
			int[] topicArray=new int[]{100,101,102,103,104,105,106,107,108,109,0,1};
			double a=Math.random()*11;
			int index=(int)Math.round(a);
			String msg = "307|010|11|ff770acf0187a1030025c99000007f54|6|460075054677155|8645100291436300|17862129254fffffffffffffffffffff|1|100.70.255.86|100.70.224.119|2152|2152|1662317957|2149874669|0|94869261|CMNET.mnc007.mcc460.gprs|"+topicArray[index]+"|1433128418556|1433128454999|2|FFFFFFFFF|3110|255|2|10.106.76.214|FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF|49522|255|117.135.128.149|FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF|80|1254|595|6|5|0|0|0|0|18|42|0|0|20|22|11|1352|1|1|1\r\n";
			
			String as="0200CB503305FEFF4801020AA370440028123E0200270664005235939389F068948020020237878163346604F2FFFFFFFFFFFFFFFFFFFF140000014F2B94D60E0000014F2B94D638000014FFFF00FFFFFF00C13C72FFFFFF63383135FFFF8CC8150F29FFFFFFFF0A444B2AFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF6446FF31FFFFFFFFFFFFFFFFFFFFFFFF644670288E3C8E3C633B059B7102FFFFFFFFFFFF636D6E65742E6D6E633030322E6D63633436302E67707273000000000000000000";
			byte[] buffer=new byte[as.length()/2];
			StringBuilder sb=new StringBuilder();
			
			for(int i=0;i<as.length()/2;i++){
				int pos=2*i;
				buffer[i] = (byte) (charToByte(as.charAt(pos)) << 4 | charToByte(as.charAt(pos+1)));
			}
			
			short totalLength = (short) (msg.length() + 9);

			byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
			byte[] messageTypeArray = ConvToByte.shortToByte(messageType);
			byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);
//			byte[] mobileEventTypeArray = ConvToByte.shortToByte(mobileEventType);
//			byte[] orderIdArray = ConvToByte.longToByte(orderId);
			byte[] msgArray = msg.getBytes();

			byte[] requestArray = new byte[totalLength];
			int pos = 0;
			System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(messageTypeArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
			pos += 4;
			requestArray[pos] = totalContents;
			pos++;
//			System.arraycopy(mobileEventTypeArray, 0, requestArray, pos, 2);
//			pos += 2;
//			System.arraycopy(orderIdArray, 0, requestArray, pos, 8);
//			pos += 8;
//			System.arraycopy(msgArray, 0, requestArray, pos, msgArray.length);
			System.arraycopy(buffer, 0, requestArray, pos, buffer.length);

			this.out.write(requestArray);

			int rsp_totalLength = this.dis.readUnsignedShort();
//			System.out.println("rsp_totalLength="+rsp_totalLength);
			int rsp_messageType = this.dis.readUnsignedShort();
//			System.out.println("rsp_messageType="+rsp_messageType);
			int rsp_sequenceId = this.dis.readInt();
//			System.out.println("rsp_sequenceId="+rsp_sequenceId);
			byte rsp_totalContents = this.dis.readByte();
			byte rsp_result= this.dis.readByte();
//			System.out.println("rsp_result="+rsp_result);
			return 1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	 private static byte charToByte(char c) {   
		   return (byte) "0123456789ABCDEF".indexOf(c);   
	 } 
	public byte responseLinkDataCheck(int sendflag,int sendDataInfo) {
		try {
			short totalLength = 17;
			short messageType = 7;
			int sequenceId = 5;
			byte totalContents = 1;
	

			byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
			byte[] messageTypeArray = ConvToByte.shortToByte(messageType);
			byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);
			byte[] sendflagArray = ConvToByte.intToByte(sendflag);
			byte[] sendDataInfoArray = ConvToByte.intToByte(sendDataInfo);

			byte[] requestArray = new byte[totalLength];
			int pos = 0;
			System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(messageTypeArray, 0, requestArray, pos, 2);
			pos += 2;
			System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
			pos += 4;
			requestArray[pos] = totalContents;
			pos++;
			System.arraycopy(sendflagArray, 0, requestArray, pos, 4);
			pos += 4;
			System.arraycopy(sendDataInfoArray, 0, requestArray, pos, 4);
			this.out.write(requestArray);
			this.out.flush();

			int rsp_totalLength = this.dis.readUnsignedShort();
			System.out.println("rsp_totalLength="+rsp_totalLength);
			int rsp_messageType = this.dis.readUnsignedShort();
			System.out.println("rsp_messageType="+rsp_messageType);
			int rsp_sequenceId = this.dis.readInt();
			System.out.println("rsp_sequenceId="+rsp_sequenceId);
			byte rsp_totalContents = this.dis.readByte();
			System.out.println("rsp_totalContents="+rsp_totalContents);
			int rsp_sendflag = this.dis.readInt();
			System.out.println("rsp_sendflag="+rsp_sendflag);
			byte rsp_result = this.dis.readByte();
			System.out.println("rsp_result="+rsp_result);
			int rsp_senddatainfo = this.dis.readInt();
			System.out.println("rsp_senddatainfo="+rsp_senddatainfo);
			int rsp_receive = this.dis.readInt();
			System.out.println("rsp_receive="+rsp_receive);
			return rsp_result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	private void close(){
		try {
			System.out.println("关闭连接！服务器："+this.socket.getInetAddress().getHostAddress());
			this.socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
