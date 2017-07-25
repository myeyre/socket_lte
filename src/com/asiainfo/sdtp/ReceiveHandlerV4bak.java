package com.asiainfo.sdtp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ReceiveHandlerV4bak extends Thread {
	private Socket socket = null;
	private String interface_type = "";
	private Producer<String, String> producer = null;
	private List<KeyedMessage<String,String>> msglist = new ArrayList<KeyedMessage<String, String>>();
	private long receiveNum = 0L;
	private long sendNum = 0L;
	private long receiveTotalNum = 0L;
	private long receiveXdrNum = 0L;
	private boolean is_login = false;
	private HashMap<String,Long> sendTopicNum=new HashMap<String,Long>();
	
	
	public ReceiveHandlerV4bak(ThreadGroup tg,Socket socket,String interface_type)throws IOException {
		super(tg,"ServerHandlerV4");
		this.socket = socket;
		this.interface_type=interface_type;

		InputStream  is = this.getClass().getClassLoader().getResourceAsStream("producer.properties");
		Properties props = new Properties();
//		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
//		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
//		props.put("metadata.broker.list", "cloud1:9092, cloud22:9092, cloud3:9092");
		props.load(is);
		ProducerConfig config = new ProducerConfig(props);
		this.producer = new Producer<String,String>(config);
	}

	public void run() {
		short messageType=0;
		int sequenceId=0;
		byte totalContents=0;
		int totalLength=0;
		long lastSendTime=System.currentTimeMillis();
		HashMap<String,String> topicMap=new HashMap<String,String>();
		StringBuilder sb=new StringBuilder();
		//��ʼ��ҵ��������topic�Ķ�Ӧ��ϵ
		topicMap.put("100", "topic_lte_common");
		topicMap.put("101", "topic_lte_dns");
		topicMap.put("102", "topic_lte_mms");
		topicMap.put("103", "topic_lte_http");
		topicMap.put("104", "topic_lte_ftp");
		topicMap.put("105", "topic_lte_email");
		topicMap.put("106", "topic_lte_voip");
		topicMap.put("107", "topic_lte_rtsp");
		topicMap.put("108", "topic_lte_im");
		topicMap.put("109", "topic_lte_p2p_gn_s11");
		topicMap.put("0", "topic_lte_p2p_gn_s11");
		topicMap.put("1", "topic_lte_p2p_gn_s11");
		
		try {
			this.socket.setSoLinger(true, 3);// ���ùر�ʱ�������ر����ӣ�����ݷ�����ɻ�3����ٹر�
			DataInputStream dis = new DataInputStream(this.socket.getInputStream());
			DataOutputStream out = new DataOutputStream(this.socket.getOutputStream());
			
			do {
				int readnum = 0;
			    totalLength=dis.readUnsignedShort();

				int length = totalLength - 2;
				byte[] buffer = new byte[length];
				readnum = 0;
				while (readnum < length) {
					int num = dis.read(buffer, readnum, length - readnum);
					if (num > 0) {
						readnum += num;
					}
				}
				int off = 0;

				messageType = ConvToByte.byteToShort(buffer, off);
				off += 2;

				sequenceId = ConvToByte.byteToInt(buffer, off);
				off += 4;
				totalContents = buffer[off++];
				if (this.is_login && messageType == 5) {
					//

					this.receiveNum++;// sdtp���У����
					this.receiveXdrNum++;// ��¼���յ�xdr��
					// ���Ӧ��
					byte[] requestArray = responseNotifyEventData(sequenceId, totalContents);
					out.write(requestArray);
//					out.flush();
					String topicName = "";

					// s1-u�ĳ��ϣ����Ϊ���ģ�ֱ��ת���ַ�
					if ("s1_u".equals(this.interface_type)) {
						String[] messages = new String(buffer, off, buffer.length - off).split("\\r\\n");
						// ��¼���յ����������
						this.receiveTotalNum += messages.length;
						for (String message : messages) {
							String[] fields = message.split("\\|");
							if (fields.length >= 19) {
								String appTypeCode = fields[18];
								topicName = topicMap.get(appTypeCode);
							} else {
								sb.delete(0, sb.length());
								sb.append("total_length=" + totalLength);
								sb.append("sequenceId=" + sequenceId);
								sb.append("totalContents=" + totalContents);
								sb.append("message=" + message + "\n");
								CountWriter.writeerror(sb.toString());// ��¼�����
								continue;
							}
							if (topicName == null || "".equals(topicName))
								topicName = "topic_lte_p2p_gn_s11";
							KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, message);
							msglist.add(km);
							long num = sendTopicNum.get(topicName) == null ? 0 : sendTopicNum.get(topicName);
							sendTopicNum.put(topicName, num + 1);
						}
						// s1-mme���ϣ����Ϊxdr����Ҫ����
					} else if ("s1_mme".equals(this.interface_type)) {
						// ��¼���յ����������
						this.receiveTotalNum += totalContents;
						topicName = "topic_lte_s1_mme";
						int xdrtype = (buffer[off] & 0xff);// ���ӿ�/�ϳɽӿ�xdr��ʶ
						off += 1;
						// ���ӿ�xdr���ӿ�������5�ĳ��ϣ��ж�Ϊs1-mme�ӿڣ������¼��badmessage
						for (int i = 0; i < (totalContents & 0xff); i++) {
							String message = "";
							if (xdrtype == 2 && (buffer[off + 4] & 0xff) == 5) {
								message = getMessages(buffer, off);
							} else {
								sb.delete(0, sb.length());
								sb.append("total_length=" + totalLength);
								sb.append("sequenceId=" + sequenceId);
								sb.append("totalContents=" + totalContents);
								sb.append("body=");
								for (int j = off; j < buffer.length; j++) {
									sb.append(String.format("%02X", buffer[i]));
								}
								sb.append("\n");
								CountWriter.writeerror(sb.toString());// ��¼�����
								break;
							}
							KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName, message);
							msglist.add(km);
							long num = sendTopicNum.get(topicName) == null ? 0 : sendTopicNum.get(topicName);
							sendTopicNum.put(topicName, num + 1);
						}

					}

					// ��Ϣ�ۻ��500�����߾����ϴη��ͳ���3��ʱ������Ϣ,��ÿ����Ϣ������
					if (msglist.size() >= 500 || ((System.currentTimeMillis() - lastSendTime) >= 3000 && msglist.size() > 0)) {
						lastSendTime = System.currentTimeMillis();
						producer.send(msglist);
						this.sendNum += msglist.size();
						msglist.clear();
					}
					// ��¼��ݰ���Ϣ����
					CountWriter.writepackage("sequenceId=" + sequenceId + ";totalContents=" + totalContents + ";\n");
				} else if (messageType == 1) {// verNego �汾Э��
					byte[] requestArray = responseVerNego(sequenceId, totalContents);
					out.write(requestArray);
					out.flush();
				} else if (messageType == 2) {// linkAuth Ȩ����֤
					byte[] loginId = new byte[12];
					byte[] digestArray = new byte[16];
					byte[] timestamp = new byte[4];
					byte[] rand = new byte[2];
					System.arraycopy(buffer, off, loginId, 0, 12);
					off += 12;
					System.arraycopy(buffer, off , digestArray, 0, 16);
					off += 16;
					System.arraycopy(buffer, off , timestamp, 0, 4);
					off += 4;
					System.arraycopy(buffer, off , rand, 0, 2);

					byte[] requestArray = responseLinkAuth(sequenceId, totalContents, loginId, digestArray, timestamp,
					        rand);
					out.write(requestArray);
					out.flush();

					this.is_login = true;
				} else if (this.is_login && messageType == 3) {// linkCheck ��·���
					byte[] requestArray = responseLinkCheck(sequenceId, totalContents);
					out.write(requestArray);
					out.flush();
				} else if (this.is_login && messageType == 7) {// linkDataCheck ��·��ݷ���У��
					int  sendflag=ConvToByte.byteToInt(buffer, off);
					off += 4;
					int  sendDataInfo=ConvToByte.byteToInt(buffer, off);

					byte[] requestArray = responseLinkDataCheck(sequenceId, totalContents,sendflag,sendDataInfo,(int)receiveNum);
					this.receiveNum=0;
					out.write(requestArray);
					out.flush();
				} else {
					break;// �ͷ����ӣ�δ��֤Ȩ�޻���messageType����1��2��3, 7
				}
			} while (messageType != 4);// linkRel �����ͷ�
			System.out.println("messageType=" + messageType);
			if(msglist.size()>0){
				producer.send(msglist);
				this.sendNum+=msglist.size();
				msglist.clear();
			}
			byte[] requestArray = responseLinkRel(sequenceId, totalContents);
			out.write(requestArray);
			out.flush();
		} catch (Exception e) {
			System.out.println("totalLength="+totalLength+";messageType="+messageType+";sequenceId="+sequenceId+";totalContents="+totalContents);
			e.printStackTrace();
			
		} finally {
			close();
		}
	}

	public byte[] responseVerNego(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8001;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
        System.out.println("VerNego :"+reslut);
		return requestArray;
	}

	public byte[] responseLinkAuth(int sequenceId, byte totalContents, byte[] loginId, byte[] digestArray,
	        byte[] timestamp, byte[] rand) {
		short totalLength = 74;
		int messageType = 0x8002;
		byte reslut = 0;
		String passwd = "asiainfo123";


		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		int pos = 0;
		try {
			// ��ȡMD5������
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			// ���������Ӧ��SHA256����ֵ
			byte[] sha_passwd = md.digest(passwd.getBytes());
			StringBuilder sb=new StringBuilder();
			for (byte b : sha_passwd)
				sb.append(String.format("%02x", b));
			// ƴ����ʱ�ֶΣ��Լ���digest���Ϳͻ��˷��͵�ֵ�ȶ�

			String str_tmp=(new String(loginId))+sb.toString()+ConvToByte.byteToInt(timestamp, 0)+"rand="+ConvToByte.byteToShort(rand, 0);

			// ����digest
			byte[] digest = md.digest(str_tmp.getBytes());
			// �ȶԼ������digest�Ϳͻ��˷��͹�����digest�Ƿ���ȣ���ȷ���1�����򷵻�0
			reslut = Arrays.equals(digest, digestArray) ? (byte) 1 : (byte) 0;
			
			sb.delete(0, sb.length());
			for (byte b : digestArray)
				sb.append(String.format("%02x", b));
			String receive=sb.toString();
			sb.delete(0, sb.length());
			for (byte b : digest)
				sb.append(String.format("%02x", b));
			System.out.println("LinkAuth :received digest="+receive+";our digest="+sb.toString());
			
			reslut=1;//������֤��������ݽ���
			System.out.println("LinkAuth :"+reslut);
		} catch (Exception e) {
			e.printStackTrace();
		}

		byte[] requestArray = new byte[totalLength];
		pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
		pos++;
		System.arraycopy(digestArray, 0, requestArray, pos, digestArray.length);

		return requestArray;
	}

	public byte[] responseLinkCheck(int sequenceId, byte totalContents) {
		short totalLength = 9;
		int messageType = 0x8003;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		
		System.out.println("LinkCheck ");
		return requestArray;
	}

	public byte[] responseLinkRel(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8004;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
		System.out.println("LinkRel:"+reslut);
		return requestArray;
	}

	public byte[] responseNotifyEventData(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8005;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;

		return requestArray;
	}
	
	public byte[] responseLinkDataCheck(int sequenceId, byte totalContents,int sendflag,int sendDataInfo,int receiveDatainfo) {
		short totalLength = 22;
		int messageType = 0x8007;
		byte reslut=0 ;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);
		byte[] sendflagArray = ConvToByte.intToByte(sendflag);
		byte[] sendDataInfoArray = ConvToByte.intToByte(sendDataInfo);
		byte[] recciveDatainfoArray = ConvToByte.intToByte(receiveDatainfo);
		
		if(sendDataInfo==receiveDatainfo){
			reslut=0;
		}else if(sendDataInfo>receiveDatainfo){
			reslut=1;
		}else if(sendDataInfo<receiveDatainfo){
			reslut=2;
		}
		
		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		System.arraycopy(sendflagArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = reslut;
		pos++;
		System.arraycopy(sendDataInfoArray, 0, requestArray, pos, 4);
		pos += 4;
		System.arraycopy(recciveDatainfoArray, 0, requestArray, pos, 4);
		System.out.println("threadid:"+this.getId()+"; responseLinkDataCheck:reslut="+reslut+";sendDataInfo="+sendDataInfo+";recciveDatainfo="+receiveDatainfo);
		return requestArray;
	}
	
	private String getMessages(byte[] buffer,int off){
		StringBuilder sb=new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)+"|");//Length
		off+=2;
		sb.append(ConvToByte.decodeTBCD(buffer,off,off+2,true)+"|");//City
		off+=2;
		sb.append(buffer[off]+"|");//Interface
		off+=1;
		sb.append(ConvToByte.getHexString(buffer,off,off+16)+"|");//XDR ID
		off+=16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer,off)+"|");//RAT
		off+=1;
		sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)+"|");//IMSI
		off+=8;
		sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)+"|");//IMEI
		off+=8;
		sb.append(ConvToByte.decodeTBCD(buffer,off,off+16,false)+"|");//MSISDN
		off+=16;
		sb.append((buffer[off]&0xff)+"|");//Procedure Type
		off+=1;
		sb.append(ConvToByte.byteToLong(buffer,off)+"|");//Procedure Start Time
		off+=8;
		sb.append(ConvToByte.byteToLong(buffer,off)+"|");//Procedure End Time
		off+=8;
		sb.append((buffer[off]&0xff)+"|");//Procedure Status
		off+=1;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//Request Cause
		off+=2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//Failure Cause
		off+=2;
		sb.append((buffer[off]&0xff)+"|");//Keyword 1
		off+=1;
		sb.append((buffer[off]&0xff)+"|");//Keyword 2
		off+=1;
		sb.append((buffer[off]&0xff)+"|");//Keyword 3
		off+=1;
		sb.append((buffer[off]&0xff)+"|");//Keyword 4
		off+=1;
		sb.append(ConvToByte.byteToUnsignedInt(buffer,off)+"|");//MME UE S1AP ID
		off+=4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//Old MME Group ID
		off+=2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer,off)+"|");//Old MME Code
		off+=1;
		sb.append(ConvToByte.getHexString(buffer,off,off+4)+"|");//Old M-TMSI
		off+=4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//MME Group ID
		off+=2;
		sb.append(ConvToByte.byteToUnsignedByte(buffer,off)+"|");//MME Code
		off+=1;
		sb.append(ConvToByte.getHexString(buffer,off,off+4)+"|");//M-TMSI
		off+=4;
		sb.append(ConvToByte.getHexString(buffer,off,off+4)+"|");//TMSI
		off+=4;
		sb.append(ConvToByte.getIpv4(buffer,off)+"|");//USER_IPv4
		off+=4;
		sb.append(ConvToByte.getIpv6(buffer,off)+"|");//USER_IPv6
		off+=16;
		sb.append(ConvToByte.getIp(buffer,off)+"|");//MME IP Add
		off+=16;
		sb.append(ConvToByte.getIp(buffer,off)+"|");//eNB IP Add
		off+=16;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//MME Port
		off+=2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//eNB Port
		off+=2;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//TAC
		off+=2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer,off)+"|");//Cell ID
		off+=4;
		sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//Other TAC
		off+=2;
		sb.append(ConvToByte.byteToUnsignedInt(buffer,off)+"|");//Other ECI
		off+=4;
		sb.append(new String(buffer,off,32).trim()+"|");//APN
		off+=32;
		
		int epsBearerNum=ConvToByte.byteToUnsignedByte(buffer,off);
		sb.append(epsBearerNum+"|");//EPS Bearer Number
		off+=1;
		for(int n=0;n<epsBearerNum;n++){
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)+"|");//Bearer 1 ID
			off+=1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)+"|");//Bearer 1 Type
			off+=1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)+"|");//Bearer 1 QCI
			off+=1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)+"|");//Bearer 1 Status
			off+=1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//Bearer 1 Request Cause
		    off+=2;
		    sb.append(ConvToByte.byteToUnsignedShort(buffer,off)+"|");//Bearer 1 Failure Cause
		    off+=2;
		    sb.append(ConvToByte.byteToUnsignedInt(buffer,off)+"|");//Bearer 1 eNB GTP-TEID
		    off+=4;
		    sb.append(ConvToByte.byteToUnsignedInt(buffer,off)+"|");//Bearer 1 SGW GTP-TEID
		    off+=4;
		}
		
		return sb.toString();
	}

	public long getReceiveNum() {
		return this.receiveNum;
	}

	public long getSendNum() {
		return this.sendNum;
	}

	public long getReceiveTotalNum() {
    	return receiveTotalNum;
    }

	public long getReceiveXdrNum() {
    	return receiveXdrNum;
    }

	public HashMap<String, Long> getSendTopicNum() {
    	return sendTopicNum;
    }

	private void close() {
		try {
			System.out.println("�ر����ӣ��ͻ��ˣ�" + this.socket.getInetAddress().getHostAddress());
			this.socket.close();
			this.producer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			//��ӡ��־
			CountWriter.addCount(Thread.currentThread().getId()+"", new CountEntity(this.receiveTotalNum,this.receiveXdrNum,this.sendNum,this.sendTopicNum));
//			CountWriter.write(Thread.currentThread().getId()+"");
			if(Thread.activeCount()==1){
				CountWriter.writeCount();
//				CountWriter.close();
			}
		}
	}
}
