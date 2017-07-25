package com.asiainfo.sdtp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ServerHandlerV4 implements Runnable {
	private Socket socket = null;
	private Producer<String, String> producer = null;
	private List<KeyedMessage<String,String>> msglist = new ArrayList<KeyedMessage<String, String>>();
	private long receiveNum = 0L;
	private long sendNum = 0L;
	private long receiveTotalNum = 0L;
	private long receiveXdrNum = 0L;
	private boolean is_login = false;
	private HashMap<String,Long> sendTopicNum=new HashMap<String,Long>();
	
	
	public ServerHandlerV4(Socket socket)throws IOException {
		this.socket = socket;

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
		
		//��ʼ��xdr������topic�Ķ�Ӧ��ϵ
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

//			FileWriter fw = new FileWriter("/bigdata/interface/ltelog/sockettest.txt");
//			FileOutputStream fout = new FileOutputStream(new File("/bigdata/interface/ltelog/sockettest_bin.txt"));   
//			FileWriter fw = new FileWriter("/home/ocdc/sockettest.txt");
//			FileOutputStream fout = new FileOutputStream(new File("/home/ocdc/sockettest_bin.txt"));   
			
			do {
//				byte[] totalLengthArray = new byte[2];
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
//				System.out.println("sequenceId=" + sequenceId+":messageType="+messageType+";totalLength="+totalLength);
				totalContents = buffer[off++];
				
				
//				StringBuilder sb =new StringBuilder();
//				for(int i=off;i<buffer.length;i++){
//					sb.append(String.format("%02X", buffer[i]));
//				}
//				fw.write("totalLength="+totalLength+";messageType="+messageType+";sequenceId="+sequenceId+";totalContents="+totalContents+";body="+sb.toString()+"\n");
//				fout.write(("totalLength="+totalLength+";messageType="+messageType+";sequenceId="+sequenceId+";totalContents="+totalContents+";body=").getBytes());
//				fout.write(buffer,off,buffer.length-off);
				
				if (this.is_login && messageType == 5) {
					//
					
					this.receiveNum++;//sdtp���У����
					this.receiveXdrNum++;//��¼���յ�xdr��
					//���Ӧ��
					byte[] requestArray = responseNotifyEventData(sequenceId, totalContents);
					out.write(requestArray);
					out.flush();
					
					//ת��kafka test topic
					String[] messages=new String(buffer,off,buffer.length-off).split("\\r\\n");
					//��¼���յ����������
					this.receiveTotalNum +=messages.length;
					for(String message : messages){
						String[] fields=message.split("\\|");
						if(fields.length<19){
							CountWriter.writeerror(message+"\n");//��¼�����
							continue;
						}
						String appTypeCode=fields[18];
						String topicName=topicMap.get(appTypeCode);
						if (topicName==null)topicName="topic_lte_p2p_gn_s11";
						KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName,message);
						msglist.add(km);
						long num=sendTopicNum.get(topicName)==null?0:sendTopicNum.get(topicName);
						sendTopicNum.put(topicName, num+1);
					}
					//��Ϣ�ۻ��500�����߾����ϴη��ͳ���10��ʱ������Ϣ,��ÿ����Ϣ������
					if(msglist.size()>=500||((System.currentTimeMillis()-lastSendTime)>=10000 && msglist.size()>0)){
						lastSendTime=System.currentTimeMillis();
						producer.send(msglist);
						this.sendNum+=msglist.size();
						msglist.clear();
					}
                 //��¼��ݰ���Ϣ����
					CountWriter.writepackage("sequenceId="+sequenceId+";totalContents="+totalContents+";split_msg="+messages.length+";\n");
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
			byte[] requestArray = responseLinkRel(sequenceId, totalContents);
			out.write(requestArray);
			out.flush();
			// socket.shutdownOutput();
//			fw.flush();
//			fw.close();
		} catch (Exception e) {
			System.out.println("totalLength="+totalLength+";messageType="+messageType+";sequenceId="+sequenceId+";totalContents="+totalContents);
			e.printStackTrace();
			
		} finally {
//			this.producer.close();
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
		System.out.println("responseLinkDataCheck:reslut="+reslut+";sendDataInfo="+sendDataInfo+";recciveDatainfo="+receiveDatainfo);
		return requestArray;
	}

	public long getReceiveNum() {
		return this.receiveNum;
	}

	public long getSendNum() {
		return this.sendNum;
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
			CountWriter.write(Thread.currentThread().getId()+"");
			if(Thread.activeCount()==1){
				CountWriter.writeCount();
				CountWriter.close();
			}
		}
	}
}
