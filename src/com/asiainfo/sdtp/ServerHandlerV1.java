package com.asiainfo.sdtp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Arrays;

public class ServerHandlerV1 implements Runnable {
	private Socket socket = null;
	private long receiveNum = 0L;
	private long sendNum = 0L;
	private boolean is_login = false;
	public ServerHandlerV1(Socket socket)throws IOException {
		this.socket = socket;

//		InputStream  is = getClass().getResourceAsStream("/producer.properties");
//		Properties props = new Properties();
//		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
//		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
//		props.put("metadata.broker.list", "cloud1:9092, cloud22:9092, cloud3:9092");
//		props.load(is);
//		ProducerConfig config = new ProducerConfig(props);
//		this.producer = new Producer<String, byte[]>(config);
	}

	public void run() {
		try {
			this.socket.setSoLinger(true, 3);// ���ùر�ʱ�������ر����ӣ�����ݷ�����ɻ�3����ٹر�
			DataInputStream dis = new DataInputStream(this.socket.getInputStream());
			DataOutputStream out = new DataOutputStream(this.socket.getOutputStream());

			FileWriter fw = new FileWriter("/bigdata/interface/ltelog/sockettest.txt");
//			FileWriter fw = new FileWriter("/home/ocdc/sockettest.txt");
			short messageType;
			int sequenceId;
			byte totalContents;
			do {
				byte[] totalLengthArray = new byte[2];
				int readnum = 0;
				while (readnum < 2) {
					int num = dis.read(totalLengthArray, readnum, 2 - readnum);
					if (num > 0) {
						readnum += num;
					}
				}
				short totalLength = ConvToByte.byteToShort(totalLengthArray, 0);

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
				
				
				StringBuilder sb =new StringBuilder();
				for(int i=off;i<buffer.length;i++){
					sb.append(String.format("%02X", buffer[i]));
				}
				fw.write("totalLength="+totalLength+";messageType="+messageType+";sequenceId="+sequenceId+";totalContents="+totalContents+";body="+sb.toString()+"\n");
				if (this.is_login && messageType == 5) {
					//
					
					this.receiveNum++;
					
					//���Ӧ��
					byte[] requestArray = responseNotifyEventData(sequenceId, totalContents);
					out.write(requestArray);
					out.flush();
					
					
					
					
					
					
					
					
					
					
					
					
					
					
//					String msg = new String(buffer, off , totalLength - 9);
//					String msg="";
//					this.receiveNum += 1L;
//					//8.2.3.1	������Ϣ������21��
//					int xdr_length= ConvToByte.byteToShort(buffer, off);//����ָʾ���XDR��ռ���ֽ��� 2�ֽ�
//					if (xdr_length<(2266)){//httpҵ��ʱ��xdr��С����
//						msg="error:xdr's length is"+xdr_length+", less than 2266!";
//						fw.write(msg + "\n");
//						continue;
//					}
//					msg +=xdr_length+",";
//                    off +=2;
//                    msg += (buffer[off] & 0xff)+""+(buffer[off+1] & 0xff);//������ţ���010��?�� 2�ֽ�
//					msg +=",";
//                    off +=2;
//                    msg +=buffer[off] & 0xff;  //�ӿ����ͣ�16���Ʊ��� 1�ֽ�
//                    msg +=",";
//                   off +=1;
//                   String xDRID="";
//                   for (int i=0;i<16;i++){
//                	   xDRID+=String.format("%02X", buffer[off]);
//                	   off +=1;
//                   }
//                   msg +=xDRID;  //DPI�豸��Ψһ��xDR��ţ�16���Ʊ��롣16�ֽ�
//                   msg +=",";
//                   
//                   //8.2.3.2	�ƶ���ͨ����Ϣ����������PS�ೡ����������92��ipv6ʱΪ116����ʱ��������ֻ�ж���ipv4����ipv6
//                   off+=33;
//                   String iptype=(buffer[off] &0xff)==0x01 ? "IPv4":"IPv6";
//                   msg +=",";
//                   msg+=iptype;
//                   if("IPv4".equals(iptype)){
//                	   off+=59;
//                   }else{
//                	   off+=83;
//                   }
//                  
//                   //8.2.3.3	ͨ��ҵ����Ϣ������137��ֻ����ҵ�����ͱ��룬������ʱ������
//                   int service_type=buffer[off] &0xff;
//                   msg+=service_type;
//                   msg +=",";
//                   off+=137;
//                   
//                   //8.2.3.4	�ض�ҵ����Ϣ ֻ����http��url���������
//                   if (service_type==103){
//                	   off+=81;
//                   }
//                   byte[] uri=new byte[512];
//                   System.arraycopy(buffer, off, uri, 0, 512);
//                   msg+=new String(uri);
//                   
//                   
//					fw.write(msg + "\n");
//					String partition = String.valueOf(this.receiveNum % 8L);
//					KeyedMessage<String, byte[]> kafka_message = new KeyedMessage<String, byte[]>("localupdate", partition, msg.getBytes());
//					this.msglist.add(kafka_message);
//					if (this.receiveNum % 1000L == 0L) {
//						this.producer.send(this.msglist);
//						this.sendNum += 1000L;
//						this.msglist = new ArrayList();
//					}
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
				} else if (this.is_login && messageType == 8) {// ��ر���ϱ�
					byte[] requestArray = SCGIDInform_Resp(sequenceId, totalContents);
					out.write(requestArray);
					out.flush();
				}else if (this.is_login && messageType == 9) {// ״̬֪ͨ����
					byte[] requestArray = statusNotification_Resp(sequenceId, totalContents);
					out.write(requestArray);
					out.flush();
				}else if (this.is_login && messageType == 10) {// �澯�ϱ�����
					byte[] requestArray = alarmInform_Resp(sequenceId, totalContents);
					out.write(requestArray);
					out.flush();
				}
				else {
					break;// �ͷ����ӣ�δ��֤Ȩ�޻���messageType����1��2��3
				}
			} while (messageType != 4);// linkRel �����ͷ�
			System.out.println("messageType=" + messageType);
			byte[] requestArray = responseLinkRel(sequenceId, totalContents);
			out.write(requestArray);
			out.flush();
			// socket.shutdownOutput();
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
//			this.producer.close();
			close();
		}
	}

	public byte[] responseVerNego(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 32769;
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
		short totalLength = 26;
		short messageType =(short) 32770;
		byte reslut = 0;
		String passwd = "asiainfo123";

		byte[] sha_passwd = new byte[32];

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.shortToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		int pos = 0;
		try {
			// ��ȡMD5������
			MessageDigest md = MessageDigest.getInstance("MD5");
			// ���������Ӧ��MD5����ֵ
			sha_passwd = md.digest(passwd.getBytes());
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
		System.arraycopy(messageTypeArray, 0, requestArray, pos, 2);
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
		short messageType = (short)32771;

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
		
		System.out.println("LinkCheck ");
		return requestArray;
	}

	public byte[] responseLinkRel(int sequenceId, byte totalContents) {
		short totalLength = 10;
		short messageType = (short)32772;
		byte reslut = 1;

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
		requestArray[pos] = reslut;
		System.out.println("LinkRel:"+reslut);
		return requestArray;
	}

	public byte[] responseNotifyEventData(int sequenceId, byte totalContents) {
		short totalLength = 10;
		short messageType =(short) 0x8005;
		byte reslut = 1;

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
		requestArray[pos] = reslut;

		return requestArray;
	}
	
	public byte[] SCGIDInform_Resp(int sequenceId, byte totalContents) {
		short totalLength = 10;
		short messageType =(short) 0x8008;
		byte reslut = 1;

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
		requestArray[pos] = reslut;

		return requestArray;
	}
	
	
	public byte[] statusNotification_Resp(int sequenceId, byte totalContents) {
		short totalLength = 10;
		short messageType =(short) 0x8009;
		byte reslut = 1;

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
		requestArray[pos] = reslut;

		return requestArray;
	}
	
	public byte[] alarmInform_Resp(int sequenceId, byte totalContents) {
		short totalLength = 10;
		short messageType =(short) 0x800A;
		byte reslut = 1;

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
		requestArray[pos] = reslut;

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
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
