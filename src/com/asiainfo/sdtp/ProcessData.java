package com.asiainfo.sdtp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang.StringUtils;

import kafka.producer.KeyedMessage;

public class ProcessData extends Thread {
	private String interface_type = "";
	private HashMap<String,Long> sendTopicNum=new HashMap<String,Long>();
	private SendDataToKafka[] nextstep;
	public  ArrayBlockingQueue<byte[]> msg_queue ;
	
	public ProcessData(ThreadGroup tg,SendDataToKafka[] nextstep)throws IOException {
		super(tg,"ServerHandlerV4");
		this.nextstep=nextstep;
		this. msg_queue=new  ArrayBlockingQueue<byte[]>(Integer.parseInt(Utils.getProperties("process_queue_size")));
		this. interface_type=Utils.getProperties("interface_type");
	}

	public void run() {
		short messageType=0;
		int sequenceId=0;
		byte totalContents=0;
		int totalLength=0;
		HashMap<String,String> topicMap=new HashMap<String,String>();
		Random random = new Random(this.getId());
		StringBuilder sb=new StringBuilder();
		//topic map
		topicMap.put("1", "topic_lte_uu_in");
		topicMap.put("2", "topic_lte_x2_in");
		topicMap.put("3", "topic_lte_uemr_in");
		topicMap.put("4", "topic_lte_mr_in");
		topicMap.put("5", "topic_lte_s1_mme");//
		topicMap.put("6", "topic_lte_s6a_in");//
		topicMap.put("7", "topic_lte_s11_in");//
		topicMap.put("8", "topic_lte_s10_in"); //
		topicMap.put("9", "topic_lte_sgs_in");//
		
		topicMap.put("13", "topic_lte_gm_in");
		topicMap.put("14", "topic_lte_mw_in");
		topicMap.put("15", "topic_lte_mg_in");
		topicMap.put("16", "topic_lte_mi_in");
		topicMap.put("17", "topic_lte_mj_in");
		topicMap.put("18", "topic_lte_isc_in");
		
		topicMap.put("19", "topic_lte_sv_in");
		
		topicMap.put("20", "topic_lte_cx_in");
		topicMap.put("21", "topic_lte_dx_in");
		topicMap.put("22", "topic_lte_sh_in");
		topicMap.put("23", "topic_lte_dh_in");
		topicMap.put("24", "topic_lte_zh_in");

		topicMap.put("25", "topic_lte_gx_in");
		topicMap.put("26", "topic_lte_rx_in");
		
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
			
			while(true) {
				int off = 0;
				byte[] buffer=this.msg_queue.take();
			    totalLength=buffer.length+2;

				messageType = ConvToByte.byteToShort(buffer, off);
				off += 2;

				sequenceId = ConvToByte.byteToInt(buffer, off);
				off += 4;
				totalContents = buffer[off++];
					//

					String topicName = "";

					// s1-u
					if ("s1_u".equals(this.interface_type)) {
						String[] messages = new String(buffer, off, buffer.length - off).split("\\r\\n");
//						this.receiveTotalNum += messages.length;
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
//							msglist.add(km);
							while(true){
								if(nextstep[random.nextInt(nextstep.length)].msg_queue.offer(km))
									break;
							}
							long num = sendTopicNum.get(topicName) == null ? 0 : sendTopicNum.get(topicName);
							sendTopicNum.put(topicName, num + 1);
						}
						// s1-mme
					} else if ("s1_mme".equals(this.interface_type)) {
						//int xdrtype = (buffer[off] & 0xff); 单接口和合成接口标识
						off += 1;
						for (int i = 0; i < (totalContents & 0xff); i++) {
							int lte_type = buffer[off + 4] & 0xff;
							topicName = topicMap.get(String.valueOf(lte_type));
							KeyedMessage<String, String> km =null;
							
							switch (lte_type) {
							case 1:
								km = UuDataHandler(buffer, off, topicName);
								break;
							case 2:
								km = X2DataHandler(buffer, off, topicName);
								break;
							case 3:
								km = uemrDataHandler(buffer, off, sdf, topicName);
								break;
							case 4:
								km = cellmrDataHandler(buffer, off, sdf, topicName);
								break;
							case 5:
								km = getMessages(buffer, off, topicName);
								break;
							case 6:
								km = s6aDataHandler(buffer, off, sdf, topicName);
								break;
							case 7:
							case 8:
								km = s11DataHandler(buffer, off, sdf, topicName);
								break;
							case 9:
								km = sgsDataHandler(buffer, off, sdf, topicName);
								break;
							case 13:
							case 14:
							case 15:
							case 16:
							case 17:
							case 18:
								km = gm_mw__mg_mi_mj_isc_DataHandler(buffer, off, topicName);
								break;
							case 19:
								km = svDataHandler(buffer, off, topicName);
								break;
							case 20:
							case 21:
							case 22:
							case 23:
							case 24:
								km = cx_dx_sh_dh_DataHandler(buffer, off, topicName);
								break;
							case 25:
							case 26:
								km = gx_rx_DataHandler(buffer, off, topicName);
								break;
							default:
								sb.delete(0, sb.length());
								sb.append("total_length=" + totalLength);
								sb.append("sequenceId=" + sequenceId);
								sb.append("totalContents=" + totalContents);
								sb.append("body=");
								sb.append(getOtherMessageCommon(buffer,off));
								sb.append("\n");
								CountWriter.writeerror(sb.toString());
								break;
							}
							
							
							if(topicName != null)
							{
								nextstep[random.nextInt(nextstep.length)].msg_queue.put(km);
								long num = sendTopicNum.get(topicName) == null ? 0 : sendTopicNum.get(topicName);
								sendTopicNum.put(topicName, num + 1);
							}
							
						}                                                              

					}

			}
		} catch (Exception e) {
			System.out.println("totalLength="+totalLength+";messageType="+messageType+";sequenceId="+sequenceId+";totalContents="+totalContents);
			e.printStackTrace();
			
		} finally {
			
		}
	}
		private KeyedMessage<String, String> getMessages(byte[] buffer,int off, String topicName){
			StringBuilder sb=new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
			off+=2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+2,true)).append("|");//City
			off+=2;
			sb.append(buffer[off]+"|");//Interface
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+16)).append("|");//XDR ID
			off+=16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//RAT
			off+=1;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMSI
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMEI
			off+=8;
			sb.append(StringUtils.replaceChars(ConvToByte.decodeTBCD(buffer, off, off + 16,false),"f", "")).append("|");//MSISDN
			off+=16;
			sb.append((buffer[off]&0xff)).append("|");//Procedure Type
			off+=1;
			sb.append(ConvToByte.byteToLong(buffer,off)).append("|");//Procedure Start Time
			off+=8;
			sb.append(ConvToByte.byteToLong(buffer,off)).append("|");//Procedure End Time
			off+=8;
			sb.append((buffer[off]&0xff)).append("|");//Procedure Status
			off+=1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Request Cause
			off+=2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Failure Cause
			off+=2;
			sb.append((buffer[off]&0xff)).append("|");//Keyword 1
			off+=1;
			sb.append((buffer[off]&0xff)).append("|");//Keyword 2
			off+=1;
			sb.append((buffer[off]&0xff)).append("|");//Keyword 3
			off+=1;
			sb.append((buffer[off]&0xff)).append("|");//Keyword 4
			off+=1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//MME UE S1AP ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Old MME Group ID
			off+=2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//Old MME Code
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+4)).append("|");//Old M-TMSI
			off+=4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//MME Group ID
			off+=2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//MME Code
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+4)).append("|");//M-TMSI
			off+=4;
			sb.append(ConvToByte.getHexString(buffer,off,off+4)).append("|");//TMSI
			off+=4;
			sb.append(ConvToByte.getIpv4(buffer,off)).append("|");//USER_IPv4
			off+=4;
			sb.append(ConvToByte.getIpv6(buffer,off)).append("|");//USER_IPv6
			off+=16;
			sb.append(ConvToByte.getIp(buffer,off)).append("|");//MME IP Add
			off+=16;
			sb.append(ConvToByte.getIp(buffer,off)).append("|");//eNB IP Add
			off+=16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//MME Port
			off+=2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//eNB Port
			off+=2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//TAC
			off+=2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Cell ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Other TAC
			off+=2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Other ECI
			off+=4;
			sb.append(ConvToByte.getHexString2(buffer, off, off + 32).trim()).append("|");//APN
			off+=32;
			
			int epsBearerNum=ConvToByte.byteToUnsignedByte(buffer,off);
			sb.append(epsBearerNum+"|");//EPS Bearer Number
			int IfEpsBearerNum = epsBearerNum == 255 ? 0 : epsBearerNum;
			off+=1;
			for(int n=0;n<IfEpsBearerNum;n++){
				sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//Bearer 1 ID
				off+=1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//Bearer 1 Type
				off+=1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//Bearer 1 QCI
				off+=1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//Bearer 1 Status
				off+=1;
				sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Bearer 1 Request Cause
			    off+=2;
			    sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Bearer 1 Failure Cause
			    off+=2;
			    sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Bearer 1 eNB GTP-TEID
			    off+=4;
			    sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Bearer 1 SGW GTP-TEID
			    off+=4;
			}
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
			km = new KeyedMessage<String, String>(topicName, sb.toString());
			
			return km;
		}
		
		private KeyedMessage<String, String> UuDataHandler(byte[] buffer, int off, String topicName) {
			StringBuilder sb = new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			// 公共信息
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length unsigned int
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City byte
			off += 2;
			sb.append(buffer[off] ).append("|");// Interface unsigned int
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR
																			// ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT unsigned int
			off += 1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN byte
			off += 16;
			
            /*//	Uu接口信息
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Type byte
			off += 1;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// Procedure Start
																// Time
			off += 8;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// Procedure End
																// Time
			off += 8;
			
			sb.append((buffer[off] & 0xff)).append("|");// Keyword 1
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Keyword 2
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// PLMN ID
																			
			off += 3;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// eNB ID
																			
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Cell ID
																		
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// C-RNTI
			
			off += 2;
			
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Target eNB ID
			off += 4;
			sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");// Target Cell ID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Target C-RNTI
			off += 2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// MME UE 
			off += 4;
			
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//MME UE S1AP ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//MME Group ID
			off+=2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//MME Code
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+4)).append("|");//M-TMSI
			off+=4;
			
			
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// CSFB Indication
			off+=1;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Redirected Network
			off+=1;
			
			int epsBearerNum=ConvToByte.byteToUnsignedByte(buffer,off);
			sb.append(epsBearerNum).append("|");//EPS Bearer Number
			off+=1;
			for(int n=0;n<epsBearerNum;n++){
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
																			// 1--n ID
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
				  															// 1--n status
				off += 1;
			}
			// System.out.println("mme======"+sb.toString());
*/
            sb.append(new Date());
            
            km = new KeyedMessage<String, String>(topicName,imsi, sb.toString());
			
			return km;
		}
		
		private KeyedMessage<String, String> X2DataHandler(byte[] buffer, int off, String topicName) {
			StringBuilder sb = new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			// 公共信息
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City
			off += 2;
			sb.append(buffer[off] ).append("|");// Interface
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR
																			// ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT
			off += 1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN
			off += 16;
			
			// X2接口信息
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Type byte
			off += 1;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// Procedure Start
																// Time
			off += 8;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// Procedure End
																// Time
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Source Cell ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Target Cell ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Source eNB ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//Target eNB ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer,off)).append("|");//MME UE S1AP ID
			off+=4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//MME Group ID
			off+=2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//MME Code
			off+=1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Request Cause
		    off+=2;
		    sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//Failure Cause
		    off+=2;
		    
		    int epsBearerNum=ConvToByte.byteToUnsignedByte(buffer,off);
			sb.append(epsBearerNum).append("|");//EPS Bearer Number
			off+=1;
			for(int n=0;n<epsBearerNum;n++){
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
																			// 1--n ID
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
				  															// 1--n status
				off += 1;
			}
			
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}
		
		private KeyedMessage<String, String> s11DataHandler( byte[] buffer, int off,SimpleDateFormat sdf, String topicName) {
			StringBuilder sb = new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			// 公共信息
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City
			off += 2;
			sb.append(buffer[off] ).append("|");// Interface
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR
																			// ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT
			off += 1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN
			off += 16;
			
			// S11接口信息
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Type
			off += 1;
			sb.append(sdf.format(ConvToByte.byteToLong(buffer, off))).append("|");// Procedure Start
																// Time
			off += 8;
			sb.append(sdf.format(ConvToByte.byteToLong(buffer, off))).append("|");// Procedure End
																// Time
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Failure
																			// Cause
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Request
																			// Cause
			off += 2;
			sb.append(ConvToByte.getIpv4(buffer, off)).append("|");// USER_IPv4
			off += 4;
			sb.append(ConvToByte.getIpv6(buffer, off)).append("|");// USER_IPv6
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// MME Address
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// SGW/Old MME Address
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// MME Port
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// SGW/Old
																			// MME
																			// Port
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// MME
																		// Control
																		// TEID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Old MME
																		// /SGW
																		// Control
																		// TEID
			off += 4;
			// System.out.print("s11 sb1=============" + sb.toString());
			sb.append(ConvToByte.getHexString2(buffer, off, off + 32)).append("|"); // APN
			// sb.append(ConvToByte.getHexString2(buffer, off, off + 32)).append("|");
			// //APN
			// System.out.println("s11======"+sb.toString());
			// sb.append(new String(buffer,off,32).trim()).append("|");//APN
			// System.out.print("s11 sb2=============" + sb.toString());

			off += 32;
			int epsBearerNum = ConvToByte.byteToUnsignedByte(buffer, off);
			sb.append(epsBearerNum ).append("|");// EPS Bearer Number
			int IfEpsBearerNum = epsBearerNum == 255 ? 0 : epsBearerNum;
			off += 1;
			for (int n = 0; n < IfEpsBearerNum; n++) {
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
																			// 1 ID
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
																			// 1
																			// Type
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
																			// 1 QCI
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer
																			// 1
																			// Status
				off += 1;
				sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Bearer
																			// 1 eNB
																			// GTP-TEID
				off += 4;
				sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Bearer
																			// 1 SGW
																			// GTP-TEID
				off += 4;

			}
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}

		public KeyedMessage<String, String> uemrDataHandler( byte[] buffer, int off,SimpleDateFormat sdf, String topicName) {
			StringBuilder sb = new StringBuilder();
			KeyedMessage<String, String> km = null;

			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City
			off += 2;
			sb.append(buffer[off] ).append("|");// Interface
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR
																			// ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT
			off += 1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// MME
																			// Group
																			// ID
			off += 2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// MME Code
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// MME UE
																		// S1AP ID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// eNB ID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Cell ID
			off += 4;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// Time
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");// MR type
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// PHR
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// eNB
																			// Received
																			// Power
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");// UL SINR
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// TA
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// AoA
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Serving
																			// Freq
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");// Serving RSRP
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Serving RSRQ
			off += 1;
			int neighborCellNum = ConvToByte.byteToUnsignedByte(buffer, off);
			sb.append(neighborCellNum ).append("|");// Neighbor Cell Number
			off += 1;
			// System.out.print("uemr sb1=============" + sb.toString());
			for (int n = 0; n < neighborCellNum; n++) {
				sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Neighbor
																				// 1
																				// Cell
																				// PCI
				off += 2;
				sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Neighbor
																				// 1
																				// Freq
				off += 2;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Neighbor
																			// 1
																			// RSRP
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Neighbor
																			// 1
																			// RSRQ
				off += 1;
			}
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}

		private KeyedMessage<String, String> s6aDataHandler( byte[] buffer, int off,SimpleDateFormat sdf, String topicName) {
			StringBuilder sb = new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City
			off += 2;
			sb.append(buffer[off] ).append("|");// Interface
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR
																			// ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT
			off += 1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN
			off += 16;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Type
			off += 1;
			sb.append(sdf.format(ConvToByte.byteToLong(buffer, off))).append("|");// Procedure Start
																// Time
			off += 8;
			sb.append(sdf.format(ConvToByte.byteToLong(buffer, off))).append("|");// Procedure End
																// Time
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Cause
			off += 2;
			sb.append(ConvToByte.getIpv4(buffer, off)).append("|");// USER_IPv4
			off += 4;
			sb.append(ConvToByte.getIpv6(buffer, off)).append("|");// USER_IPv6
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// MME Address
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// HSS Address
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// MME Port
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// HSS Port
			off += 2;
			sb.append(ConvToByte.getHexString(buffer, off, 44)).append("|");// Origin-Realm
			off += 44;
			sb.append(ConvToByte.getHexString(buffer, off, 44)).append("|");// Destination-Realm
			off += 44;
			sb.append(ConvToByte.getHexString(buffer, off, 64)).append("|");// Origin-Host
			off += 64;
			sb.append(ConvToByte.getHexString(buffer, off, 64)).append("|");// Destination-Host
			off += 64;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, 4)).append("|");// Application-ID
			off += 4;
			sb.append((buffer[off] & 0xff)).append("|");// Subscriber-Status
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Access-Restriction-Data
			off += 1;
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}

		private KeyedMessage<String, String> sgsDataHandler(byte[] buffer, int off,SimpleDateFormat sdf, String topicName) {
			//List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String, String>>();
			StringBuilder sb = new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City
			off += 2;
			sb.append(buffer[off] ).append("|");// Interface
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR
																			// ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT
			off += 1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN
			off += 16;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Type
			off += 1;
			sb.append(sdf.format(ConvToByte.byteToLong(buffer, off))).append("|");// Procedure Start
																// Time
			off += 8;
			sb.append(sdf.format(ConvToByte.byteToLong(buffer, off))).append("|");// Procedure End
																// Time
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Status
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Sgs Cause
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Reject Cause
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// CP Cause
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// RP Cause
			off += 1;
			sb.append(ConvToByte.getIpv4(buffer, off)).append("|");// USER_IPv4
			off += 4;
			sb.append(ConvToByte.getIpv6(buffer, off)).append("|");// USER_IPv6
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// MME IP Add
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// MSC Server OP Add
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// MME Port
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// MSC
																			// Server
																			// Port
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");// Service Indicator
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, 55)).append("|");// MME Name
			off += 55;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// TMSI
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// New LAC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Old LAC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// TAC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Cell ID
			off += 4;
			sb.append(ConvToByte.getHexString(buffer, off, 24)).append("|");// Calling ID
			off += 24;
			sb.append((buffer[off] & 0xff)).append("|");// VLR Name Length
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, buffer.length)).append("|");
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}

		private KeyedMessage<String, String> cellmrDataHandler( byte[] buffer, int off,SimpleDateFormat sdf, String topicName) {
			StringBuilder sb = new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City
			off += 2;
			sb.append(buffer[off] ).append("|");// Interface
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR
																			// ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT
			off += 1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN
			off += 16;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// eNB ID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Cell ID
			off += 4;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// Time
			off += 8;
			sb.append(ConvToByte.getHexString(buffer, off, 20)).append("|");// eNB
																		// Received
																		// Interfere
			off += 20;
			sb.append(ConvToByte.getHexString(buffer, off, 9)).append("|");// UL Packet
																		// Loss
			off += 9;
			sb.append(ConvToByte.getHexString(buffer, off, 9)).append("|");// DL packet
																		// Loss
			off += 9;
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}

		/**
		 * 17	Gm接口/Mw接口/Mg接口/Mi接口/Mj接口/ISC接口XDR数据结构
		 * @param buffer
		 * @param off
		 * @return
		 */
		private KeyedMessage<String, String> gm_mw__mg_mi_mj_isc_DataHandler(byte[] buffer,int off, String topicName){
			StringBuilder sb=new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			//公共信息
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
			off+=2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+2,true)).append("|");//City
			off+=2;
			sb.append(buffer[off]+"|");//Interface
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+16)).append("|");//XDR ID
			off+=16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//RAT
			off+=1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMEI
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+16,false)).append("|");//MSISDN
			off+=16;
			
			// Gm接口/Mw接口/Mg接口/Mi接口/Mj接口/ISC接口信息
			sb.append((buffer[off] & 0xff)).append("|");    //Procedure Type
			off += 1;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");    //Procedure Start Time
			off += 8;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");    //Procedure End Time
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");    //Service Type
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");    //Procedure Status
			off += 1;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");    //CALLING_NUMBER
			off += 16;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");    //CALLED_NUMBER
			off += 16;
			sb.append(new String(buffer,off,32).trim()).append("|");    //CALLING_PARTY_URI
			off += 32;
			sb.append(new String(buffer,off,32).trim()).append("|");    //REQUEST _URI
			off += 32;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //USER_IP
			off += 16;
			sb.append(ConvToByte.getHexString(buffer, off, 128)).append("|");    //CALLID
			off += 128;
			sb.append(ConvToByte.getHexString(buffer, off, 64)).append("|");    //ICID
			off += 64;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //SOURCE_NE_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");    //SOURCE_NE_PORT
			off += 2;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //DEST_NE_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");    //DEST_NE_PORT
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");    //CALL_SIDE
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");    //SOURCE_ACCESS_TYPE
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //SOURCE_ECI
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");    //SOURCE_TAC
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");    //SOURCE_IMSI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");    //SOURCE_IMEI
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");    //DEST_ACCESS_TYPE
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //DEST_ECI
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");    //DEST_TAC
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");    //DEST_IMSI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");    //DEST_IMEI
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");    //AUTH_TYPE
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //EXPIRES_TIME_REQ
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //EXPIRES_TIME_RSP
			off += 4;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //CALLING_SDP_IP_ADDR
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");    //CALLING_AUDIO_SDP_PORT
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");    //CALLING_VIDEO_SDP_PORT
			off += 2;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //CALLED_SDP_IP_ADDR
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");    //CALLED_AUDIO_SDP_PORT
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");    //CALLED_VIDEO_PORT
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");    //AUDIO_CODEC
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");    //VIDEO_CODEC
			off += 1;
			sb.append(new String(buffer,off,32).trim()).append("|");    //REDIRECTING_PARTY_ADDRESS
			off += 32;
			sb.append(new String(buffer,off,32).trim()).append("|");    //ORIGINAL_PARTY_ADDRESS
			off += 32;
			sb.append((buffer[off] & 0xff)).append("|");    //REDIRECT_REASON
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");    //RESPONSE_CODE
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");    //FINISH_WARNING_CODE
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");    //FINISH_REASON_PROTOCOL
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");    //FINISH_REASON_CAUSE
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //FIRFAILTIME
			off += 4;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //FIRST_FAIL_NE_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //ALERTING_TIME
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //ANSWER_TIME
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //RELEASE_TIME
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //CALL_DURATION
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //AUTH_REQ_TIME
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");    //AUTH_RSP_TIME
			off += 4;
			sb.append(new String(buffer,off,24).trim()).append("|");    //STN_SR
			off += 24;
			sb.append(ConvToByte.getHexString(buffer, off, 64)).append("|");    //ATCF_MGMT
			off += 64;
			sb.append(ConvToByte.getHexString(buffer, off, 64)).append("|");    //ATU_STI
			off += 64;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //C_MSISDN
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");    //SSI
			off += 16;

			
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}
		
		/**
		 * 18	Sv接口XDR数据结构 解析报错
		 * @param buffer
		 * @param off
		 * @return
		 * @throws UnsupportedEncodingException 
		 */
		private KeyedMessage<String, String> svDataHandler(byte[] buffer,int off, String topicName) {
			StringBuilder sb=new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			//System.out.println("接收数据："+ConvToByte.getHexString(buffer, 0, buffer.length));
			//公共信息
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
			off+=2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+2,true)).append("|");//City
			off+=2;
			sb.append(buffer[off]+"|");//Interface
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+16)).append("|");//XDR ID
			off+=16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//RAT
			off+=1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMEI
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+16,false)).append("|");//MSISDN
			off+=16;
			
			// Sv接口信息
			sb.append((buffer[off] & 0xff)).append("|");//Procedure Type
			off += 1;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure Start Time
			off += 8;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure End Time
			off += 8;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");//SOURCE_NE_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//SOURCE_NE_PORT
			off += 2;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");//DEST_NE_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//DEST_NE_PORT
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");//ROAM_DIRECTION
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//HOME_MCC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//HOME_MNC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//MCC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//MNC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//TARGET_LAC 
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//SOURCE_TAC
			off += 2;
			sb.append(ConvToByte.getHexString(buffer,off,off+4)).append("|");//SOURCE_ECI
			off += 4;
			sb.append((buffer[off] & 0xff)).append("|");//SV_FLAGS
			off += 1;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");//UL_C_MSC_IP
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");//DL_C_MME_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//UL_C_MSC_TEID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//DL_C_MME_TEID
			off += 4;
			sb.append(ConvToByte.getHexString(buffer,off,off+24)).append("|");//STN_SR
			off += 24;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//TARGET_RNC_ID
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//TARGET_CELL_ID
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");//ARP
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");//REQUEST_RESULT
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");//RESULT
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");//SV_CAUSE
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");//POST_FAILURE_CAUSE
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//RESP_DELAY
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//SV_DELAY
			off += 4;

//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}
		
		/**
		 * 19	Cx/Dx/Sh/Dh接口/Zh接口XDR数据结构
		 * @param buffer
		 * @param off
		 * @return
		 */
		private KeyedMessage<String, String> cx_dx_sh_dh_DataHandler(byte[] buffer,int off, String topicName){
			StringBuilder sb=new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			//公共信息
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
			off+=2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+2,true)).append("|");//City
			off+=2;
			sb.append(buffer[off]+"|");//Interface
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+16)).append("|");//XDR ID
			off+=16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//RAT
			off+=1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMEI
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+16,false)).append("|");//MSISDN
			off+=16;
			
			// Cx/Dx/Sh/Dh接口/Zh接口信息
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure Start Time
			off += 8;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure End Time 
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");//TRANSACTION_TYPE
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");//TRANSACTION_STATUS
			off += 1;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");//SOURCE_NE_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//SOURCE_NE_PORT
			off += 2;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");//DESTINATION_NE_IP
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer,off)).append("|");//DESTINATION_NE_PORT
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//RESULT_CODE
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//EXPERIMENTAL_RESULT_CODE
			off += 4;
			sb.append(new String(buffer,off,44).trim()).append("|");//ORIGIN_REALM
			off += 44;
			sb.append(new String(buffer,off,44).trim()).append("|");//DESTINATION_REALM
			off += 44;
			sb.append(new String(buffer,off,64).trim()).append("|");//ORIGIN_HOST
			off += 64;
			sb.append(new String(buffer,off,64).trim()).append("|");//DESTINATION_HOST
			off += 64;

//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}
		
		/**
		 * 20	Gx接口/Rx接口XDR数据结构
		 * @param buffer
		 * @param off
		 * @return
		 */
		private KeyedMessage<String, String> gx_rx_DataHandler(byte[] buffer,int off, String topicName){
			StringBuilder sb=new StringBuilder();
			KeyedMessage<String, String> km = null;
			
			//公共信息
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
			off+=2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+2,true)).append("|");//City
			off+=2;
			sb.append(buffer[off]+"|");//Interface
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+16)).append("|");//XDR ID
			off+=16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//RAT
			off+=1;
			String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
			sb.append(imsi).append("|");// IMSI byte
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMEI
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+16,false)).append("|");//MSISDN
			off+=16;
			
			// Gx/Rx接口信息
			sb.append((buffer[off] & 0xff)).append("|");//Procedure Type
			off += 1;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure Start Time
			off += 8;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");//Procedure End Time
			off += 8;
			sb.append(new String(buffer,off,64).trim()).append("|");//ICID
			off += 64;
			sb.append(new String(buffer,off,44).trim()).append("|");//ORIGIN_REALM
			off += 44;
			sb.append(new String(buffer,off,44).trim()).append("|");//DESTINATION_REALM
			off += 44;
			sb.append(new String(buffer,off,64).trim()).append("|");//ORIGIN_HOST
			off += 64;
			sb.append(new String(buffer,off,64).trim()).append("|");//DESTINATION_HOST
			off += 64;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");//SGSN_SGW_SIG_IP
			off += 16;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, true)).append("|");//AF_APP_ID
			off += 16;
			sb.append((buffer[off] & 0xff)).append("|");//CC_REQUEST_TYPE
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");//RX_REQUEST_TYPE
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//MEDIA_TYPE
			off += 4;
			sb.append((buffer[off] & 0xff)).append("|");//ABORT_CAUSE
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//RESULT_CODE
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//EXPERIMENTAL_RESULT_CODE
			off += 4;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");//SESSION_RELEASE_CAUSE
			off += 4;
			
//			sb.deleteCharAt(sb.lastIndexOf("|"));
			sb.append(new Date());
			
            km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
			
			return km;
		}
		
		@SuppressWarnings("unused")
		private List<KeyedMessage<String, String>> normallDataHandler(String topic, byte[] buffer, int off, String date,
				String time_stamp,SimpleDateFormat sdf) {
			String[] messages = new String(buffer, off, buffer.length - off).split("\\r\\n");
			List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String, String>>();
			for (String message : messages) {
				if ("0".equals(time_stamp)) {
					message += "|" + date;
				}
				KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic, message);
				list.add(km);
				long num = sendTopicNum.get(topic) == null ? 0 : sendTopicNum.get(topic);
				sendTopicNum.put(topic, num + 1);
			}
			return list;
		}
		private String getOtherMessageCommon(byte[] buffer, int off){
			StringBuilder sb = new StringBuilder();
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");//Length
			off+=2;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+2,true)).append("|");//City
			off+=2;
			sb.append(buffer[off]).append("|");//Interface
			off+=1;
			sb.append(ConvToByte.getHexString(buffer,off,off+16)).append("|");//XDR ID
			off+=16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer,off)).append("|");//RAT
			off+=1;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMSI
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+8,true)).append("|");//IMEI
			off+=8;
			sb.append(ConvToByte.decodeTBCD(buffer,off,off+16,false)).append("|");//MSISDN
			off+=16;
			return sb.toString();
		}
		
		public HashMap<String, Long> getSendTopicNum() {
	    	return sendTopicNum;
	    }

}
