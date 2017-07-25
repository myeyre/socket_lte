package com.asiainfo.test;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import  com.asiainfo.sdtp.*;
public class test {
	
	public static void main(String[] args) {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
		format.format(new Date());
		try {
			FileWriter fw = new FileWriter("D:/"+format.format(new Date())+".txt");
			fw.write("aas\n");
			fw.flush();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main1(String[] args) throws IOException, NoSuchAlgorithmException {
//		String as="3330377C3031307C31317C66663737306163663031383761313033303032356339393030303030376635347C367C3436303037353035343637373135357C383634353130303239313433363330307C31373836323132393235346666666666666666666666666666666666666666667C317C3130302E37302E3235352E38367C3130302E37302E3232342E3131397C323135327C323135327C313636323331373935377C323134393837343636397C307C39343836393236317C434D4E45542E6D6E633030372E6D63633436302E677072737C3130307C313433333132383431383535367C313433333132383435343939397C327C4646464646464646467C333131307C3235357C327C31302E3130362E37362E3231347C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C34393532327C3235357C3131372E3133352E3132382E3134397C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C38307C313235347C3539357C367C357C307C307C307C307C31387C34327C307C307C32307C32327C31317C313335327C317C317C310D0A3330377C3031307C31317C66663737306163663031383761313033303032356362323030303030376635347C367C3436303037353035343337313838357C383635313938303238363832393230307C31373836323030373239336666666666666666666666666666666666666666667C317C3130302E37302E3235352E38317C3130302E37302E3233342E39347C323135327C323135327C313738323635383237337C313030363637383736337C307C39343136383538397C434D4E45542E6D6E633030372E6D63633436302E677072737C3130307C313433333132383537343632357C313433333132383537353039307C327C4646464646464646467C3330367C3235357C327C31302E3138322E36382E3136347C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C34373835307C3235357C3138302E39372E3138322E3133357C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C3434337C313234327C31313034347C387C31327C307C307C317C307C3130397C37367C307C307C35307C3133327C3435357C313335327C317C317C320D0A3330377C3031307C31317C66663737306163663031383761323033303032316363303630303030376635347C367C3436303032353030353738393332337C333532363235303634353035363830317C31383330353330313136306666666666666666666666666666666666666666667C317C3130302E37302E3235352E38337C3130302E39312E3234332E38357C323135327C323135327C3136343336373336357C313330393338383338387C307C39353634383737397C636D6E65747C3130307C313433333132383432363430317C313433333132383435343939367C327C4646464646464646467C3336327C3235357C327C31302E35322E3235312E39337C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C34363231397C3235357C3131312E33302E3133362E3135327C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C38307C3838307C333132317C377C367C307C307C307C307C32337C32377C307C307C37307C32357C3331357C313335327C317C317C320D0A3330377C3031307C31317C66663030303030303030303030303030303030303030303030303030376635347C367C3436303030393231333634393131337C333536393834303634333439383330337C31333835333432383738376666666666666666666666666666666666666666667C317C3130302E37302E3235352E38387C3130302E39312E3232382E34357C323135327C323135327C353431367C323431383935383037377C307C39343733353837337C434D4E45542E6D6E633030302E6D63633436302E677072737C3130307C313433333132383435323534337C313433333132383435343937317C3078464646467C4646464646464646467C313633317C3235357C327C31302E39342E3135312E3133327C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C36303234317C3235357C31372E3136372E3133372E32397C464646463A464646463A464646463A464646463A464646463A464646463A464646463A464646467C3434337C3435367C313231307C367C367C307C307C307C307C307C307C307C307C307C307C36353533357C313231327C317C317C310D0A3330377C3031307C31317C66663737306163663031383761333035303032306565313630303030376635347C367C3436303032383036393534373133347C333535303636303632313032363230317C31353836333539393538386666666666666666666666666666666666666666667C317C3130302E37302E3235352E38377C3130302E37302E3235312E3231327C323135327C323135327C323838383230373735367C323238393839303636337C307C39363033373133327C6";
//		String as="0200BFFFFF05FE4A84E30100A3384400121FFF0100030664000257352859F368154620267467003104351311F1FFFFFFFFFFFFFFFFFFFF050000014F070683570000014F0706856D00FFFF020EC0143703008CD9118773FFFFFFFF0ADA3B2FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF6446FF31FFFFFFFFFFFFFFFFFFFFFFFF644682A28E3C8E3C54AD0579E402FFFFFFFFFFFF636D6E65742E6D6E633030322E6D63633436302E67707273000000000000000000";
//		String as="0200CB503305FEFF4801020AA370440028123E0200270664005235939389F068948020020237878163346604F2FFFFFFFFFFFFFFFFFFFF140000014F2B94D60E0000014F2B94D638000014FFFF00FFFFFF00C13C72FFFFFF63383135FFFF8CC8150F29FFFFFFFF0A444B2AFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF6446FF31FFFFFFFFFFFFFFFFFFFFFFFF644670288E3C8E3C633B059B7102FFFFFFFFFFFF636D6E65742E6D6E633030322E6D63633436302E67707273000000000000000000";
		String as="FE";
		int off=1;
		byte[] buffer=new byte[as.length()/2];
		StringBuilder sb=new StringBuilder();
//		System.out.println("as.length="+as.length());
		
		for(int i=0;i<as.length()/2;i++){
			int pos=2*i;
			buffer[i] = (byte) (charToByte(as.charAt(pos)) << 4 | charToByte(as.charAt(pos+1)));
		}
		System.out.print(buffer[0] & 0xff);
		System.exit(-1);
		Random random=new Random();
		int[] count=new int[10];
		for(int i=0;i<100000;i++){
			count[random.nextInt(10)]+=1;

		}
		for(int i=0;i<10;i++){
			System.out.print(count[i]+",");
		}
		System.out.print("\n");
//		String a="321|0531|5|fe5d43cf0104912844000d220e000005|6|460004194636906|8653720291999200|13906414156fffffffffffffffffffff|2|1438758814014|1438758814054|0|65535|255|79711555|768|80|ef040170|68623e94|10.171.253.35|FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF|100.70.255.129|100.70.143.188|36412|36412|21745|203246605|65535|4294967295|CMWAP.MNC000.MCC460.GPRS|1|5|255|9|1|108727301|123471647";
//		byte[] str=a.getBytes("utf8");
//		for(byte c:str){
////			System.out.format("%02X", c);
//		}
//		MessageDigest md = MessageDigest.getInstance("SHA-256");
////		int totallength=ConvToByte.byteToShort(buffer, off);
////		off+=2;
////		System.out.println("totallength="+totallength);
//		byte[] login_idarray=new byte[12];
//		System.arraycopy(buffer, 0 , login_idarray, 0, 12);
//		byte[] passwdarray=md.digest("asiainfo123".getBytes());
//		
//		StringBuilder sb=new StringBuilder();
//
//		
//		
//		System.out.println(new String (buffer));
//		HashMap<String,String> topicMap=new HashMap<String,String>();
//		topicMap.put("key", "value");
//		String tmp=topicMap.get("");
//		for (byte c :"asdgfadgasdxzfga".getBytes()){
//	System.out.format("%02X", c);
//		}
//		System.out.println(tmp);
//		System.out.println(Charset.defaultCharset().displayName());   
		
//		sb.append(str);
		
		sb.append(byteToUnsignedShort(buffer, off)+"|");//Length
		off+=2;
		sb.append(decodeTBCD(buffer,off,off+2)+"|");//City
		off+=2;
		sb.append(buffer[off]+"|");//Interface
		off+=1;
		sb.append(getHexString(buffer,off,off+16)+"|");//XDR ID
		off+=16;
		sb.append(byteToUnsignedByte(buffer,off)+"|");//RAT
		off+=1;
		sb.append(decodeTBCD(buffer,off,off+8)+"|");//IMSI
		off+=8;
		sb.append(decodeTBCD(buffer,off,off+8)+"|");//IMEI
		off+=8;
		sb.append(decodeTBCD(buffer,off,off+16)+"|");//MSISDN
		off+=16;
		sb.append((buffer[off]&0xff)+"|");//Procedure Type
		off+=1;
		sb.append(ConvToByte.byteToLong(buffer,off)+"|");//Procedure Start Time
		off+=8;
		sb.append(ConvToByte.byteToLong(buffer,off)+"|");//Procedure End Time
		off+=8;
		sb.append((buffer[off]&0xff)+"|");//Procedure Status
		off+=1;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//Request Cause
		off+=2;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//Failure Cause
		off+=2;
		sb.append((buffer[off]&0xff)+"|");//Keyword 1
		off+=1;
		sb.append((buffer[off]&0xff)+"|");//Keyword 2
		off+=1;
		sb.append((buffer[off]&0xff)+"|");//Keyword 3
		off+=1;
		sb.append((buffer[off]&0xff)+"|");//Keyword 4
		off+=1;
		sb.append(byteToUnsignedInt(buffer,off)+"|");//MME UE S1AP ID
		off+=4;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//Old MME Group ID
		off+=2;
		sb.append(byteToUnsignedByte(buffer,off)+"|");//Old MME Code
		off+=1;
		sb.append(byteToUnsignedInt(buffer,off)+"|");//Old M-TMSI
		off+=4;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//MME Group ID
		off+=2;
		sb.append(byteToUnsignedByte(buffer,off)+"|");//MME Code
		off+=1;
		sb.append(byteToUnsignedInt(buffer,off)+"|");//M-TMSI
		off+=4;
		sb.append(byteToUnsignedInt(buffer,off)+"|");//TMSI
		off+=4;
		sb.append(getIpv4(buffer,off)+"|");//USER_IPv4
		off+=4;
		sb.append(getIpv6(buffer,off)+"|");//USER_IPv6
		off+=16;
		sb.append(getIp(buffer,off)+"|");//MME IP Add
		off+=16;
		sb.append(getIp(buffer,off)+"|");//eNB IP Add
		off+=16;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//MME Port
		off+=2;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//eNB Port
		off+=2;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//TAC
		off+=2;
		sb.append(byteToUnsignedInt(buffer,off)+"|");//Cell ID
		off+=4;
		sb.append(byteToUnsignedShort(buffer,off)+"|");//Other TAC
		off+=2;
		sb.append(byteToUnsignedInt(buffer,off)+"|");//Other ECI
		off+=4;
		sb.append(new String(buffer,off,32).trim()+"|");//APN
		off+=32;
		
		int epsBearerNum=byteToUnsignedByte(buffer,off);
		sb.append(epsBearerNum+"|");//EPS Bearer Number
		off+=1;
		for(int i=0;i<epsBearerNum;i++){
			sb.append(byteToUnsignedByte(buffer,off)+"|");//Bearer 1 ID
			off+=1;
			sb.append(byteToUnsignedByte(buffer,off)+"|");//Bearer 1 Type
			off+=1;
			sb.append(byteToUnsignedByte(buffer,off)+"|");//Bearer 1 QCI
			off+=1;
			sb.append(byteToUnsignedByte(buffer,off)+"|");//Bearer 1 Status
			off+=1;
			sb.append(byteToUnsignedShort(buffer,off)+"|");//Bearer 1 Request Cause
		    off+=2;
		    sb.append(byteToUnsignedShort(buffer,off)+"|");//Bearer 1 Failure Cause
		    off+=2;
		    sb.append(byteToUnsignedInt(buffer,off)+"|");//Bearer 1 eNB GTP-TEID
		    off+=4;
		    sb.append(byteToUnsignedInt(buffer,off)+"|");//Bearer 1 SGW GTP-TEID
		    off+=4;
		}
		
		
		System.out.println(sb.toString());
	}

	 private static byte charToByte(char c) {   
		   return (byte) "0123456789ABCDEF".indexOf(c);   
	 } 
	 
	 public static int byteToUnsignedShort(byte[] b, int off) {
			short s = 0;
			short s0 = (short) (b[(1 + off)] & 0xFF);
			short s1 = (short) (b[off] & 0xFF);
			s1 = (short) (s1 << 8);
			s = (short) (s0 | s1);
			return s&0xFFFF;
		}
	 
	 public static int byteToUnsignedByte(byte[] b, int off) {
			short s1 = (short) (b[off] & 0xFF);
			return s1&0xFFFF;
		}
	  
	 private static String decodeTBCD(byte [] c,int start,int end){ 
		 StringBuilder sb=new StringBuilder();
		 for(int i=start;i<end;i++){
		   String tmp=String.format("%02x",c[i]);   
		   sb.append(tmp.charAt(1));
		   sb.append(tmp.charAt(0));
		 }
		 int str_end=sb.indexOf("f");
		 if(str_end>0)sb.setLength(str_end);
		 return sb.toString();
	 } 
	 
	 private static String getHexString(byte [] c,int start,int end){ 
		 StringBuilder sb=new StringBuilder();
		 for(int i=start;i<end;i++){
		   sb.append(String.format("%02x",c[i]));  
		 }
		 return sb.toString();
	 } 
	 
	 public static long byteToUnsignedInt(byte[] b, int off) {
		    int s = 0;
		    int s0 = b[(3 + off)] & 0xFF;
		    int s1 = b[(2 + off)] & 0xFF;
		    int s2 = b[(1 + off)] & 0xFF;
		    int s3 = b[off] & 0xFF;
			s3 <<= 24;
			s2 <<= 16;
			s1 <<= 8;
			s = s0 | s1 | s2 | s3;
			return s&0xFFFFFFFFL;
		}
	 public static String getIpv4(byte[] b, int off) {
		    String ip = "";
		    int s0 = b[(3 + off)] & 0xFF;
		    int s1 = b[(2 + off)] & 0xFF;
		    int s2 = b[(1 + off)] & 0xFF;
		    int s3 = b[off] & 0xFF;
			
		    ip = s0 +"."+ s1 +"."+ s2+"."+ s3;
			return ip;
		}
	 public static String getIpv6(byte[] b, int off) {
		    String ip = "";
		    for(int i=off;i<off+16;i++){
		    	ip +=String.format("%02X",b[i]);
		    	if (i!=off&&(i-off)%2==1 && i!=off+15){
		    		ip +=":";
		    	}
		    }
			return ip;
		}
	 public static String getIp(byte[] b, int off) {

		    int start=0;
		    StringBuilder sb=new StringBuilder();
		    String tmp=String.format("%02X",b[off]);
	    	if("FF".equals(tmp)){
	    		start=off+12;
	    	}else{
	    		start=off;
	    	}
		    for(int i=start;i<off+16;i++){
		    	if(start!=off){
		    		sb.append(b[i]&0xff);
		    		if (i!=off+15)sb.append(".");
		    	}
		    	if(start==off){
		    		sb.append(String.format("%02X",b[i]));
		    		if (i!=start&&(i-start)%2==1&&i!=off+15)sb.append(":");
		    	}
		    }

			return sb.toString();
		}
}
