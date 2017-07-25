package com.asiainfo.test;

import java.security.MessageDigest;

public class sha256 {
	public static void main(String[] args) {
		String t = "passwd";
		String login_id="user        ";
		String timestamp="1289959463";
		
	   byte[]login=login_id.getBytes();
	   System.out.println(login.length);
		StringBuilder sb=new StringBuilder();
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(t.getBytes());
			byte[] dest=md.digest();
			System.out.println(dest.length);
			for (byte b : dest)//digest方法只能被调用一次，调用后md会恢复到初始设置
				sb.append(String.format("%02x", b));
			System.out.println(sb);
			
			String str=login_id+sb.toString()+timestamp+"rand=99";
			byte[] strb=str.getBytes();
			byte[] dp=new byte[login.length+16+strb.length];
			int pos = 0;
			System.arraycopy(login, 0, dp, pos, login.length);
			pos+=login.length;
			System.arraycopy(dest, 0, dp, pos, dest.length);
			pos+=dest.length;
			System.arraycopy(strb, 0, dp, pos, strb.length);
//			dp="user        76a2173be6393254e72ffa4d6df1030a1289959463rand=99".getBytes();
			dest=md.digest(strb);
			sb=new StringBuilder();
			for (byte b : dest)//digest方法只能被调用一次，调用后md会恢复到初始设置
				sb.append(String.format("%02x", b));
			System.out.println(sb);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	
	}
}
