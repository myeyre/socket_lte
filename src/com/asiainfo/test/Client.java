package com.asiainfo.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class Client extends Thread {

	Socket sk = null;
	BufferedReader reader = null;
	// PrintWriter wtr = null;
	BufferedWriter wtr = null;
	BufferedReader keyin = null;
	private static String hexStr = "0123456789ABCDEF";

	public Client() {
		keyin = new BufferedReader(new InputStreamReader(System.in));
		try {
			sk = new Socket("192.168.137.101", 1987);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void run() {
		try {
			reader = new BufferedReader(new InputStreamReader(sk.getInputStream(), "utf-8"));
			// wtr = new PrintWriter(sk.getOutputStream());
			wtr = new BufferedWriter(new OutputStreamWriter(sk.getOutputStream(), "utf-8"));

			while (true) {
				String get = keyin.readLine();
				if (get != null & get.length() > 0) {
					// wtr.println(get);
					wtr.write(get + "\n");
					wtr.flush();
					// wtr.close();
					// System.out.println(get + "发送完毕");
				}
				if (reader != null) {
					String line = reader.readLine();
					System.out.println("从服务器来的信息：" + line);

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		byte t = HexStringToBinary("9E")[0];
		t = Byte.decode("0x2f");
		t = (byte) 0x9e;
//		byte b[] = new byte[] { 0x2f, 0x12 };

		System.out.println(t);
		System.out.println(BinaryToHexString(new byte[] { t }));
		new Client().start();
	}

	/**
	 * 
	 * @param bytes
	 * @return 将二进制转换为十六进制字符输出
	 */
	public static String BinaryToHexString(byte[] bytes) {

		String result = "";
		String hex = "";
		for (int i = 0; i < bytes.length; i++) {
			// 字节高4位
			hex = String.valueOf(hexStr.charAt((bytes[i] & 0xF0) >> 4));
			// 字节低4位
			hex += String.valueOf(hexStr.charAt(bytes[i] & 0x0F));
			result += hex;
		}
		return result;
	}

	/**
	 * 
	 * @param hexString
	 * @return 将十六进制转换为字节数组
	 */
	public static byte[] HexStringToBinary(String hexString) {
		// hexString的长度对2取整，作为bytes的长度
		int len = hexString.length() / 2;
		byte[] bytes = new byte[len];
		byte high = 0;// 字节高四位
		byte low = 0;// 字节低四位

		for (int i = 0; i < len; i++) {
			// 右移四位得到高位
			high = (byte) ((hexStr.indexOf(hexString.charAt(2 * i))) << 4);
			low = (byte) hexStr.indexOf(hexString.charAt(2 * i + 1));
			bytes[i] = (byte) (high | low);// 高地位做或运算
		}
		return bytes;
	}
}
