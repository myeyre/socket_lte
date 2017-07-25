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
					// System.out.println(get + "�������");
				}
				if (reader != null) {
					String line = reader.readLine();
					System.out.println("�ӷ�����������Ϣ��" + line);

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
	 * @return ��������ת��Ϊʮ�������ַ����
	 */
	public static String BinaryToHexString(byte[] bytes) {

		String result = "";
		String hex = "";
		for (int i = 0; i < bytes.length; i++) {
			// �ֽڸ�4λ
			hex = String.valueOf(hexStr.charAt((bytes[i] & 0xF0) >> 4));
			// �ֽڵ�4λ
			hex += String.valueOf(hexStr.charAt(bytes[i] & 0x0F));
			result += hex;
		}
		return result;
	}

	/**
	 * 
	 * @param hexString
	 * @return ��ʮ������ת��Ϊ�ֽ�����
	 */
	public static byte[] HexStringToBinary(String hexString) {
		// hexString�ĳ��ȶ�2ȡ������Ϊbytes�ĳ���
		int len = hexString.length() / 2;
		byte[] bytes = new byte[len];
		byte high = 0;// �ֽڸ���λ
		byte low = 0;// �ֽڵ���λ

		for (int i = 0; i < len; i++) {
			// ������λ�õ���λ
			high = (byte) ((hexStr.indexOf(hexString.charAt(2 * i))) << 4);
			low = (byte) hexStr.indexOf(hexString.charAt(2 * i + 1));
			bytes[i] = (byte) (high | low);// �ߵ�λ��������
		}
		return bytes;
	}
}
