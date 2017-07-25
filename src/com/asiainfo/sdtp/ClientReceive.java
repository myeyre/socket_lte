package com.asiainfo.sdtp;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientReceive implements Runnable {
	private Socket socket = null;

	public ClientReceive(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		try {
			DataInputStream dis = new DataInputStream(this.socket.getInputStream());
			int readnum;
			int length;
			byte[] totalLengthArray = new byte[2];
			readnum = 0;
			while (readnum < 2) {
				int num = dis.read(totalLengthArray, readnum, 2 - readnum);
				if (num > 0) {
					readnum += num;
				}
			}
			short totalLength = ConvToByte.byteToShort(totalLengthArray, 0);

			length = totalLength - 2;
			byte[] buffer = new byte[length];
			readnum = 0;

			int num = dis.read(buffer, readnum, length - readnum);
			if (num > 0) {
				readnum += num;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
