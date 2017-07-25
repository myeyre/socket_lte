package com.asiainfo.sdtp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

public class Client {
	private Socket socket = null;
	private String ipaddress = "192.168.137.101";
	private int port = 10101;

	public void service() {
		try {
			for (int i=0;i<4;i++){
				this.socket = new Socket(InetAddress.getByName(this.ipaddress), this.port);
				ClientHandler handler = new ClientHandler(this.socket);
				new Thread(handler).start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Client client = new Client();
		client.service();
	}
}
