package com.asiainfo.test.nio;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;

public class Client {
	public void start() {
		try {
			SocketAddress address = new InetSocketAddress("localhost", 55555);
			SocketChannel client = SocketChannel.open(address);
			client.configureBlocking(false);
			String a = "asdasdasdasddffasfas";
			ByteBuffer buffer = ByteBuffer.allocate(20);
			buffer.put(a.getBytes());
			buffer.clear();
//			int d = client.write(buffer);
			System.out.println("发送数据: " + new String(buffer.array()));
			while (true) {
				buffer.flip();
				int i = client.read(buffer);
				if (i > 0) {
					byte[] b = buffer.array();
					System.out.println("接收数据: " + new String(b));
					client.close();
					System.out.println("连接关闭!");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
