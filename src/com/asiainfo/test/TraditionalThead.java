package com.asiainfo.test;

public class TraditionalThead {

	private Thread shutdownHook;

	public static void main(String[] args) {

		TraditionalThead b = new TraditionalThead();

		b.addShutdownHook();

		while (true) {

		}

	}

	public void addShutdownHook() {

		shutdownHook = new Thread("BootStrapShutdownHook") {

			public void run() {

				System.out.println("ShutdownHook Executed...");

			}

		};

		Runtime.getRuntime().addShutdownHook(shutdownHook);

	}

}
