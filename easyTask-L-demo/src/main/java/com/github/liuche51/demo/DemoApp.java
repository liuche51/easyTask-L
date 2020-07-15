package com.github.liuche51.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApp
{
		public static void main(String[] args) {
			SpringApplication.run(DemoApp.class, args);
			System.out.println("===============================================================================");
			System.out.println("============= DemoApp Started ON SpringBoot Success=============");
			System.out.println("===============================================================================");
		}
}
