/*
 * @Author: LHD
 * @Date: 2024-09-09 16:33:37
 * @LastEditors: 308twin 790816436@qq.com
 * @LastEditTime: 2024-09-10 14:18:06
 * @Description: 
 * 
 * Copyright (c) 2024 by 308twin@790816436@qq.com, All Rights Reserved. 
 */
package btree4j.server;

import java.io.*;
import java.net.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {
    private int port;

    public SocketServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Server is running on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected!");
            handleClient(clientSocket);
        }
    }

     private void handleClient(Socket clientSocket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            System.out.println("Received from client: " + inputLine);
            out.println("Echo: " + inputLine);  // 回传数据给客户端
        }

        clientSocket.close(); // 关闭客户端连接
    }
}
