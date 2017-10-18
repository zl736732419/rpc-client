package com.zheng.rpc;

import com.zheng.rpc.common.encrypt.RpcDecoder;
import com.zheng.rpc.common.encrypt.RpcEncoder;
import com.zheng.rpc.common.params.RpcRequest;
import com.zheng.rpc.common.params.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.CountDownLatch;

/**
 * 远程调用客户端
 * Created by zhenglian on 2017/10/17.
 */
public class RpcClient extends SimpleChannelInboundHandler<RpcResponse>  {
    private CountDownLatch latch;
    private String host;
    private Integer port;

    /**
     * 响应结果
     */
    private RpcResponse response;
    
    public RpcClient(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    /**
     * 客户端发送远程调用消息
     * @param request
     * @return
     */
    public RpcResponse send(RpcRequest request) {
        latch = new CountDownLatch(1); // 这里为了同步阻塞获取调用结果
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            // 向pipeline中添加编码、解码、业务处理的handler
                            channel.pipeline()
                                    .addLast(new RpcEncoder(RpcRequest.class))  //OUT - 1
                                    .addLast(new RpcDecoder(RpcResponse.class)) //IN - 1
                                    .addLast(RpcClient.this);                   //IN - 2
                        }
                    });
            ChannelFuture future = bootstrap.connect(host, port).sync();
            //将request对象写入outbundle处理后发出（即RpcEncoder编码器）
            future.channel().writeAndFlush(request).sync();
            latch.await(); // 同步阻塞等待调用结果
            if (response != null) {
                future.channel().closeFuture().sync();
            }
            return response;
        } catch(Exception e) {
            throw new RuntimeException("调用远程服务器服务发生错误，错误信息: " + e.getLocalizedMessage());  
        } finally {
            group.shutdownGracefully();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse msg) throws Exception {
        this.response = msg;
        latch.countDown();
    }
}
