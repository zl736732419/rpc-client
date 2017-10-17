package com.zheng.rpc;

import com.zheng.rpc.common.params.RpcRequest;
import com.zheng.rpc.common.params.RpcResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.UUID;

/**
 * rpc远程服务代理
 * 客户端引用程序需要通过该类来创建代理类
 * 并通过代理类调用远程服务器的服务接口
 * Created by zhenglian on 2017/10/17.
 */
public class RpcProxy {
    
    private String serverAddress;
    
    private ServiceDiscovery serviceDiscovery;

    public RpcProxy(ServiceDiscovery serviceDiscovery) {
        this.serviceDiscovery = serviceDiscovery;
    }
    
    /**
     * 通过动态代理生成指定服务类的远程调用代理类
     * @param interfaceCls
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> T create(Class<T> interfaceCls) {
        T obj = (T) Proxy.newProxyInstance(interfaceCls.getClassLoader(),
                new Class<?>[]{interfaceCls}, new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        //创建RpcRequest，封装被代理类的属性
                        RpcRequest request = new RpcRequest();
                        request.setRequestId(UUID.randomUUID().toString());
                        request.setClassName(method.getDeclaringClass().getName());
                        request.setMethodName(method.getName());
                        request.setParameterTypes(method.getParameterTypes());
                        request.setParameters(args);
                        // 获取zk服务器地址信息，通过netty建立连接发送远程调用请求
                        if (Optional.ofNullable(serviceDiscovery).isPresent()) {
                            serverAddress = serviceDiscovery.discover();
                        }
                        //随机获取服务的地址
                        String[] array = serverAddress.split(":");
                        String host = array[0];
                        int port = Integer.parseInt(array[1]);
                        //创建Netty实现的RpcClient，链接服务端
                        RpcClient client = new RpcClient(host, port);
                        //通过netty向服务端发送请求
                        RpcResponse response = client.send(request);
                        //返回信息
                        if (response.isError()) {
                            throw response.getError();
                        } else {
                            return response.getResult();
                        }
                    }
                });
        return obj;
    }
    
}
