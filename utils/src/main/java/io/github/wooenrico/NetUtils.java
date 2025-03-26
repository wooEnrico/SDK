package io.github.wooenrico;

import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 网络工具类
 */
public final class NetUtils {

    /**
     * 获取主机名
     *
     * @return 主机名
     * @throws UnknownHostException 异常
     */
    public static String getInet4HostName() throws UnknownHostException {
        return Inet4Address.getLocalHost().getCanonicalHostName();
    }

    /**
     * 获取主机名
     *
     * @return 主机名
     * @throws UnknownHostException 异常
     */
    public static String getInet6HostName() throws UnknownHostException {
        return Inet6Address.getLocalHost().getCanonicalHostName();
    }

    /**
     * 获取ipv6 网络地址
     *
     * @return 网络地址列表
     * @throws SocketException 异常
     */
    public static List<Inet6Address> getLocalInet6Address() throws SocketException {
        return getLocalInetAddress(Inet6Address.class);
    }

    /**
     * 获取ipv4 网络地址
     *
     * @return 网络地址列表
     * @throws SocketException 异常
     */
    public static List<Inet4Address> getLocalInet4Address() throws SocketException {
        return getLocalInetAddress(Inet4Address.class);
    }

    /**
     * 根据网络类型获取网络地址
     *
     * @param tClass 网络类型类
     * @param <T>    泛型
     * @return 网络地址列表
     * @throws SocketException 异常
     */
    public static <T> List<T> getLocalInetAddress(Class<T> tClass) throws SocketException {
        List<InetAddress> localInetAddress = getLocalInetAddress();
        if (localInetAddress.isEmpty()) {
            return Collections.emptyList();
        }
        return localInetAddress.stream()
                .filter(inetAddress -> inetAddress.getClass().equals(tClass))
                .map(inetAddress -> (T) inetAddress)
                .collect(Collectors.toList());
    }

    /**
     * 获取本地网络地址
     *
     * @return 网络地址列表
     * @throws SocketException 异常
     */
    public static List<InetAddress> getLocalInetAddress() throws SocketException {
        List<InetAddress> list = new ArrayList<>();
        Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> ips = ni.getInetAddresses();
            while (ips.hasMoreElements()) {
                InetAddress inetAddress = ips.nextElement();
                list.add(inetAddress);
            }
        }
        return list;
    }
}
