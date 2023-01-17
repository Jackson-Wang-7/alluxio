/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.web;

import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.util.network.NettyUtils;
import alluxio.worker.WorkerProcess;

import com.google.common.base.Throwables;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Runs a netty data server that responds to block requests.
 */
public class WorkerNettyWebServer {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerNettyWebServer.class);

  private final ServerBootstrap mBootstrap;
  private final ChannelFuture mChannelFuture;
  private final InetSocketAddress mSocketAddress;
  private final long mQuietPeriodMs =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD);
  private final long mTimeoutMs = 15000;

  /**
   * Creates a new instance of {@link WorkerNettyWebServer}.
   *
   * @param webAddress the server address
   * @param workerProcess the Alluxio worker process
   */
  public WorkerNettyWebServer(InetSocketAddress webAddress, final WorkerProcess workerProcess) {
    mSocketAddress = webAddress;
    mBootstrap = createBootstrap();
    FileSystem fileSystem = FileSystem.Factory.create();
    mBootstrap.handler(new LoggingHandler(LogLevel.DEBUG))
              .childHandler(new WorkerNettyServerInitializer(fileSystem, workerProcess));
    try {
      mChannelFuture = mBootstrap.bind(webAddress).sync();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * stop.
   */
  public void stop() {
    boolean completed;
    completed =
        mChannelFuture.channel().close().awaitUninterruptibly(mTimeoutMs);
    if (!completed) {
      LOG.warn("Closing the channel timed out.");
    }
    completed =
        mBootstrap.group().shutdownGracefully(mQuietPeriodMs, mTimeoutMs, TimeUnit.MILLISECONDS)
            .awaitUninterruptibly(mTimeoutMs);
    if (!completed) {
      LOG.warn("Forced group shutdown because graceful shutdown timed out.");
    }
    completed = mBootstrap.childGroup()
        .shutdownGracefully(mQuietPeriodMs, mTimeoutMs, TimeUnit.MILLISECONDS)
        .awaitUninterruptibly(mTimeoutMs);
    if (!completed) {
      LOG.warn("Forced child group shutdown because graceful shutdown timed out.");
    }
  }

  /**
   * get port.
   * @return port
   */
  public int getLocalPort() {
    return mSocketAddress.getPort();
  }

  /**
   * get host.
   * @return host
   */
  public String getHost() {
    return mSocketAddress.getAddress().getHostAddress();
  }

  private ServerBootstrap createBootstrap() {
    final ServerBootstrap boot = createBootstrapOfType(
        Configuration.getEnum(PropertyKey.WORKER_NETWORK_NETTY_CHANNEL, ChannelType.class));
//    final ServerBootstrap boot = createBootstrapOfType(ChannelType.NIO);

    // use pooled buffers
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    // set write buffer
    // this is the default, but its recommended to set it in case of change in future netty.
    boot.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
        (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_HIGH));
    boot.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
        (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_LOW));

    // more buffer settings on Netty socket option, one can tune them by specifying
    // properties, e.g.:
    // alluxio.worker.network.netty.backlog=50
    // alluxio.worker.network.netty.buffer.send=64KB
    // alluxio.worker.network.netty.buffer.receive=64KB
//    if (Configuration.global().keySet().contains(PropertyKey.WORKER_NETWORK_NETTY_BACKLOG)) {
//      boot.option(ChannelOption.SO_BACKLOG,
//          Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BACKLOG));
//    }
//    if (Configuration.global().keySet().contains(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_SEND)) {
//      boot.option(ChannelOption.SO_SNDBUF,
//          (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_SEND));
//    }
//    if (Configuration.global().keySet().
//      contains(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_RECEIVE)) {
//      boot.option(ChannelOption.SO_RCVBUF,
//          (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_RECEIVE));
//    }
    return boot;
  }

  /**
   * Creates a default {@link ServerBootstrap} where the channel and groups are
   * preset.
   *
   * @param type the channel type; current channel types supported are nio and epoll
   * @return an instance of {@code ServerBootstrap}
   */
  private ServerBootstrap createBootstrapOfType(final ChannelType type) {
    final ServerBootstrap boot = new ServerBootstrap();
    final int bossThreadCount = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);
    // If number of worker threads is 0, Netty creates (#processors * 2) threads by default.
    final int workerThreadCount =
        Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS);
    String dataServerEventLoopNamePrefix = "data-server-tcp-socket";
    final EventLoopGroup bossGroup = NettyUtils
        .createEventLoop(type, bossThreadCount, dataServerEventLoopNamePrefix + "-boss-%d", true);
    final EventLoopGroup workerGroup = NettyUtils
        .createEventLoop(type, workerThreadCount, dataServerEventLoopNamePrefix + "-worker-%d",
            true);

    final Class<? extends ServerChannel> socketChannelClass = NettyUtils.getServerChannelClass(
        false, Configuration.global());

    boot.group(bossGroup, workerGroup).channel(socketChannelClass);
    if (type == ChannelType.EPOLL) {
      boot.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
    }

    return boot;
  }
}
