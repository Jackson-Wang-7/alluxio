package alluxio.web;

import alluxio.client.file.FileSystem;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

/**
 * worker netty server initializer.
 */
public class WorkerNettyServerInitializer extends ChannelInitializer<SocketChannel> {

  private final WorkerProcess mWorkerProcess;

  private final FileSystem mFileSystem;

  /**
   * @param fileSystem the Alluxio file system
   * @param workerProcess the Alluxio worker process
   */
  public WorkerNettyServerInitializer(FileSystem fileSystem, WorkerProcess workerProcess) {
    mFileSystem = fileSystem;
    mWorkerProcess = workerProcess;
  }

  @Override
  protected void initChannel(SocketChannel channel) throws Exception {
    ChannelPipeline pipeline = channel.pipeline();
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast("httpAggregator", new HttpObjectAggregator(512 * 1024));
    pipeline.addLast(
        new WorkerNettyServerHandler(mFileSystem, mWorkerProcess.getWorker(BlockWorker.class)));
  }
}
