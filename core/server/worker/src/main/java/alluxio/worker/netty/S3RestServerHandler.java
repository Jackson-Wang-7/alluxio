package alluxio.worker.netty;

//import alluxio.network.netty.FileTransferType;
//import alluxio.worker.dora.DoraWorker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

//import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

/**
 * worker netty server handler.
 */
public class S3RestServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
//  private static final Logger LOG = LoggerFactory.getLogger(S3RestServerHandler.class);
//  private final DoraWorker mDoraWorker;
  /** The executor to run {@link AbstractReadHandler.PacketReader}. */
//  private final ExecutorService mPacketReaderExecutor;
  /**
   * The transfer type used by the data server.
   */
//  private final FileTransferType mTransferType;

  /**
   * Creates an instance of {@link S3RestServerHandler}.
   *
   */
  public S3RestServerHandler() {
  }

//  /**
//   * Creates an instance of {@link S3RestServerHandler}.
//   *
//   * @param executorService the executor service to run data readers
//   * @param worker block worker
//   * @param fileTransferType the file transfer type
//   */
//  public S3RestServerHandler(ExecutorService executorService,
//                             DoraWorker worker, FileTransferType fileTransferType) {
//    mPacketReaderExecutor = executorService;
//    mDoraWorker = worker;
//    mTransferType = fileTransferType;
//  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest object) throws Exception {
//    if (!(object instanceof FullHttpRequest)) {
//      ctx.fireChannelRead(object);
//      return;
//    }
    FullHttpRequest request = (FullHttpRequest) object;

    if (HttpUtil.is100ContinueExpected(request)) {
      ctx.write(
          new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.CONTINUE));
    }
//
//    try (LockResource lr = new LockResource(mLock)) {
//      Preconditions.checkState(mContext == null || !mContext.isPacketReaderActive());
//      mContext = createRequestContext(msg);
//    }

    if (request.method().equals(HttpMethod.GET)) {
      getObject(ctx, request);
    }
  }

  private void getObject(ChannelHandlerContext ctx, FullHttpRequest request) {
    return;
  }

//  private void getObject(ChannelHandlerContext ctx, FullHttpRequest request)
//      throws S3Exception, IOException, AlluxioException {
//    String uri = request.uri();
//    String authentication = request.headers().get("");
//    String user = NettyRestUtil.getUserFromAuthorization(authentication, Configuration.global());
//    String range = request.headers().get("Range");
//    S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
//    FileSystem userFs = NettyRestUtil.createFileSystemForUser(user, mFsClient);
//    AlluxioURI objectUri = new AlluxioURI(uri);
//
//    //todo(wyy) parse range read
//    URIStatus status;
//    status = userFs.getStatus(objectUri);
//    //todo: Judge whether it is local data
//    long offset = s3Range.getOffset(status.getLength());
//    long blockSize = status.getBlockSizeBytes();
//    long blockId = status.getBlockIds().get(0);
//
//    HttpResponse response =
//        new DefaultHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
//    response.headers().set(HttpHeaderNames.CONTENT_TYPE,
//        NettyRestUtil.deserializeContentType(status.getXAttr()));
//    response.headers()
//        .set(HttpHeaderNames.LAST_MODIFIED, new Date(status.getLastModificationTimeMs()));
//    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, s3Range.getLength(status.getLength()));
//
//    String entityTag = NettyRestUtil.getEntityTag(status);
//    if (entityTag != null) {
//      response.headers().set(S3Constants.S3_ETAG_HEADER, entityTag);
//    } else {
//      LOG.debug("Failed to find ETag for object: " + objectUri);
//    }
//
//    TaggingData tagData = NettyRestUtil.deserializeTags(status.getXAttr());
//    if (tagData != null) {
//      int taggingCount = tagData.getTagMap().size();
//      if (taggingCount > 0) {
//        response.headers().set(S3Constants.S3_TAGGING_COUNT_HEADER, taggingCount);
//      }
//    }
//    boolean keepAlive = HttpUtil.isKeepAlive(request);
//    if (keepAlive) {
//      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
//    }
//
//    BlockInfo blockInfo = status.getBlockInfo(blockId);
//    Optional<BlockMeta> meta = mBlockWorker.getBlockStore().getVolatileBlockMeta(blockId);
//    if (!meta.isPresent()) {
//      throw new IOException();
//    }
//    String fileLocalPath = meta.get().getPath();
//    RandomAccessFile file = new RandomAccessFile(fileLocalPath, "r");
//
//    ctx.write(response);
//    ctx.write(new DefaultFileRegion(file.getChannel(), 0, file.length()));
//    ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
//    if (!keepAlive) {
//      future.addListener(ChannelFutureListener.CLOSE);
//    }
//  }
}
