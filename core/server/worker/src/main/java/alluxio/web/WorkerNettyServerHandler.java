package alluxio.web;

import alluxio.AlluxioURI;
import alluxio.S3RangeSpec;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.proto.journal.File;
import alluxio.wire.BlockInfo;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Optional;

/**
 * worker netty server handler.
 */
public class WorkerNettyServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerNettyServerHandler.class);
  private final FileSystem mFsClient;
  private final DefaultBlockWorker mBlockWorker;

  /**
   * xxxx.
   * @param fileSystem
   * @param blockWorker
   */
  public WorkerNettyServerHandler(FileSystem fileSystem, BlockWorker blockWorker) {
    mFsClient = fileSystem;
    mBlockWorker = (DefaultBlockWorker) blockWorker;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
    if (HttpUtil.is100ContinueExpected(request)) {
      ctx.write(
          new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.CONTINUE));
    }
    if (request.method().equals(HttpMethod.GET)) {
      getObject(ctx, request);
    } else {
      putObject(ctx, request);
    }
  }

  private void putObject(ChannelHandlerContext ctx, FullHttpRequest request)
      throws S3Exception, NoSuchAlgorithmException, IOException, AlluxioException {
    String uri = request.uri();
    String authentication = request.headers().get("");
    String user = NettyRestUtil.getUserFromAuthorization(authentication, Configuration.global());
    FileSystem userFs = NettyRestUtil.createFileSystemForUser(user, mFsClient);
    AlluxioURI objectUri = new AlluxioURI(uri);
    String decodedLength = request.headers().get("x-amz-decoded-content-length");
    String contentLength = request.headers().get(HttpHeaderNames.CONTENT_LENGTH);
    boolean isChunkedEncoding = decodedLength != null;
    long toRead;
    ByteBuf buf = request.content();
    InputStream readStream = new ByteBufInputStream(buf);

    // todo(wyy) chunked encoding read
    // The request body can be in the aws-chunked encoding format, or not encoded at all
    // determine if it's encoded, and then which parts of the stream to read depending on
    // the encoding type.
    if (isChunkedEncoding) {
      toRead = Long.parseLong(decodedLength);
      readStream = new ChunkedEncodingInputStream(new ByteBufInputStream(buf));
    } else {
      toRead = Long.parseLong(contentLength);
    }
    CreateFilePOptions filePOptions =
        CreateFilePOptions.newBuilder()
            .setRecursive(true)
            .setMode(PMode.newBuilder()
                .setOwnerBits(Bits.ALL)
                .setGroupBits(Bits.ALL)
                .setOtherBits(Bits.NONE).build())
            .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
            .build();

    MessageDigest md5 = MessageDigest.getInstance("MD5");
    FileOutStream os = userFs.createFile(objectUri, filePOptions);
    try (DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5)) {
      long read = ByteStreams.copy(ByteStreams.limit(readStream, toRead),
          digestOutputStream);
      if (read < toRead) {
        throw new IOException(String.format(
            "Failed to read all required bytes from the stream. Read %d/%d",
            read, contentLength));
      }
    }

    byte[] digest = md5.digest();
    String base64Digest = BaseEncoding.base64().encode(digest);
    String contentMD5 = request.headers().get("Content-MD5");
    if (contentMD5 != null && !contentMD5.equals(base64Digest)) {
      // The object may be corrupted, delete the written object and return an error.
      try {
        userFs.delete(objectUri, DeletePOptions.newBuilder().setRecursive(true).build());
      } catch (Exception e2) {
        // intend to continue and return BAD_DIGEST S3Exception.
      }
      throw new S3Exception(objectUri.getPath(), S3ErrorCode.BAD_DIGEST);
    }

    String entityTag = Hex.encodeHexString(digest);
    // persist the ETag via xAttr
    // TODO(czhu): try to compute the ETag prior to creating the file
    //  to reduce total RPC RTT
    userFs.setAttribute(new AlluxioURI(uri), SetAttributePOptions.newBuilder()
        .putXattr("s3_etag", ByteString.copyFrom(entityTag, StandardCharsets.UTF_8))
        .setXattrUpdateStrategy(File.XAttrUpdateStrategy.UNION_REPLACE)
        .build());
    // PutObject

    FullHttpResponse response =
        new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
    response.headers().set("ETAG", entityTag);
    ChannelFuture future = ctx.writeAndFlush(response);
    future.addListener(ChannelFutureListener.CLOSE);
  }

  private void getObject(ChannelHandlerContext ctx, FullHttpRequest request)
      throws S3Exception, IOException, AlluxioException {
    String uri = request.uri();
    String authentication = request.headers().get("");
    String user = NettyRestUtil.getUserFromAuthorization(authentication, Configuration.global());
    String range = request.headers().get("Range");
    S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
    FileSystem userFs = NettyRestUtil.createFileSystemForUser(user, mFsClient);
    AlluxioURI objectUri = new AlluxioURI(uri);

    //todo(wyy) parse range read
    URIStatus status;
    status = userFs.getStatus(objectUri);
    //todo: Judge whether it is local data
    long offset = s3Range.getOffset(status.getLength());
    long blockSize = status.getBlockSizeBytes();
    long blockId = status.getBlockIds().get(0);

    HttpResponse response =
        new DefaultHttpResponse(request.protocolVersion(), HttpResponseStatus.OK);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE,
        NettyRestUtil.deserializeContentType(status.getXAttr()));
    response.headers()
        .set(HttpHeaderNames.LAST_MODIFIED, new Date(status.getLastModificationTimeMs()));
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, s3Range.getLength(status.getLength()));

    String entityTag = NettyRestUtil.getEntityTag(status);
    if (entityTag != null) {
      response.headers().set(S3Constants.S3_ETAG_HEADER, entityTag);
    } else {
      LOG.debug("Failed to find ETag for object: " + objectUri);
    }

    TaggingData tagData = NettyRestUtil.deserializeTags(status.getXAttr());
    if (tagData != null) {
      int taggingCount = tagData.getTagMap().size();
      if (taggingCount > 0) {
        response.headers().set(S3Constants.S3_TAGGING_COUNT_HEADER, taggingCount);
      }
    }
    boolean keepAlive = HttpUtil.isKeepAlive(request);
    if (keepAlive) {
      response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    }

    BlockInfo blockInfo = status.getBlockInfo(blockId);
    Optional<BlockMeta> meta = mBlockWorker.getBlockStore().getVolatileBlockMeta(blockId);
    if (!meta.isPresent()) {
      throw new IOException();
    }
    String fileLocalPath = meta.get().getPath();
    RandomAccessFile file = new RandomAccessFile(fileLocalPath, "r");

    ctx.write(response);
    ctx.write(new DefaultFileRegion(file.getChannel(), 0, file.length()));
    ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
    if (!keepAlive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }
}
