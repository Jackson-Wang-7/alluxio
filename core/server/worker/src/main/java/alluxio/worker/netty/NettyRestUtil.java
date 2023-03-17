package alluxio.worker.netty;

import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;
import alluxio.s3.TaggingData;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.security.auth.Subject;
import javax.ws.rs.core.MediaType;

/**
 * netty rest util.
 */
public class NettyRestUtil {

  /**
   * @param user the {@link Subject} name of the filesystem user
   * @param fs the source {@link FileSystem} to base off of
   * @return A {@link FileSystem} with the subject set to the provided user
   */
  public static FileSystem createFileSystemForUser(
      String user, FileSystem fs) {
    if (user == null) {
      // Used to return the top-level FileSystem view when not using Authentication
      return fs;
    }

    final Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    return FileSystem.Factory.get(subject, fs.getConf());
  }

  /**
   * Given xAttr, parses and returns the Content-Type header metadata
   * as its corresponding {@link MediaType}, or otherwise defaults
   * to {@link MediaType#APPLICATION_OCTET_STREAM_TYPE}.
   * @param xAttr the Inode's xAttrs
   * @return the {@link MediaType} corresponding to the Content-Type header
   */
  public static MediaType deserializeContentType(Map<String, byte[]> xAttr) {
    MediaType type = MediaType.APPLICATION_OCTET_STREAM_TYPE;
    // Fetch the Content-Type from the Inode xAttr
    if (xAttr == null) {
      return type;
    }
    if (xAttr.containsKey("s3_content_type")) {
      String contentType = new String(xAttr.get("s3_content_type"), StandardCharsets.UTF_8);
      if (!contentType.isEmpty()) {
        type = MediaType.valueOf(contentType);
      }
    }
    return type;
  }

  /**
   * Gets the user from the authorization header string for AWS Signature Version 4.
   * @param authorization the authorization header string
   * @param conf the {@link AlluxioConfiguration} Alluxio conf
   * @return the user
   */
  @VisibleForTesting
  public static String getUserFromAuthorization(String authorization, AlluxioConfiguration conf)
      throws S3Exception {
    if (conf.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE) == AuthType.NOSASL) {
      return null;
    }
    if (authorization == null) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    // Parse the authorization header defined at
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
    // All other authorization types are deprecated or EOL (as of writing)
    // Example Header value (spaces turned to line breaks):
    // AWS4-HMAC-SHA256
    // Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,
    // SignedHeaders=host;range;x-amz-date,
    // Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024

    // We only care about the credential key, so split the header by " " and then take everything
    // after the "=" and before the first "/"
    String[] fields = authorization.split(" ");
    if (fields.length < 2) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    String credentials = fields[1];
    String[] creds = credentials.split("=");
    // only support version 4 signature
    if (creds.length < 2 || !StringUtils.equals("Credential", creds[0])
        || !creds[1].contains("/")) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    final String user = creds[1].substring(0, creds[1].indexOf("/")).trim();
    if (user.isEmpty()) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    return user;
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource object complete path
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toObjectS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
      e.setResource(resource);
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.PRECONDITION_FAILED);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_KEY);
    } catch (AccessControlException e) {
      return new S3Exception(e, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * This helper method is used to get the ETag xAttr on an object.
   * @param status The {@link URIStatus} of the object
   * @return the entityTag String, or null if it does not exist
   */
  public static String getEntityTag(URIStatus status) {
    if (status.getXAttr() == null
        || !status.getXAttr().containsKey(S3Constants.ETAG_XATTR_KEY)) {
      return null;
    }
    return new String(status.getXAttr().get(S3Constants.ETAG_XATTR_KEY),
        S3Constants.XATTR_STR_CHARSET);
  }

  /**
   * Given xAttr, parses and deserializes the Tagging metadata
   * into a {@link TaggingData} object. Returns null if no data exists.
   * @param xAttr the Inode's xAttrs
   * @return the deserialized {@link TaggingData} object
   */
  public static TaggingData deserializeTags(Map<String, byte[]> xAttr)
      throws IOException {
    // Fetch the S3 tags from the Inode xAttr
    if (xAttr == null || !xAttr.containsKey(S3Constants.TAGGING_XATTR_KEY)) {
      return null;
    }
    return TaggingData.deserialize(xAttr.get(S3Constants.TAGGING_XATTR_KEY));
  }
}