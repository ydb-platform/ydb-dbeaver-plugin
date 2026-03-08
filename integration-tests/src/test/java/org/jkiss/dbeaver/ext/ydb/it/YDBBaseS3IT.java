package org.jkiss.dbeaver.ext.ydb.it;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.SetBucketPolicyArgs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for integration tests that require a local S3 (MinIO).
 * Connects to MinIO at s3.url system property (default http://localhost:9000),
 * creates the test bucket with a public-read policy if it doesn't exist.
 */
public abstract class YDBBaseS3IT extends YDBBaseIT {

    protected static final String S3_BUCKET = "test-bucket";

    private static final String S3_ENDPOINT = System.getProperty("s3.url", "http://localhost:9000");
    private static final String S3_ACCESS_KEY = System.getProperty("s3.accessKey", "minioadmin");
    private static final String S3_SECRET_KEY = System.getProperty("s3.secretKey", "minioadmin");

    protected static String s3Location;

    private static MinioClient minioClient;

    @BeforeAll
    void setUpS3() throws Exception {
        minioClient = MinioClient.builder()
            .endpoint(S3_ENDPOINT)
            .credentials(S3_ACCESS_KEY, S3_SECRET_KEY)
            .build();

        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(S3_BUCKET).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(S3_BUCKET).build());
        }

        String policy =
            "{\"Version\":\"2012-10-17\",\"Statement\":[{" +
            "\"Effect\":\"Allow\",\"Principal\":\"*\"," +
            "\"Action\":[\"s3:GetObject\",\"s3:ListBucket\"]," +
            "\"Resource\":[\"arn:aws:s3:::" + S3_BUCKET + "\",\"arn:aws:s3:::" + S3_BUCKET + "/*\"]}]}";

        minioClient.setBucketPolicy(SetBucketPolicyArgs.builder()
            .bucket(S3_BUCKET)
            .config(policy)
            .build());

        s3Location = S3_ENDPOINT + "/" + S3_BUCKET + "/";
    }
}
