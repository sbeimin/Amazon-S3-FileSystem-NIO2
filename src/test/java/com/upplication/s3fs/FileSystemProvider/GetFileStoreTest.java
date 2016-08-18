package com.upplication.s3fs.FileSystemProvider;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3UnitTestBase;
import com.upplication.s3fs.util.AmazonS3ClientMock;
import com.upplication.s3fs.util.AmazonS3MockFactory;
import com.upplication.s3fs.util.S3EndpointConstant;

public class GetFileStoreTest extends S3UnitTestBase {

    private S3FileSystemProvider s3fsProvider;

    @Before
    public void setup() {
        s3fsProvider = spy(new S3FileSystemProvider());
        doReturn(false).when(s3fsProvider).overloadPropertiesWithSystemEnv(any(Properties.class), anyString());
        doReturn(new Properties()).when(s3fsProvider).loadAmazonProperties();
    }


	@Test
    public void getFileStore() throws IOException {
        // fixtures
        AmazonS3ClientMock client = AmazonS3MockFactory.getAmazonClientMock();
        client.bucket("bucketA").dir("dir").file("dir/file1");

        // act
        Path file1 = createNewS3FileSystem().getPath("/bucketA/dir/file1");
        // assert
		FileStore fileStore = s3fsProvider.getFileStore(file1);
		assertNotNull(fileStore);
    }

    /**
     * create a new file system for s3 scheme with fake credentials
     * and global endpoint
     *
     * @return FileSystem
     * @throws IOException
     */
    private S3FileSystem createNewS3FileSystem() throws IOException {
        try {
            return s3fsProvider.getFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST);
        } catch (FileSystemNotFoundException e) {
            return (S3FileSystem) FileSystems.newFileSystem(S3EndpointConstant.S3_GLOBAL_URI_TEST, null);
        }

    }
}
