package io.github.wooenrico;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class InputStreamUtil {

    /**
     * Read the inputStream and close it.
     *
     * @param inputStream InputStream
     * @return byte[]
     * @throws IOException IOException
     */
    public static byte[] readAndClose(InputStream inputStream) throws IOException {

        if (inputStream == null) {
            return null;
        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] bytes = new byte[1024];
        for (int len = -1; (len = inputStream.read(bytes)) != -1; ) {
            byteArrayOutputStream.write(bytes, 0, len);
        }

        inputStream.close();

        return byteArrayOutputStream.toByteArray();
    }
}
