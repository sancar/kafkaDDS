package io.github.sancar.kafkadds.totalorderbrodacast;

public class Records {

    public static final String HEADER_KEY_OPERATION = "OPERATION";

    public static class HeaderValues {
        public static final String WAIT_KEY = "WAIT_KEY";
        public static final String WRITE_ATTEMPT = "WRITE_ATTEMPT";
    }

    // Wait Key. Value of WaitKey is "true"
    public record WaitKey(String waitKey) { private static final String keyType = "WAIT_KEY";}

    public record WriteAttemptKey(String key) { private static final String keyType = "WRITE_ATTEMPT";}

    public record WriteAttemptValue(int version, String value) {
    }

}
