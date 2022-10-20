package io.github.wooenrico;

public final class ProjectVersion {
    public static final String VERSION = "${project.version}";
    public static final String COMMIT_ID = "${buildNumber}";
    public static final String SCM_BRANCH = "${scmBranch}";
    public static final String TIMESTAMP = "${timestamp}";
}
