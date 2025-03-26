package io.github.wooenrico;

/**
 * Project version information.
 */
public final class ProjectVersion {
    /**
     * version information.
     */
    public static final String VERSION = "${project.version}";
    /**
     * git commit id.
     */
    public static final String COMMIT_ID = "${buildNumber}";
    /**
     * git branch name.
     */
    public static final String SCM_BRANCH = "${scmBranch}";
    /**
     * build timestamp.
     */
    public static final String TIMESTAMP = "${timestamp}";
}
