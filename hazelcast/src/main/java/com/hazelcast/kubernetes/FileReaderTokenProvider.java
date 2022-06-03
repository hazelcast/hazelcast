package com.hazelcast.kubernetes;

public class FileReaderTokenProvider implements KubernetesTokenProvider {
    private final String tokenPath;
    private final KubernetesConfig.FileContentsReader fileContentsReader;

    public FileReaderTokenProvider(String tokenPath) {
        this.tokenPath = tokenPath;
        this.fileContentsReader = new KubernetesConfig.DefaultFileContentsReader();
    }

    @Override
    public String getToken() {
        return fileContentsReader.readFileContents(tokenPath);
    }
}
