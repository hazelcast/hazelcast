[<img src="https://code.visualstudio.com/assets/images/code-stable.png" alt="VSCode logo" width="200"/>](https://code.visualstudio.com/)

# VS Code IDE Configurations

## Extensions
Repository includes [workplace recommended extensions](https://code.visualstudio.com/docs/editor/extension-marketplace#_workspace-recommended-extensions), which are recommended automatically when opened for the first time:

![VS Code Install recommended extension toast](./install-recommended-extension-toast.png)

Or can be reviewed from the extension window:

![VS Code extensions preview showing workspace recommended extensions](./workspace-recommended-extensions.png)

## `.vscode/settings.json`
```
{
  "java.configuration.maven.userSettingsFile": "${workspaceFolder}/.mvn/wrapper/maven-wrapper.properties",
  "java.jdt.ls.vmargs": "-XX:+UseParallelGC -XX:GCTimeRatio=4 -XX:AdaptiveSizePolicyWeight=90 -Dsun.zip.disableMemoryMapping=true -Xlog:disable",
  "java.compile.nullAnalysis.mode": "automatic",
  "java.debug.settings.vmArgs": "-ea"
}
```
