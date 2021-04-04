# flink 源码撕裂指南

## flink shell 提交任务流程

入口位于 CliFrontend.main()

```java
public static void main(final String[] args) {
        EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);

        // 1. find the configuration directory
        final String configurationDirectory = getConfigurationDirectoryFromEnv();

        // 2. load the global configuration
        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(configurationDirectory);

        // 3. load the custom command lines
        //myread 依次添加三种客户端模式GenericCLI，Yarn，Standalone
        final List<CustomCommandLine> customCommandLines =
                loadCustomCommandLines(configuration, configurationDirectory);

        int retCode = 31;
        try {
            final CliFrontend cli = new CliFrontend(configuration, customCommandLines);

            SecurityUtils.install(new SecurityConfiguration(cli.configuration));
            //myread 核心执行逻辑
            retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.parseAndRun(args));
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            LOG.error("Fatal error while running command line interface.", strippedThrowable);
            strippedThrowable.printStackTrace();
        } finally {
            System.exit(retCode);
        }
    }
```

走进cli.parseAndRun(args)

里面其实会判断shell命令中 flink 后跟的是什么命令 run ，stop，help

``` java
switch (action) {
                case ACTION_RUN:
                    run(params);
                    return 0;
                case ACTION_RUN_APPLICATION:
                    runApplication(params);
                    return 0;
                case ACTION_LIST:
                    list(params);
                    return 0;
                    ...
```

重点是run（）方法-》

```java

    /**
     * Executions the run action.
     *
     * @param args Command line arguments for the run action.
     */
    protected void run(String[] args) throws Exception {
        LOG.info("Running 'run' command.");
        //myread 获取flink run xxxx后的配置参数，如 -t -y
        final Options commandOptions = CliFrontendParser.getRunCommandOptions();
        //myread 解析配置参数
        final CommandLine commandLine = getCommandLine(commandOptions, args, true);

        // evaluate help flag
        if (commandLine.hasOption(HELP_OPTION.getOpt())) {
            CliFrontendParser.printHelpForRun(customCommandLines);
            return;
        }

        //myread 根据inActive 方法来依次判断采用哪个客户端
        final CustomCommandLine activeCommandLine =
                validateAndGetActiveCommandLine(checkNotNull(commandLine));

        final ProgramOptions programOptions = ProgramOptions.create(commandLine);

        //myread 获取jar包和主方法入口
        final List<URL> jobJars = getJobJarAndDependencies(programOptions);

        //myread 封装运行资源参数，如核心树，内存大小
        final Configuration effectiveConfiguration =
                getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);

        LOG.debug("Effective executor configuration: {}", effectiveConfiguration);

        try (PackagedProgram program = getPackagedProgram(programOptions, effectiveConfiguration)) {
            //myread 执行main方法
            executeProgram(effectiveConfiguration, program);
        }
    }
```

而executeProgram（）方法里的逻辑为：

- 封装环境
- invokeInteractiveModeForExecution 调用main方法

至此，任务提交完毕。

## flink 任务执行流程

1. 在用户提交的代码中，真正的执行逻辑都是 StreamExecuteEnvironment . executeAsync()

```java
public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
        checkNotNull(streamGraph, "StreamGraph cannot be null.");
        checkNotNull(
                configuration.get(DeploymentOptions.TARGET),
                "No execution.target specified in your configuration file.");

        final PipelineExecutorFactory executorFactory =
                executorServiceLoader.getExecutorFactory(configuration);

        checkNotNull(
                executorFactory,
                "Cannot find compatible factory for specified execution.target (=%s)",
                configuration.get(DeploymentOptions.TARGET));

        CompletableFuture<JobClient> jobClientFuture =
                //myread 根据配置生成对应的执行器，然后执行
                executorFactory
                        .getExecutor(configuration)
                        .execute(streamGraph, configuration, userClassloader);
```

2. 走进 execute方法

```java
 @Override
    public CompletableFuture<JobClient> execute(
            @Nonnull final Pipeline pipeline,
            @Nonnull final Configuration configuration,
            @Nonnull final ClassLoader userCodeClassloader)
            throws Exception {
        //myread 从StreamGraph 转化为JobGraph
        final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);

        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                //myread 创建集群描述器,包含了yarn，flink的环境和配置信息
                clusterClientFactory.createClusterDescriptor(configuration)) {
            final ExecutionConfigAccessor configAccessor =
                    ExecutionConfigAccessor.fromConfiguration(configuration);

            //myread 封装了一些JM，TM，Slots设置
            final ClusterSpecification clusterSpecification =
                    clusterClientFactory.getClusterSpecification(configuration);

            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    //myread 部署核心逻辑
                    clusterDescriptor.deployJobCluster(
                            clusterSpecification, jobGraph, configAccessor.getDetachedMode());
            LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

            return CompletableFuture.completedFuture(
                    new ClusterClientJobClientAdapter<>(
                            clusterClientProvider, jobGraph.getJobID(), userCodeClassloader));
        }
    }
```

- clusterClientFactory.createClusterDescriptor 核心逻辑如下：

```java
//myread 初始化yarn客户端并启动，然后创建集群描述器
    private YarnClusterDescriptor getClusterDescriptor(Configuration configuration) {
        final YarnClient yarnClient = YarnClient.createYarnClient();
        final YarnConfiguration yarnConfiguration =
                Utils.getYarnAndHadoopConfiguration(configuration);
        
        yarnClient.init(yarnConfiguration);
        yarnClient.start();

        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                false);
    }
```

3. 走进clusterDescriptor.deployJobCluster：

```java
private ClusterClientProvider<ApplicationId> deployInternal(
            ClusterSpecification clusterSpecification,
            String applicationName,
            String yarnClusterEntrypoint,
            @Nullable JobGraph jobGraph,
            boolean detached)
            throws Exception {

        final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        //myread 验证k6s权限
        if (HadoopUtils.isKerberosSecurityEnabled(currentUser)) {
            boolean useTicketCache =
                    flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);

            if (!HadoopUtils.areKerberosCredentialsValid(currentUser, useTicketCache)) {
                throw new RuntimeException(
                        "Hadoop security with Kerberos is enabled but the login user "
                                + "does not have Kerberos credentials or delegation tokens!");
            }
        }
        //myread 检查集群环境是否ok
        isReadyForDeployment(clusterSpecification);

        // ------------------ Check if the specified queue exists --------------------

        checkYarnQueues(yarnClient);
        //myread 检查yarn集群资源是否满足要求
        // ------------------ Check if the YARN ClusterClient has the requested resources
        // --------------
        // Create application via yarnClient
        final YarnClientApplication yarnApplication = yarnClient.createApplication();
        final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();

       ...
         
        final ClusterEntrypoint.ExecutionMode executionMode =
                detached
                        ? ClusterEntrypoint.ExecutionMode.DETACHED
                        : ClusterEntrypoint.ExecutionMode.NORMAL;

        flinkConfiguration.setString(
                ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, executionMode.toString());

        ApplicationReport report =
                //myread 核心执行逻辑
                startAppMaster(
                        flinkConfiguration,
                        applicationName,
                        yarnClusterEntrypoint,
                        jobGraph,
                        yarnClient,
                        yarnApplication,
                        validClusterSpecification);

        // print the application id for user to cancel themselves.
        if (detached) {
            final ApplicationId yarnApplicationId = report.getApplicationId();
            logDetachedClusterInformation(yarnApplicationId, LOG);
        }

        setClusterEntrypointInfoToConfig(report);

        return () -> {
            try {
                return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());
            } catch (Exception e) {
                throw new RuntimeException("Error while creating RestClusterClient.", e);
            }
        };
    }
```

4. startAppMaster

```java
private ApplicationReport startAppMaster(
            Configuration configuration,
            String applicationName,
            String yarnClusterEntrypoint,
            JobGraph jobGraph,
            YarnClient yarnClient,
            YarnClientApplication yarnApplication,
            ClusterSpecification clusterSpecification)
            throws Exception {

        // ------------------ Initialize the file systems -------------------------
        //myread 初始化文件系统
        org.apache.flink.core.fs.FileSystem.initialize(
                configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

        final FileSystem fs = FileSystem.get(yarnConfiguration);

        // hard coded check for the GoogleHDFS client because its not overriding the getScheme()
        // method.
        if (!fs.getClass().getSimpleName().equals("GoogleHadoopFileSystem")
                && fs.getScheme().startsWith("file")) {
            LOG.warn(
                    "The file system scheme is '"
                            + fs.getScheme()
                            + "'. This indicates that the "
                            + "specified Hadoop configuration path is wrong and the system is using the default Hadoop configuration values."
                            + "The Flink YARN client needs to store its files in a distributed file system");
        }

        ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();

        //myread 获取文件上传路径
        final List<Path> providedLibDirs =
                Utils.getQualifiedRemoteSharedPaths(configuration, yarnConfiguration);
        //myread 获取文件上传器
        final YarnApplicationFileUploader fileUploader =
                YarnApplicationFileUploader.from(
                        fs,
                        getStagingDir(fs),
                        providedLibDirs,
                        appContext.getApplicationId(),
                        getFileReplication());
  ...
    
    // write job graph to tmp file and add it to local resource
        // TODO: server use user main method to generate job graph
        //myread 将作业图写入到本地一个临时文件
        if (jobGraph != null) {
            File tmpJobGraphFile = null;
            try {
                tmpJobGraphFile = File.createTempFile(appId.toString(), null);
                try (FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
                        ObjectOutputStream obOutput = new ObjectOutputStream(output)) {
                    obOutput.writeObject(jobGraph);
                }

                final String jobGraphFilename = "job.graph";
                configuration.setString(JOB_GRAPH_FILE_PATH, jobGraphFilename);

                fileUploader.registerSingleLocalResource(
                        jobGraphFilename,
                        new Path(tmpJobGraphFile.toURI()),
                        "",
                        LocalResourceType.FILE,
                        true,
                        false);
                classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
            } catch (Exception e) {
                LOG.warn("Add job graph to local resource fail.");
                throw e;
            } finally {
                if (tmpJobGraphFile != null && !tmpJobGraphFile.delete()) {
                    LOG.warn("Fail to delete temporary file {}.", tmpJobGraphFile.toPath());
                }
            }
        }
  ...
      amContainer.setLocalResources(fileUploader.getRegisteredLocalResources());
        //myread jar包，flink依赖，flink 配置文件上传结束
        fileUploader.close();

        // Setup CLASSPATH and environment variables for ApplicationMaster
        //myread 封装环境信息，类路径到AM
        final Map<String, String> appMasterEnv = new HashMap<>();
        // set user specified app master environment variables
        appMasterEnv.putAll(
                ConfigurationUtils.getPrefixedKeyValuePairs(
                        ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX, configuration));
  ...
      //myread 封装环境信息，类路径到AM
        amContainer.setEnvironment(appMasterEnv);
   Runtime.getRuntime().addShutdownHook(deploymentFailureHook);
        LOG.info("Submitting application master " + appId);
        //myread 启动ApplicationMaster
        yarnClient.submitApplication(appContext);

        LOG.info("Waiting for the cluster to be allocated");
        final long startTime = System.currentTimeMillis();
        ApplicationReport report;
        YarnApplicationState lastAppState = YarnApplicationState.NEW;
        //myread 根据容器状态打印日志
        loop:
        while (true) {
            try {
                report = yarnClient.getApplicationReport(appId);
            } catch (IOException e) {
                throw new YarnDeploymentException("Failed to deploy the cluster.", e);
            }
            YarnApplicationState appState = report.getYarnApplicationState();
            LOG.debug("Application State: {}", appState);
            switch (appState) {
                case FAILED:
                case KILLED:
                    throw new YarnDeploymentException(
                            "The YARN application unexpectedly switched to state "
                                    + appState
                                    + " during deployment. \n"
                                    + "Diagnostics from YARN: "
                                    + report.getDiagnostics()
                                    + "\n"
                                    + "If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n"
                                    + "yarn logs -applicationId "
                                    + appId);
                    // break ..
                case RUNNING:
                    LOG.info("YARN application has been deployed successfully.");
                    break loop;
                case FINISHED:
                    LOG.info("YARN application has been finished successfully.");
                    break loop;
                default:
                    if (appState != lastAppState) {
                        LOG.info("Deploying cluster, current state " + appState);
                    }
                    if (System.currentTimeMillis() - startTime > 60000) {
                        LOG.info(
                                "Deployment took more than 60 seconds. Please check if the requested resources are available in the YARN cluster");
                    }
            }
            lastAppState = appState;
            Thread.sleep(250);
        }

        // since deployment was successful, remove the hook
        ShutdownHookUtil.removeShutdownHook(deploymentFailureHook, getClass().getSimpleName(), LOG);
        return report;
    }
```

