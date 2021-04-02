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

