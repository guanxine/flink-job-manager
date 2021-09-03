package com.mi.flink.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.cli.*;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.*;
import org.apache.flink.runtime.rest.messages.taskmanager.*;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class XJCliFrontend extends CliFrontend {

    private static final Logger LOG = LoggerFactory.getLogger(XJCliFrontend.class);
    int sleepIntervalInMS = 1000 * 5;

    // actions
    private static final String ACTION_XJ_RUN = "xj_run";
    private static final String ACTION_XJ_APP = "xj_app";
    private static final String ACTION_XJ_LOG = "xj_log";

    public XJCliFrontend(Configuration configuration, List<CustomCommandLine> customCommandLines) {
        super(configuration, customCommandLines);
    }

    public XJCliFrontend(Configuration configuration, ClusterClientServiceLoader clusterClientServiceLoader, List<CustomCommandLine> customCommandLines) {
        super(configuration, clusterClientServiceLoader, customCommandLines);
    }

    public int parseParameters(String[] args) {

        // check for action
        if (args.length < 1) {
            System.out.println("Please specify an action.");
            return 1;
        }

        // get action
        String action = args[0];

        // remove action from parameters
        final String[] params = Arrays.copyOfRange(args, 1, args.length);

        try {
            switch (action) {
                case ACTION_XJ_RUN:
                    xjRun(params);
                    return 0;
                case ACTION_XJ_APP:
                    xjApp(params);
                    return 0;
                case ACTION_XJ_LOG:
                    xjLog(params);
                    return 0;
                default:
                    return super.parseParameters(args);
            }
        } catch (Exception e) {
            return handleError(e);
        }
    }

    private void xjLog(String[] params) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(params);
        String appId = parameterTool.get("appId", "");
        String taskId = parameterTool.get("taskId", "");
        if (appId.isEmpty()) {
            LOG.info("Usage: {} -appId <appId>", ACTION_XJ_LOG);
            return;
        }

        Configuration configuration = getConfiguration();
        YarnClusterClientFactory yarnClusterClientFactory = new YarnClusterClientFactory();
        YarnClusterDescriptor clusterDescriptor = yarnClusterClientFactory.createClusterDescriptor(configuration);

        YarnClient yarnClient = clusterDescriptor.getYarnClient();

        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
        ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
        if (applicationReport == null) {
            LOG.info("Application {} not found.", applicationId);
        }

        ClusterClient<ApplicationId> clusterClient = clusterDescriptor.retrieve(applicationId).getClusterClient();
        CompletableFuture<Collection<JobStatusMessage>> collectionCompletableFuture = clusterClient.listJobs();
        Collection<JobStatusMessage> jobStatusMessages = collectionCompletableFuture.get();
        LOG.info("Application {} status {}.", applicationId, applicationReport.getYarnApplicationState());

        List<JobStatusMessage> runJobs = jobStatusMessages.stream().filter(item -> item.getJobState() == JobStatus.RUNNING).collect(Collectors.toList());

        if (applicationReport.getYarnApplicationState() == YarnApplicationState.FAILED
                || applicationReport.getYarnApplicationState() == YarnApplicationState.FINISHED
                || applicationReport.getYarnApplicationState() == YarnApplicationState.KILLED
                || runJobs.isEmpty()) {
            LOG.info("Flink job has finished, please use : yarn logs --help");
            return;
        }

        for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
            LOG.info("Job {} name {} status {}.", jobStatusMessage.getJobId(), jobStatusMessage.getJobName(), jobStatusMessage.getJobState());
        }

        final URI webURI = new URI(clusterClient.getWebInterfaceURL());
        final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(configuration), Executors.newSingleThreadScheduledExecutor());
        Collection<TaskManagerInfo> taskManagerInfos = getTasks(restClient, webURI);

        Collection<TaskManagerInfo> logTaskInfos = taskId.isEmpty() ? taskManagerInfos :
                taskManagerInfos.stream().filter(item -> item.getResourceId().equals(taskId)).collect(Collectors.toList());
        for (TaskManagerInfo taskManagerInfo : logTaskInfos) {
            LOG.info("-------------------------------task {} log start----------------------------------", taskManagerInfo.getResourceId());

            // log list
            TaskManagerMessageParameters taskManagerMessageParameters = new TaskManagerMessageParameters();
            taskManagerMessageParameters.taskManagerIdParameter.resolve(taskManagerInfo.getResourceId());
            CompletableFuture<LogListInfo> logListInfoCompletableFuture = restClient.sendRequest(
                    webURI.getHost(),
                    webURI.getPort(),
                    TaskManagerLogsHeaders.getInstance(),
                    taskManagerMessageParameters,
                    EmptyRequestBody.getInstance());

            LogListInfo logListInfo = logListInfoCompletableFuture.get();
            Collection<LogInfo> logInfos = logListInfo.getLogInfos();

            TaskManagerCustomLogHeaders taskManagerCustomLogHeaders = TaskManagerCustomLogHeaders.getInstance();

            for (LogInfo logInfo : logInfos) {
                LOG.info("log info = [name {}, size {}]", logInfo.getName(), logInfo.getSize());

                // write log content to hdfs
                TaskManagerFileMessageParameters taskManagerFileMessageParameters = new TaskManagerFileMessageParameters();
                taskManagerFileMessageParameters.taskManagerIdParameter.resolve(taskManagerInfo.getResourceId());
                taskManagerFileMessageParameters.logFileNamePathParameter.resolve(logInfo.getName());
                RestAPIVersion apiVersion = RestAPIVersion.getLatestVersion(taskManagerCustomLogHeaders.getSupportedAPIVersions());
                String versionedHandlerURL = "/" + apiVersion.getURLVersionPrefix() + taskManagerCustomLogHeaders.getTargetRestEndpointURL();
                String targetUrl = MessageParameters.resolveUrl(versionedHandlerURL, taskManagerFileMessageParameters);

                String url = "http://" + webURI.getHost() + ":" + webURI.getPort() + targetUrl;
                LOG.info("{} log web url {}", taskManagerInfo.getResourceId(), url);
                LOG.info("-------------------------------task {} log {} start----------------------------------", taskManagerInfo.getResourceId(), logInfo.getName());
                String content = WebUtils.getFromHTTP(url);
                System.out.println(content);
                LOG.info("-------------------------------task {} log {} end----------------------------------", taskManagerInfo.getResourceId(), logInfo.getName());

//                        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
//
//                        try (final FileSystem fs = FileSystem.get(yarnConfiguration)) {
//                            // fs
//                            Path appDir = new Path(fs.getHomeDirectory(), Constants.APP);
//                            if (!fs.exists(appDir)) {
//                                fs.mkdirs(appDir);
//                            }
//                            Path logPath = new Path(appDir, applicationId + "/logs/" + taskManagerInfo.getResourceId() + "/" + logInfo.getName());
//
//                            LOG.info("Trying to write log to path [{}]", logPath);
//                            IOUtils.copyBytes(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
//                                    fs.create(logPath, true), true);
//                        }
            }
            LOG.info("-------------------------------task {} log end----------------------------------", taskManagerInfo.getResourceId());
        }

    }

    private Collection<TaskManagerInfo> getTasks(RestClient restClient, URI webURI) throws ConfigurationException, IOException, InterruptedException, ExecutionException {
        Duration timeout = Duration.ofSeconds(20);
        Deadline deadline = Deadline.now().plus(timeout);

        Collection<TaskManagerInfo> taskManagerInfos;
        while (true) {

            if (deadline.isOverdue()) {
                LOG.info("Get task manager info timeout");
                return Collections.emptyList();
            }
            CompletableFuture<TaskManagersInfo> taskManagersInfoCompletableFuture = restClient.sendRequest(
                    webURI.getHost(),
                    webURI.getPort(),
                    TaskManagersHeaders.getInstance(),
                    EmptyMessageParameters.getInstance(),
                    EmptyRequestBody.getInstance());


            final TaskManagersInfo taskManagersInfo = taskManagersInfoCompletableFuture.get();

            taskManagerInfos = taskManagersInfo.getTaskManagerInfos();

            // wait until the task manager has registered and reported its slots
            if (hasTaskManagerConnectedAndReportedSlots(taskManagerInfos)) {
                break;
            } else {
                sleep(sleepIntervalInMS);
            }
        }
        return taskManagerInfos;
    }

    private void xjApp(String[] params) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(params);
        String appId = parameterTool.get("appId", "");

        Configuration configuration = getConfiguration();
        YarnClusterClientFactory yarnClusterClientFactory = new YarnClusterClientFactory();
        YarnClusterDescriptor clusterDescriptor = yarnClusterClientFactory.createClusterDescriptor(configuration);

        YarnClient yarnClient = clusterDescriptor.getYarnClient();

        if (appId.isEmpty()) {
            Set sets = new HashSet<String>();
            sets.add(Constants.APP_FLINK_TYPE);
            List<ApplicationReport> applications = yarnClient.getApplications(sets, EnumSet.of(YarnApplicationState.RUNNING));
            for (ApplicationReport application : applications) {
                LOG.info("Application {} name {} type {} status {} final-status {}", application.getApplicationId(), application.getName(), application.getApplicationType(), application.getYarnApplicationState(), application.getFinalApplicationStatus());
            }
        } else {
            ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
            ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
            if (applicationReport == null) {
                LOG.info("Application {} not found.", applicationId);
            }
            ClusterClient<ApplicationId> clusterClient = clusterDescriptor.retrieve(applicationId).getClusterClient();
            CompletableFuture<Collection<JobStatusMessage>> collectionCompletableFuture = clusterClient.listJobs();
            Collection<JobStatusMessage> jobStatusMessages = collectionCompletableFuture.get();
            LOG.info("Application {} name {} status {}.", applicationId, applicationReport.getName(), applicationReport.getYarnApplicationState());
            for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
                LOG.info("Job {} name {} status {}.", jobStatusMessage.getJobId(), jobStatusMessage.getJobName(), jobStatusMessage.getJobState());
            }

            List<JobStatusMessage> runJobs = jobStatusMessages.stream().filter(item -> item.getJobState() == JobStatus.RUNNING).collect(Collectors.toList());

            if (runJobs.isEmpty()) {
                LOG.info("No job run in application {}.", applicationId);
                return;
            }

            final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(configuration), Executors.newSingleThreadScheduledExecutor());
            final URI webURI = new URI(clusterClient.getWebInterfaceURL());
            Collection<TaskManagerInfo> taskManagerInfos = getTasks(restClient, webURI);
            for (TaskManagerInfo taskManagerInfo : taskManagerInfos) {
                LOG.info("task id {} address {}.", taskManagerInfo.getResourceId(), taskManagerInfo.getAddress());
            }
        }

    }

    private File getJarFile(String jarFilePath) throws FileNotFoundException {
        File jarFile = new File(jarFilePath);
        // Check if JAR file exists
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file does not exist: " + jarFile);
        } else if (!jarFile.isFile()) {
            throw new FileNotFoundException("JAR file is not a file: " + jarFile);
        }
        return jarFile;
    }

    PackagedProgram buildProgram(ProgramOptions runOptions) throws FileNotFoundException, ProgramInvocationException {
        String[] programArgs = runOptions.getProgramArgs();
        String jarFilePath = runOptions.getJarFilePath();
        List<URL> classpaths = runOptions.getClasspaths();

        // Get assembler class
        String entryPointClass = runOptions.getEntryPointClassName();
        File jarFile = null;
        if (runOptions.isPython()) {
            // If the job is specified a jar file
            if (jarFilePath != null) {
                jarFile = getJarFile(jarFilePath);
            }

            // If the job is Python Shell job, the entry point class name is PythonGateWayServer.
            // Otherwise, the entry point class of python job is PythonDriver
            if (entryPointClass == null) {
                entryPointClass = "org.apache.flink.client.python.PythonDriver";
            }
        } else {
            if (jarFilePath == null) {
                throw new IllegalArgumentException("Java program should be specified a JAR file.");
            }
            jarFile = getJarFile(jarFilePath);
        }

        return PackagedProgram.newBuilder()
                .setJarFile(jarFile)
                .setUserClassPaths(classpaths)
                .setEntryPointClassName(entryPointClass)
                .setConfiguration(super.getConfiguration())
                .setSavepointRestoreSettings(runOptions.getSavepointRestoreSettings())
                .setArguments(programArgs)
                .build();
    }

    private static final Time TIMEOUT = Time.seconds(10L);

    private <T> Configuration getEffectiveConfiguration(
            final CustomCommandLine activeCustomCommandLine,
            final CommandLine commandLine,
            final ProgramOptions programOptions,
            final List<T> jobJars) throws FlinkException {

        final ExecutionConfigAccessor executionParameters = ExecutionConfigAccessor.fromProgramOptions(
                checkNotNull(programOptions),
                checkNotNull(jobJars));

        final Configuration executorConfig = checkNotNull(activeCustomCommandLine)
                .applyCommandLineOptionsToConfiguration(commandLine);

        final Configuration effectiveConfiguration = new Configuration(executorConfig);

        executionParameters.applyToConfiguration(effectiveConfiguration);
        LOG.debug("Effective executor configuration: {}", effectiveConfiguration);
        return effectiveConfiguration;
    }

    private void xjRun(String[] args) throws Exception {
        LOG.info("Running 'run' command." + args);

        final Options commandOptions = CliFrontendParser.getRunCommandOptions();
        final CommandLine commandLine = getCommandLine(commandOptions, args, true);

        final CustomCommandLine activeCommandLine =
                validateAndGetActiveCommandLine(checkNotNull(commandLine));

        final ProgramOptions programOptions = new ProgramOptions(commandLine);

        if (!programOptions.isPython()) {
            // Java program should be specified a JAR file
            if (programOptions.getJarFilePath() == null) {
                throw new CliArgsException("Java program should be specified a JAR file.");
            }
        }

        final PackagedProgram program;
        try {
            LOG.info("Building program from JAR file");
            program = buildProgram(programOptions);
        } catch (FileNotFoundException | ProgramInvocationException e) {
            throw new CliArgsException("Could not build the program from JAR file.", e);
        }

        final List<URL> jobJars = program.getJobJarAndDependencies();
        final Configuration effectiveConfiguration = getEffectiveConfiguration(
                activeCommandLine, commandLine, programOptions, jobJars);

        LOG.info("Effective executor configuration: {}", effectiveConfiguration);

        final int defaultParallelism = effectiveConfiguration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
        final JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, effectiveConfiguration, defaultParallelism, true);

        YarnClusterClientFactory yarnClusterClientFactory = new YarnClusterClientFactory();
        YarnClusterDescriptor clusterDescriptor = yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration);
        final ClusterSpecification clusterSpecification = yarnClusterClientFactory.getClusterSpecification(effectiveConfiguration);

        LOG.info("clusterSpecification: {}", clusterSpecification);

        final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(effectiveConfiguration);

        LOG.info("Trying to deploy flink to yarn...");
        final ClusterClient<ApplicationId> clusterClient = clusterDescriptor
                .deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode())
                .getClusterClient();

        final ApplicationId applicationId = clusterClient.getClusterId();
        LOG.info("Application has been submitted with ApplicationID " + applicationId);
        LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

        YarnClient yarnClient = clusterDescriptor.getYarnClient();
        YarnApplicationState state = yarnClient.getApplicationReport(applicationId).getYarnApplicationState();

        Duration yarnAppTerminateTimeout = Duration.ofSeconds(60);
        Deadline deadline = Deadline.now().plus(yarnAppTerminateTimeout);

        RestClusterClient<ApplicationId> restClusterClient = new RestClusterClient<>(clusterDescriptor.getFlinkConfiguration(), applicationId);
        final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(effectiveConfiguration), Executors.newSingleThreadScheduledExecutor());

        while (state != YarnApplicationState.FINISHED) {
            JobStatus jobStatus = restClusterClient.getJobStatus(jobGraph.getJobID()).get();
            LOG.info("Application {} status {}.", applicationId, state);
            LOG.info("Flink job {} status {}", jobGraph.getJobID(), jobStatus);
            state = yarnClient.getApplicationReport(applicationId).getYarnApplicationState();
            if (jobStatus == JobStatus.FAILING || jobStatus == JobStatus.FAILED || jobStatus == JobStatus.FINISHED) {
                LOG.info("Trying to kill app {}, job status is {}", applicationId, jobStatus);
                clusterDescriptor.killCluster(applicationId);
                return;
            }

            if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
                throw new FlinkException("Application became FAILED or KILLED while expecting FINISHED");
            }

            if (deadline.isOverdue()) {
                restClient.shutdown(TIMEOUT);
                clusterClient.close();
//                clusterDescriptor.killCluster(applicationId);
//                throw new FlinkException("Application didn't finish before timeout");
                LOG.info("Application id {}", applicationId);
                LOG.info("Job id {}", jobGraph.getJobID());
                return;
            }

            sleep(sleepIntervalInMS);
            try {
                final URI webURI = new URI(clusterClient.getWebInterfaceURL());

                Collection<TaskManagerInfo> taskManagerInfos = getTasks(restClient, webURI);

                // there should be at least one TaskManagerInfo

                for (TaskManagerInfo taskManagerInfo : taskManagerInfos) {
                    LOG.info("slots per task manager resource id {}, slots {}.", taskManagerInfo.getResourceId(), taskManagerInfo.getNumberSlots());

                    // log list
                    TaskManagerMessageParameters taskManagerMessageParameters = new TaskManagerMessageParameters();
                    taskManagerMessageParameters.taskManagerIdParameter.resolve(taskManagerInfo.getResourceId());
                    CompletableFuture<LogListInfo> logListInfoCompletableFuture = restClient.sendRequest(
                            webURI.getHost(),
                            webURI.getPort(),
                            TaskManagerLogsHeaders.getInstance(),
                            taskManagerMessageParameters,
                            EmptyRequestBody.getInstance());

                    LogListInfo logListInfo = logListInfoCompletableFuture.get();
                    Collection<LogInfo> logInfos = logListInfo.getLogInfos();

                    TaskManagerCustomLogHeaders taskManagerCustomLogHeaders = TaskManagerCustomLogHeaders.getInstance();

                    for (LogInfo logInfo : logInfos) {
                        LOG.info("log info = [name {}, size {}]", logInfo.getName(), logInfo.getSize());

                        // write log content to hdfs
                        TaskManagerFileMessageParameters taskManagerFileMessageParameters = new TaskManagerFileMessageParameters();
                        taskManagerFileMessageParameters.taskManagerIdParameter.resolve(taskManagerInfo.getResourceId());
                        taskManagerFileMessageParameters.logFileNamePathParameter.resolve(logInfo.getName());
                        RestAPIVersion apiVersion = RestAPIVersion.getLatestVersion(taskManagerCustomLogHeaders.getSupportedAPIVersions());
                        String versionedHandlerURL = "/" + apiVersion.getURLVersionPrefix() + taskManagerCustomLogHeaders.getTargetRestEndpointURL();
                        String targetUrl = MessageParameters.resolveUrl(versionedHandlerURL, taskManagerFileMessageParameters);

                        String url = "http://" + webURI.getHost() + ":" + webURI.getPort() + targetUrl;
                        LOG.info("{} log web url {}", taskManagerInfo.getResourceId(), url);
//                        String content = WebUtils.getFromHTTP(url);
//                        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
//
//                        try (final FileSystem fs = FileSystem.get(yarnConfiguration)) {
//                            // fs
//                            Path appDir = new Path(fs.getHomeDirectory(), Constants.APP);
//                            if (!fs.exists(appDir)) {
//                                fs.mkdirs(appDir);
//                            }
//                            Path logPath = new Path(appDir, applicationId + "/logs/" + taskManagerInfo.getResourceId() + "/" + logInfo.getName());
//
//                            LOG.info("Trying to write log to path [{}]", logPath);
//                            IOUtils.copyBytes(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
//                                    fs.create(logPath, true), true);
//                        }
                    }
                }

            } finally {

            }

        }


    }

    private boolean hasTaskManagerConnectedAndReportedSlots(Collection<TaskManagerInfo> taskManagerInfos) {
        if (taskManagerInfos.isEmpty()) {
            return false;
        } else {
            final TaskManagerInfo taskManagerInfo = taskManagerInfos.iterator().next();
            LOG.info("task manager info = [{}]", taskManagerInfo);
            return taskManagerInfo.getNumberSlots() > 0;
        }
    }

    private static int handleError(Throwable t) {
        LOG.error("Error while running the command.", t);

        System.err.println();
        System.err.println("------------------------------------------------------------");
        System.err.println(" The program finished with the following exception:");
        System.err.println();

        if (t.getCause() instanceof InvalidProgramException) {
            System.err.println(t.getCause().getMessage());
            StackTraceElement[] trace = t.getCause().getStackTrace();
            for (StackTraceElement ele : trace) {
                System.err.println("\t" + ele);
                if (ele.getMethodName().equals("main")) {
                    break;
                }
            }
        } else {
            t.printStackTrace();
        }
        return 1;
    }


    public static void logEnvironmentInfo(Logger log, String componentName, String[] commandLineArgs) {
        if (log.isInfoEnabled()) {
            EnvironmentInformation.RevisionInformation rev = EnvironmentInformation.getRevisionInformation();
            String version = EnvironmentInformation.getVersion();
            String scalaVersion = EnvironmentInformation.getScalaVersion();

            String jvmVersion = EnvironmentInformation.getJvmVersion();
            String[] options = EnvironmentInformation.getJvmStartupOptionsArray();

            String javaHome = System.getenv("JAVA_HOME");

            String inheritedLogs = System.getenv("FLINK_INHERITED_LOGS");

            long maxHeapMegabytes = EnvironmentInformation.getMaxJvmHeapMemory() >>> 20;

            if (inheritedLogs != null) {
                log.info("--------------------------------------------------------------------------------");
                log.info(" Preconfiguration: ");
                log.info(inheritedLogs);
            }

            log.info("--------------------------------------------------------------------------------");
            log.info(" Starting " + componentName + " (Version: " + version + ", Scala: " + scalaVersion + ", "
                    + "Rev:" + rev.commitId + ", " + "Date:" + rev.commitDate + ")");
            log.info(" OS current user: " + System.getProperty("user.name"));
            log.info(" Current Hadoop/Kerberos user: " + EnvironmentInformation.getHadoopUser());
            log.info(" JVM: " + jvmVersion);
            log.info(" Maximum heap size: " + maxHeapMegabytes + " MiBytes");
            log.info(" JAVA_HOME: " + (javaHome == null ? "(not set)" : javaHome));

            String hadoopVersionString = EnvironmentInformation.getHadoopVersionString();
            if (hadoopVersionString != null) {
                log.info(" Hadoop version: " + hadoopVersionString);
            } else {
                log.info(" No Hadoop Dependency available");
            }

            if (options.length == 0) {
                log.info(" JVM Options: (none)");
            } else {
                log.info(" JVM Options:");
                for (String s : options) {
                    log.info("    " + s);
                }
            }

            if (commandLineArgs == null || commandLineArgs.length == 0) {
                log.info(" Program Arguments: (none)");
            } else {
                log.info(" Program Arguments:");
                for (String s : commandLineArgs) {
                    log.info("    " + s);
                }
            }
            log.info("--------------------------------------------------------------------------------");
        }
    }

    public static void main(String[] args) {
        logEnvironmentInfo(LOG, "Command Line Client", args);

        // 1. find the configuration directory
        final String configurationDirectory = getConfigurationDirectoryFromEnv();

        // 2. load the global configuration
        final Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        // 3. load the custom command lines
        final List<CustomCommandLine> customCommandLines = loadCustomCommandLines(
                configuration,
                configurationDirectory);

        try {
            final CliFrontend cli = new XJCliFrontend(
                    configuration,
                    customCommandLines);

            SecurityUtils.install(new SecurityConfiguration(configuration));
            int retCode = SecurityUtils.getInstalledContext()
                    .runSecured(() -> cli.parseParameters(args));
            System.exit(retCode);
        } catch (Throwable t) {
            final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            LOG.error("Fatal error while running command line interface.", strippedThrowable);
            strippedThrowable.printStackTrace();
            System.exit(31);
        }
    }
}