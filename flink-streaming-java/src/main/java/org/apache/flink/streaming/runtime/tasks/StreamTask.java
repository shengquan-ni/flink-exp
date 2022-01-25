/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import com.google.common.collect.HashBiMap;

import org.apache.flink.runtime.controller.ControlMessage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.util.recovery.EmptyLogStorage;
import org.apache.flink.streaming.util.recovery.HDFSLogStorage;
import org.apache.flink.streaming.util.recovery.LocalDiskLogStorage;
import org.apache.flink.runtime.security.FlinkSecurityManager;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction.Suspension;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.runtime.recovery.AbstractLogStorage;
import org.apache.flink.runtime.recovery.AsyncLogWriter;
import org.apache.flink.streaming.util.recovery.DPLogManager;
import org.apache.flink.streaming.util.recovery.DataLogManager;
import org.apache.flink.streaming.util.recovery.FutureWrapper;
import org.apache.flink.streaming.util.recovery.MailResolver;
import org.apache.flink.runtime.recovery.StepCursor;
import org.apache.flink.runtime.recovery.RecoveryUtils;
import org.apache.flink.streaming.util.recovery.MemoryStorage;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

import static org.apache.flink.runtime.concurrent.FutureUtils.assertNoException;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.ExceptionUtils.rethrowException;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed and
 * executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form the
 * Task's operator chain. Operators that are chained together execute synchronously in the same
 * thread and hence on the same stream partition. A common case for these chains are successive
 * map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators. The StreamTask is
 * specialized for the type of the head operator: one-input and two-input tasks, as well as for
 * sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 *
 * <pre>{@code
 * -- setInitialState -> provides state of all operators in the chain
 *
 * -- invoke()
 *       |
 *       +----> Create basic utils (config, etc) and load the chain of operators
 *       +----> operators.setup()
 *       +----> task specific init()
 *       +----> initialize-operator-states()
 *       +----> open-operators()
 *       +----> run()
 *       +----> close-operators()
 *       +----> dispose-operators()
 *       +----> common cleanup
 *       +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a {@code
 * StreamOperator} must be synchronized on this lock object to ensure that no methods are called
 * concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>> extends AbstractInvokable
        implements AsyncExceptionHandler {

    /** The thread group that holds all trigger timer threads. */
    public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

    /** The logger used by the StreamTask and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

    // ------------------------------------------------------------------------

    /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}.
     */
    private final StreamTaskActionExecutor actionExecutor;

    /** The input processor. Initialized in {@link #init()} method. */
    @Nullable protected StreamInputProcessor inputProcessor;

    /** the main operator that consumes the input streams of this task. */
    protected OP mainOperator;

    /** The chain of operators executed by this task. */
    protected OperatorChain<OUT, OP> operatorChain;

    /** The configuration of this streaming task. */
    protected final StreamConfig configuration;

    /** Our state backend. We use this to create a keyed state backend. */
    protected final StateBackend stateBackend;

    /** Our checkpoint storage. We use this to create checkpoint streams. */
    protected final CheckpointStorage checkpointStorage;

    private final SubtaskCheckpointCoordinator subtaskCheckpointCoordinator;

    /**
     * The internal {@link TimerService} used to define the current processing time (default =
     * {@code System.currentTimeMillis()}) and register timers for tasks to be executed in the
     * future.
     */
    protected final TimerService timerService;

    /**
     * In contrast to {@link #timerService} we should not register any user timers here. It should
     * be used only for system level timers.
     */
    protected final TimerService systemTimerService;

    /** The currently active background materialization threads. */
    private final CloseableRegistry cancelables = new CloseableRegistry();

    private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

    /**
     * Flag to mark the task "in operation", in which case check needs to be initialized to true, so
     * that early cancel() before invoke() behaves correctly.
     */
    private volatile boolean isRunning;

    /** Flag to mark this task as canceled. */
    private volatile boolean canceled;

    /**
     * Flag to mark this task as failing, i.e. if an exception has occurred inside {@link
     * #invoke()}.
     */
    private volatile boolean failing;

    private boolean disposedOperators;

    /** Thread pool for async snapshot workers. */
    private final ExecutorService asyncOperationsThreadPool;

    private final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter;

    protected final MailboxProcessor mailboxProcessor;

    final MailboxExecutor mainMailboxExecutor;

    /** TODO it might be replaced by the global IO executor on TaskManager level future. */
    private final ExecutorService channelIOExecutor;

    private Long syncSavepointId = null;
    private Long activeSyncSavepointId = null;
    private long latestAsyncCheckpointStartDelayNanos;
    private Timer timer = new Timer();
    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    protected AsyncLogWriter writer;
    protected MailResolver mailResolver;
    protected DataLogManager dataLogManager;
    public DPLogManager dpLogManager;
    public FutureWrapper isPausedFuture = new FutureWrapper();
    private String logName;
    private HashBiMap<Integer,ProcessingTimeCallback> hackCallbackMap = HashBiMap.create();

    // ------------------------------------------------------------------------

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    protected StreamTask(Environment env) throws Exception {
        this(env, null);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     */
    protected StreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
        this(env, timerService, FatalExitExceptionHandler.INSTANCE);
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                StreamTaskActionExecutor.IMMEDIATE);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the timer service) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param environment The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread
     *     pool
     * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently,
     *     only SynchronizedActionExecutor can be used to preserve locking semantics.
     */
    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                actionExecutor,
                new TaskMailboxImpl(Thread.currentThread()));
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor,
            TaskMailbox mailbox)
            throws Exception {

        super(environment);

        this.configuration = new StreamConfig(getTaskConfiguration());
        String id = getEnvironment().getJobVertexId().toString();
        TaskInfo info = getEnvironment().getTaskInfo();
        JobVertexID jobVId = getEnvironment().getJobVertexId();
        int subtaskIdx = info.getIndexOfThisSubtask();
        logName = "exampleJob-"+id.substring(id.length()-4)+"-"+info.getIndexOfThisSubtask();
        Map<String, String> globalArgs = environment.getExecutionConfig().getGlobalJobParameters().toMap();
        AbstractLogStorage storage = new EmptyLogStorage(logName);
        if(globalArgs.containsKey("enable-logging")){
            RecoveryUtils.isEnabled = Boolean.parseBoolean(globalArgs.get("enable-logging"));
            System.out.println("enable-logging = "+globalArgs.get("enable-logging"));
            if(RecoveryUtils.isEnabled) {
                if(globalArgs.containsKey("storage-type")) {
                    String t = globalArgs.get("storage-type");
                    if(t.equals("mem")){
                        storage = new MemoryStorage(logName);
                    }else if(t.equals("local")){
                        storage = new LocalDiskLogStorage(logName);
                    }
                    else if (globalArgs.containsKey("hdfs-log-storage")) {
                        storage = new HDFSLogStorage(logName, globalArgs.get("hdfs-log-storage"));
                        System.out.println(
                                "hdfs-log-storage = " + globalArgs.get("hdfs-log-storage"));
                    } else {
                        throw new RuntimeException("hdfs storage without a path1!!!!");
                    }
                }
            }
            if(globalArgs.containsKey("clear-old-log")){
                storage.clear();
            }
        }else{
            if(System.getProperty("enableLogging")!=null && System.getProperty("enableLogging").equals("true")){
                RecoveryUtils.isEnabled = true;
                System.out.println("enableLogging = true");

                if(storage instanceof EmptyLogStorage){
                    if(System.getProperty("storageType") != null) {
                        String t = System.getProperty("storageType");
                        if(t.equals("mem")){
                            storage = new MemoryStorage(logName);
                        }else if(t.equals("local")){
                            storage = new LocalDiskLogStorage(logName);
                        }else if(System.getProperty("hdfsLogStorage") != null){
                            storage = new HDFSLogStorage(logName, System.getProperty("hdfsLogStorage"));
                            System.out.println("hdfsLogStorage = "+System.getProperty("hdfsLogStorage"));
                        }else{
                            throw new RuntimeException("hdfs storage without a path2!!!!");
                        }
                    }
                }
            }
            if(System.getProperty("clearOldLog")!=null && System.getProperty("clearOldLog").equals("true")){
                storage.clear();
            }
            if(System.getProperty("logLevel") != null){
                RecoveryUtils.printLevel = Integer.parseInt(System.getProperty("logLevel"));
                System.out.println("logLevel = "+System.getProperty("logLevel"));
            }
        }
        if(globalArgs.containsKey("print-level")){
            RecoveryUtils.printLevel = Integer.parseInt(globalArgs.get("print-level"));
            System.out.println("print-level = "+globalArgs.get("print-level"));
        }
        writer = new AsyncLogWriter(storage);
        System.out.println(System.getProperty("enableOutputCache")==null?"enableOutputCache = null":("enableOutputCache = "+System.getProperty("enableOutputCache")));
        if(System.getProperty("enableOutputCache")!=null && System.getProperty("enableOutputCache").equals("true")) {
            System.out.println("enabled output cache for "+logName);
            writer.enableOutputCache();
        }
        StepCursor stepCursor = new StepCursor(storage.getStepCursor());
        for(ResultPartitionWriter rpWriter: environment.getAllWriters()){
            for(ResultSubpartition sub: ((BufferWritingResultPartition)rpWriter).subpartitions){
                ((PipelinedSubpartition)sub).registerOutput(writer, stepCursor);
            }
        }
        this.recordWriter = createRecordWriterDelegate(configuration, environment);
        this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
        this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor, environment.getTaskInfo().getTaskNameWithSubtasks());
        this.mainMailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
        if(globalArgs.get("control-delay") != null) {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if(mailbox.getState().isAcceptingMails()){
                        mainMailboxExecutor.execute(() -> {
                        }, "exp");
                    }
                }
            }, 1000, Integer.parseInt(globalArgs.get("control-delay")));
        }else{
            if(System.getProperty("controlDelay") != null) {
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        if(mailbox.getState().isAcceptingMails()){
                            mainMailboxExecutor.execute(() -> {
                            }, "exp");
                        }
                    }
                }, 1000, Integer.parseInt(System.getProperty("controlDelay")));
            }
        }
        this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);
        this.asyncOperationsThreadPool =
                Executors.newCachedThreadPool(
                        new ExecutorThreadFactory("AsyncOperations", uncaughtExceptionHandler));

        this.stateBackend = createStateBackend();
        this.checkpointStorage = createCheckpointStorage(stateBackend);

        this.subtaskCheckpointCoordinator =
                new SubtaskCheckpointCoordinatorImpl(
                        checkpointStorage.createCheckpointStorage(getEnvironment().getJobID()),
                        getName(),
                        actionExecutor,
                        getCancelables(),
                        getAsyncOperationsThreadPool(),
                        getEnvironment(),
                        this,
                        configuration.isUnalignedCheckpointsEnabled(),
                        this::prepareInputSnapshot);

        subtaskCheckpointCoordinator.registerLogWriter(writer);

        // if the clock is not already set, then assign a default TimeServiceProvider
        if (timerService == null) {
            this.timerService = createTimerService("Time Trigger for " + getName());
        } else {
            this.timerService = timerService;
        }
        this.timerService.registerLogWriter(Thread.currentThread().getId(), writer);
        this.systemTimerService = createTimerService("System Time Trigger for " + getName());
        this.channelIOExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("channel-state-unspilling"));

        injectChannelStateWriterIntoChannels();
        mailResolver = new MailResolver();
        mailResolver.bind("Timer callback",x ->{
                //System.out.println("timer callback thread: "+Thread.currentThread().getId());
                invokeProcessingTimeCallback(hackCallbackMap.get((int)x[0]),(long)x[1]);});
        int i = 0;
        for (InputGate inputGate : getEnvironment().getAllInputGates()) {
            mailResolver.bind("Input gate request partitions"+i, () -> {inputGate.requestPartitions();});
            i++;
        }
        mailResolver.bind("dispatch operator event", x -> operatorChain.dispatchOperatorEvent((OperatorID) x[0], (SerializedValue<OperatorEvent>) x[1]));
        mailResolver.bind("pause",() ->{
            mailboxProcessor.isPaused = true;
            isPausedFuture.reset();
        });
        mailResolver.bind("resume", () ->{
            mailboxProcessor.isPaused = false;
            isPausedFuture.set();
        });
        mailResolver.bind("exp", () ->{});
        final String name = info.getTaskName();
        mailResolver.bind("control", x ->{
            ControlMessage controlMessage = (ControlMessage)x[0];
            controlMessage.callback().accept(new Object[]{jobVId, subtaskIdx, name});
            if(controlMessage.EpochMode()){
                CheckpointBarrier barrier = new CheckpointBarrier(ControlMessage.FixedEpochNumber(), -1, CheckpointOptions.forCheckpointWithDefaultLocation());
                barrier.setMessage(controlMessage);
                operatorChain.broadcastEvent(barrier, false);
            }
        });

//        mailResolver.bind("checkpoint complete", (x)-> {notifyCheckpointComplete((long)x[0]);});
//
//        mailResolver.bind("checkpoint aborted", (x)-> {
//            resetSynchronousSavepointId((long)x[0], false);
//            subtaskCheckpointCoordinator.notifyCheckpointAborted(
//                     (long)x[0], operatorChain, this::isRunning);});
//
//        mailResolver.bind("checkpoint", (x) ->{
//            latestAsyncCheckpointStartDelayNanos =
//                    1_000_000
//                            * Math.max(
//                            0,
//                            System.currentTimeMillis()
//                                    - ((CheckpointMetaData)x[0]).getTimestamp());
//            triggerCheckpoint(((CheckpointMetaData)x[0]),(CheckpointOptions)x[1]);
//        });
        dpLogManager = new DPLogManager(writer, mailResolver, stepCursor);
        dataLogManager = new DataLogManager(writer, stepCursor);
        if(RecoveryUtils.isEnabled){
            dataLogManager.enable();
        }
        System.out.println("started "+logName+" at "+System.currentTimeMillis()+"recovery mode = "+!stepCursor.isRecoveryCompleted());
        environment.getMetricGroup().getIOMetricGroup().setEnableBusyTime(true);
    }

    @Override
    public void sendControl(ControlMessage controlMessage) {
        TaskInfo info = getEnvironment().getTaskInfo();
        String name = info.getTaskName();
        JobVertexID jobVId = getEnvironment().getJobVertexId();
        int subtaskIdx = info.getIndexOfThisSubtask();
        try{
            CompletableFuture<Void> f = new CompletableFuture<>();
            mainMailboxExecutor.execute(() -> {
                controlMessage.callback().accept(new Object[]{jobVId, subtaskIdx, name});
                if(controlMessage.EpochMode()){
                    CheckpointBarrier barrier = new CheckpointBarrier(ControlMessage.FixedEpochNumber(), -1, CheckpointOptions.forCheckpointWithDefaultLocation());
                    barrier.setMessage(controlMessage);
                    operatorChain.broadcastEvent(barrier, false);
                }
                f.complete(null);
            },"control",controlMessage);
            //f.get();
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    @Override
    public void pause() {
        System.out.println("StreamTask receives pause! current thread = "+Thread.currentThread().getName()+" "+Thread.currentThread().getId());
        mainMailboxExecutor.execute(() -> {},"pause");
    }

    @Override
    public void resume(){
        mainMailboxExecutor.execute(() -> {}, "resume");
    }

    @Override
    public CompletableFuture<?> shutdown() {
        timer.cancel();
        return writer.shutdown();
    }

    private TimerService createTimerService(String timerThreadName) {
        ThreadFactory timerThreadFactory =
                new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, timerThreadName);
        return new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
    }

    private void injectChannelStateWriterIntoChannels() {
        final Environment env = getEnvironment();
        final ChannelStateWriter channelStateWriter =
                subtaskCheckpointCoordinator.getChannelStateWriter();
        for (final InputGate gate : env.getAllInputGates()) {
            gate.setChannelStateWriter(channelStateWriter);
        }
        for (ResultPartitionWriter writer : env.getAllWriters()) {
            if (writer instanceof ChannelStateHolder) {
                ((ChannelStateHolder) writer).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    private CompletableFuture<Void> prepareInputSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        if (inputProcessor == null) {
            return FutureUtils.completedVoidFuture();
        }
        return inputProcessor.prepareSnapshot(channelStateWriter, checkpointId);
    }

    SubtaskCheckpointCoordinator getCheckpointCoordinator() {
        return subtaskCheckpointCoordinator;
    }

    // ------------------------------------------------------------------------
    //  Life cycle methods for specific implementations
    // ------------------------------------------------------------------------

    protected abstract void init() throws Exception;

    protected void cancelTask() throws Exception {}

    protected void cleanup() throws Exception {
        if (inputProcessor != null) {
            inputProcessor.close();
        }
    }

    /**
     * This method implements the default action of the task (e.g. processing one event from the
     * input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the action and the
     *     stream task.
     * @throws Exception on any problems in the action.
     */
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        InputStatus status = inputProcessor.processInput();
        if (status == InputStatus.MORE_AVAILABLE && recordWriter.isAvailable()) {
            return;
        }
        if (status == InputStatus.END_OF_INPUT) {
            controller.allActionsCompleted();
            return;
        }

        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        TimerGauge timer;
        CompletableFuture<?> resumeFuture;
        if (!recordWriter.isAvailable()) {
            timer = ioMetrics.getBackPressuredTimePerSecond();
            resumeFuture = recordWriter.getAvailableFuture();
        } else {
            timer = ioMetrics.getIdleTimeMsPerSecond();
            resumeFuture = inputProcessor.getAvailableFuture();
        }
        assertNoException(
                resumeFuture.thenRun(
                        new ResumeWrapper(controller.suspendDefaultAction(timer), timer)));
    }

    private void resetSynchronousSavepointId(long id, boolean succeeded) {
        if (!succeeded && activeSyncSavepointId != null && activeSyncSavepointId == id) {
            // allow to process further EndOfPartition events
            activeSyncSavepointId = null;
            operatorChain.setIgnoreEndOfInput(false);
        }
        syncSavepointId = null;
    }

    private void setSynchronousSavepointId(long checkpointId, boolean ignoreEndOfInput) {
        checkState(
                syncSavepointId == null,
                "at most one stop-with-savepoint checkpoint at a time is allowed");
        syncSavepointId = checkpointId;
        activeSyncSavepointId = checkpointId;
        operatorChain.setIgnoreEndOfInput(ignoreEndOfInput);
    }

    @VisibleForTesting
    OptionalLong getSynchronousSavepointId() {
        return syncSavepointId != null ? OptionalLong.of(syncSavepointId) : OptionalLong.empty();
    }

    private boolean isSynchronousSavepointId(long checkpointId) {
        return syncSavepointId != null && syncSavepointId == checkpointId;
    }

    private void runSynchronousSavepointMailboxLoop() throws Exception {
        assert syncSavepointId != null;

        MailboxExecutor mailboxExecutor =
                mailboxProcessor.getMailboxExecutor(TaskMailbox.MAX_PRIORITY);

        while (!canceled && syncSavepointId != null) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK
     * MAX_WATERMARK} so that all registered timers are fired.
     *
     * <p>This is used by the source task when the job is {@code TERMINATED}. In the case, we want
     * all the timers registered throughout the pipeline to fire and the related state (e.g.
     * windows) to be flushed.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void advanceToEndOfEventTime() throws Exception {}

    /**
     * Instructs the task to go through its normal termination routine, i.e. exit the run-loop and
     * call {@link StreamOperator#close()} and {@link StreamOperator#dispose()} on its operators.
     *
     * <p>This is used by the source task to get out of the run-loop when the job is stopped with a
     * savepoint.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void finishTask() throws Exception {}

    // ------------------------------------------------------------------------
    //  Core work methods of the Stream Task
    // ------------------------------------------------------------------------

    public StreamTaskStateInitializer createStreamTaskStateInitializer() {
        InternalTimeServiceManager.Provider timerServiceProvider =
                configuration.getTimerServiceProvider(getUserCodeClassLoader());
        return new StreamTaskStateInitializerImpl(
                getEnvironment(),
                stateBackend,
                TtlTimeProvider.DEFAULT,
                timerServiceProvider != null
                        ? timerServiceProvider
                        : InternalTimeServiceManagerImpl::create);
    }

    protected Counter setupNumRecordsInCounter(StreamOperator streamOperator) {
        try {
            return ((OperatorMetricGroup) streamOperator.getMetricGroup())
                    .getIOMetricGroup()
                    .getNumRecordsInCounter();
        } catch (Exception e) {
            LOG.warn("An exception occurred during the metrics setup.", e);
            return new SimpleCounter();
        }
    }

    @Override
    public final void restore() throws Exception {
        runWithCleanUpOnFail(this::executeRestore);
    }

    void executeRestore() throws Exception {
        mailboxProcessor.registerLogManager(dpLogManager);
        if (isRunning) {
            LOG.debug("Re-restore attempt rejected.");
            return;
        }
        disposedOperators = false;
        LOG.debug("Initializing {}.", getName());

        operatorChain = new OperatorChain<>(this, recordWriter);
        mainOperator = operatorChain.getMainOperator();

        // task specific initialization
        //TODO: create logger and storage here
        init();

        // save the work of reloading state, etc, if the task is already canceled
        ensureNotCanceled();

        // -------- Invoke --------
        LOG.debug("Invoking {}", getName());

//        System.out.println(logName + " all input gate start to consume states!");
//
//        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
//
//        for (InputGate inputGate : inputGates) {
//            inputGate.getStateConsumedFuture().get();
//        }
//        System.out.println(logName + " all input gate consumed states!");

        // we need to make sure that any triggers scheduled in open() cannot be
        // executed before all operators are opened
        CompletableFuture<Void> allGatesRecoveredFuture = actionExecutor.call(this::restoreGates);

        // Run mailbox until all gates will be recovered.
        mailboxProcessor.runMailboxLoop();

        ensureNotCanceled();

        checkState(
                allGatesRecoveredFuture.isDone(),
                "Mailbox loop interrupted before recovery was finished.");

        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
        int i = 0;
        for (InputGate inputGate : inputGates) {
            final int fi = i;
            mainMailboxExecutor.execute(
                    inputGate::requestPartitions,
                    "Input gate request partitions" + fi);
            i++;
        }

        isRunning = true;
    }

    private CompletableFuture<Void> restoreGates() throws Exception {
        SequentialChannelStateReader reader =
                getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
        reader.readOutputData(
                getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

        operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
        channelIOExecutor.execute(
                () -> {
                    try {
                        reader.readInputData(inputGates);
                    } catch (Exception e) {
                        asyncExceptionHandler.handleAsyncException(
                                "Unable to read channel state", e);
                    }
                });

        List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
        for (InputGate inputGate : inputGates) {
            recoveredFutures.add(inputGate.getStateConsumedFuture());
        }

        return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0])).thenRun(() -> {
            if(RecoveryUtils.isEnabled) {
                dpLogManager.enable();
            }
        })
                .thenRun(mailboxProcessor::suspend);
    }

    private void ensureNotCanceled() {
        if (canceled) {
            throw new CancelTaskException();
        }
    }

    @Override
    public final void invoke() throws Exception {
        runWithCleanUpOnFail(this::executeInvoke);

        cleanUpInvoke();
    }

    private void executeInvoke() throws Exception {
        // Allow invoking method 'invoke' without having to call 'restore' before it.
        if (!isRunning) {
            LOG.debug("Restoring during invoke will be called.");
            executeRestore();
        }

        // final check to exit early before starting to run
        ensureNotCanceled();


        // let the task do its work
        runMailboxLoop();

        // if this left the run() method cleanly despite the fact that this was canceled,
        // make sure the "clean shutdown" is not attempted
        ensureNotCanceled();

        afterInvoke();
    }

    private void runWithCleanUpOnFail(RunnableWithException run) throws Exception {
        try {
            run.run();
        } catch (Throwable invokeException) {
            failing = !canceled;
            try {
                if (!canceled) {
                    try {
                        cancelTask();
                    } catch (Throwable ex) {
                        invokeException = firstOrSuppressed(ex, invokeException);
                    }
                }

                cleanUpInvoke();
            }
            // TODO: investigate why Throwable instead of Exception is used here.
            catch (Throwable cleanUpException) {
                rethrowException(firstOrSuppressed(cleanUpException, invokeException));
            }

            rethrowException(invokeException);
        }
    }

    @VisibleForTesting
    public boolean runMailboxStep() throws Exception {
        return mailboxProcessor.runMailboxStep();
    }

    @VisibleForTesting
    public boolean isMailboxLoopRunning() {
        return mailboxProcessor.isMailboxLoopRunning();
    }

    public void runMailboxLoop() throws Exception {
        mailboxProcessor.runMailboxLoop();
    }

    protected void afterInvoke() throws Exception {
        LOG.debug("Finished task {}", getName());
        getCompletionFuture().exceptionally(unused -> null).join();

        final CompletableFuture<Void> timersFinishedFuture = new CompletableFuture<>();

        // close all operators in a chain effect way
        operatorChain.closeOperators(actionExecutor);

        // make sure no further checkpoint and notification actions happen.
        // at the same time, this makes sure that during any "regular" exit where still
        actionExecutor.runThrowing(
                () -> {

                    // make sure no new timers can come
                    FutureUtils.forward(timerService.quiesce(), timersFinishedFuture);

                    // let mailbox execution reject all new letters from this point
                    mailboxProcessor.prepareClose();

                    // only set the StreamTask to not running after all operators have been closed!
                    // See FLINK-7430
                    isRunning = false;
                });
        // processes the remaining mails; no new mails can be enqueued
        mailboxProcessor.drain();

        // make sure all timers finish
        timersFinishedFuture.get();

        LOG.debug("Closed operators for task {}", getName());

        // make sure all buffered data is flushed
        operatorChain.flushOutputs();

        // make an attempt to dispose the operators such that failures in the dispose call
        // still let the computation fail
        disposeAllOperators();
    }

    protected void cleanUpInvoke() throws Exception {
        getCompletionFuture().exceptionally(unused -> null).join();
        // clean up everything we initialized
        isRunning = false;

        // Now that we are outside the user code, we do not want to be interrupted further
        // upon cancellation. The shutdown logic below needs to make sure it does not issue calls
        // that block and stall shutdown.
        // Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
        // process) as a backup in case some shutdown procedure blocks outside our control.
        setShouldInterruptOnCancel(false);

        // clear any previously issued interrupt for a more graceful shutdown
        Thread.interrupted();

        // stop all timers and threads
        Exception suppressedException =
                runAndSuppressThrowable(this::tryShutdownTimerService, null);

        // stop all asynchronous checkpoint threads
        suppressedException = runAndSuppressThrowable(cancelables::close, suppressedException);
        suppressedException =
                runAndSuppressThrowable(this::shutdownAsyncThreads, suppressedException);

        // we must! perform this cleanup
        suppressedException = runAndSuppressThrowable(this::cleanup, suppressedException);

        // if the operators were not disposed before, do a hard dispose
        suppressedException =
                runAndSuppressThrowable(this::disposeAllOperators, suppressedException);

        // release the output resources. this method should never fail.
        suppressedException =
                runAndSuppressThrowable(this::releaseOutputResources, suppressedException);

        suppressedException =
                runAndSuppressThrowable(channelIOExecutor::shutdown, suppressedException);

        suppressedException = runAndSuppressThrowable(mailboxProcessor::close, suppressedException);

        if (suppressedException == null) {
            terminationFuture.complete(null);
        } else {
            terminationFuture.completeExceptionally(suppressedException);
            throw suppressedException;
        }

    }

    protected CompletableFuture<Void> getCompletionFuture() {
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public final Future<Void> cancel() throws Exception {
        isRunning = false;
        canceled = true;

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        // the "cancel task" call must come first, but the cancelables must be
        // closed no matter what
        try {
            cancelTask();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            getCompletionFuture()
                    .whenComplete(
                            (unusedResult, unusedError) -> {
                                // WARN: the method is called from the task thread but the callback
                                // can be invoked from a different thread
                                mailboxProcessor.allActionsCompleted();
                                try {
                                    cancelables.close();
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }
                            });
        }
        return terminationFuture;
    }

    public MailboxExecutorFactory getMailboxExecutorFactory() {
        return this.mailboxProcessor::getMailboxExecutor;
    }

    public final boolean isRunning() {
        return isRunning;
    }

    public final boolean isCanceled() {
        return canceled;
    }

    public final boolean isFailing() {
        return failing;
    }

    private void shutdownAsyncThreads() throws Exception {
        if (!asyncOperationsThreadPool.isShutdown()) {
            asyncOperationsThreadPool.shutdownNow();
        }
    }

    private void releaseOutputResources() throws Exception {
        if (operatorChain != null) {
            // beware: without synchronization, #performCheckpoint() may run in
            //         parallel and this call is not thread-safe
            actionExecutor.run(() -> operatorChain.releaseOutputs());
        } else {
            // failed to allocate operatorChain, clean up record writers
            recordWriter.close();
        }
    }

    private Exception runAndSuppressThrowable(
            ThrowingRunnable<?> runnable, @Nullable Exception originalException) {
        try {
            runnable.run();
        } catch (Throwable t) {
            // TODO: investigate why Throwable instead of Exception is used here.
            Exception e = t instanceof Exception ? (Exception) t : new Exception(t);
            return firstOrSuppressed(e, originalException);
        }

        return originalException;
    }

    /**
     * Execute @link StreamOperator#dispose()} of each operator in the chain of this {@link
     * StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
     */
    private void disposeAllOperators() throws Exception {
        if (operatorChain != null && !disposedOperators) {
            Exception disposalException = null;
            for (StreamOperatorWrapper<?, ?> operatorWrapper :
                    operatorChain.getAllOperators(true)) {
                StreamOperator<?> operator = operatorWrapper.getStreamOperator();
                try {
                    operator.dispose();
                } catch (Exception e) {
                    disposalException = firstOrSuppressed(e, disposalException);
                }
            }
            disposedOperators = true;
            if (disposalException != null) {
                throw disposalException;
            }
        }
    }

    /**
     * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
     * shutdown method was never called.
     *
     * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
     * shutdown is attempted, and cause threads to linger for longer than needed.
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!timerService.isTerminated()) {
            LOG.info("Timer service is shutting down.");
            timerService.shutdownService();
        }

        if (!systemTimerService.isTerminated()) {
            LOG.info("System timer service is shutting down.");
            systemTimerService.shutdownService();
        }

        cancelables.close();
    }

    boolean isSerializingTimestamps() {
        TimeCharacteristic tc = configuration.getTimeCharacteristic();
        return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
    }

    // ------------------------------------------------------------------------
    //  Access to properties and utilities
    // ------------------------------------------------------------------------

    /**
     * Gets the name of the task, in the form "taskname (2/5)".
     *
     * @return The name of the task.
     */
    public final String getName() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
    }

    /**
     * Gets the name of the task, appended with the subtask indicator and execution id.
     *
     * @return The name of the task, with subtask indicator and execution id.
     */
    String getTaskNameWithSubtaskAndId() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks()
                + " ("
                + getEnvironment().getExecutionId()
                + ')';
    }

    public CheckpointStorageWorkerView getCheckpointStorage() {
        return subtaskCheckpointCoordinator.getCheckpointStorage();
    }

    public StreamConfig getConfiguration() {
        return configuration;
    }

    public StreamStatusMaintainer getStreamStatusMaintainer() {
        return operatorChain;
    }

    RecordWriterOutput<?>[] getStreamOutputs() {
        return operatorChain.getStreamOutputs();
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and Restore
    // ------------------------------------------------------------------------

    @Override
    public Future<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    latestAsyncCheckpointStartDelayNanos =
                            1_000_000
                                    * Math.max(
                                            0,
                                            System.currentTimeMillis()
                                                    - checkpointMetaData.getTimestamp());
                    result.complete(triggerCheckpoint(checkpointMetaData, checkpointOptions));
                },
                "checkpoint",
                checkpointMetaData,
                checkpointOptions);
        return result;
    }

    private boolean triggerCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            // No alignment if we inject a checkpoint
            CheckpointMetricsBuilder checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L);

            subtaskCheckpointCoordinator.initInputsCheckpoint(
                    checkpointMetaData.getCheckpointId(), checkpointOptions);

            boolean success =
                    performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
            if (!success) {
                declineCheckpoint(checkpointMetaData.getCheckpointId());
            }
            return success;
        } catch (Exception e) {
            // propagate exceptions only if the task is still in "running" state
            if (isRunning) {
                throw new Exception(
                        "Could not perform checkpoint "
                                + checkpointMetaData.getCheckpointId()
                                + " for operator "
                                + getName()
                                + '.',
                        e);
            } else {
                LOG.debug(
                        "Could not perform checkpoint {} for operator {} while the "
                                + "invokable was not in state running.",
                        checkpointMetaData.getCheckpointId(),
                        getName(),
                        e);
                return false;
            }
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            if (performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics)) {
                if (isSynchronousSavepointId(checkpointMetaData.getCheckpointId())) {
                    runSynchronousSavepointMailboxLoop();
                }
            }
        } catch (CancelTaskException e) {
            LOG.info(
                    "Operator {} was cancelled while performing checkpoint {}.",
                    getName(),
                    checkpointMetaData.getCheckpointId());
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Could not perform checkpoint "
                            + checkpointMetaData.getCheckpointId()
                            + " for operator "
                            + getName()
                            + '.',
                    e);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        resetSynchronousSavepointId(checkpointId, false);
        subtaskCheckpointCoordinator.abortCheckpointOnBarrier(checkpointId, cause, operatorChain);
    }

    private boolean performCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws Exception {

        System.out.println(
                String.format("Starting checkpoint (%d) %s on task %s",
                checkpointMetaData.getCheckpointId(),
                checkpointOptions.getCheckpointType(),
                getName()));

        if (isRunning) {
            actionExecutor.runThrowing(
                    () -> {
                        if (checkpointOptions.getCheckpointType().isSynchronous()) {
                            setSynchronousSavepointId(
                                    checkpointMetaData.getCheckpointId(),
                                    checkpointOptions.getCheckpointType().shouldIgnoreEndOfInput());

                            if (checkpointOptions.getCheckpointType().shouldAdvanceToEndOfTime()) {
                                advanceToEndOfEventTime();
                            }
                        } else if (activeSyncSavepointId != null
                                && activeSyncSavepointId < checkpointMetaData.getCheckpointId()) {
                            activeSyncSavepointId = null;
                            operatorChain.setIgnoreEndOfInput(false);
                        }

                        subtaskCheckpointCoordinator.checkpointState(
                                checkpointMetaData,
                                checkpointOptions,
                                checkpointMetrics,
                                operatorChain,
                                this::isRunning);
                    });

            return true;
        } else {
            actionExecutor.runThrowing(
                    () -> {
                        // we cannot perform our checkpoint - let the downstream operators know that
                        // they
                        // should not wait for any input from this operator

                        // we cannot broadcast the cancellation markers on the 'operator chain',
                        // because it may not
                        // yet be created
                        final CancelCheckpointMarker message =
                                new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
                        recordWriter.broadcastEvent(message);
                    });

            return false;
        }
    }

    protected void declineCheckpoint(long checkpointId) {
        getEnvironment()
                .declineCheckpoint(
                        checkpointId,
                        new CheckpointException(
                                "Task Name" + getName(),
                                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
    }

    public final ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        System.out.println("notify chkpt completed received!");
        return notifyCheckpointOperation(
                () -> notifyCheckpointComplete(checkpointId),
                "checkpoint complete", checkpointId);
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () -> {
                    resetSynchronousSavepointId(checkpointId, false);
                    subtaskCheckpointCoordinator.notifyCheckpointAborted(
                            checkpointId, operatorChain, this::isRunning);
                },
                "checkpoint aborted", checkpointId);
    }

    private Future<Void> notifyCheckpointOperation(
            RunnableWithException runnable, String description, Object... params) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        mailboxProcessor
                .getMailboxExecutor(TaskMailbox.MAX_PRIORITY)
                .execute(
                        () -> {
                            try {
                                runnable.run();
                            } catch (Exception ex) {
                                result.completeExceptionally(ex);
                                throw ex;
                            }
                            result.complete(null);
                        },
                        description, params);
        return result;
    }

    private void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("executing chkpt complete!");
        writer.clearCachedOutput();
        subtaskCheckpointCoordinator.notifyCheckpointComplete(
                checkpointId, operatorChain, this::isRunning);
        if (isRunning && isSynchronousSavepointId(checkpointId)) {
            finishTask();
            // Reset to "notify" the internal synchronous savepoint mailbox loop.
            resetSynchronousSavepointId(checkpointId, true);
        }
    }

    private void tryShutdownTimerService() {
        final long timeoutMs =
                getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);
        tryShutdownTimerService(timeoutMs, timerService);
        tryShutdownTimerService(timeoutMs, systemTimerService);
    }

    private void tryShutdownTimerService(long timeoutMs, TimerService timerService) {
        if (!timerService.isTerminated()) {
            if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
                LOG.warn(
                        "Timer service shutdown exceeded time limit of {} ms while waiting for pending "
                                + "timers. Will continue with shutdown procedure.",
                        timeoutMs);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Operator Events
    // ------------------------------------------------------------------------

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        try {
            mainMailboxExecutor.execute(
                    () -> operatorChain.dispatchOperatorEvent(operator, event),
                    "dispatch operator event");
        } catch (RejectedExecutionException e) {
            // this happens during shutdown, we can swallow this
        }
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------

    private StateBackend createStateBackend() throws Exception {
        final StateBackend fromApplication =
                configuration.getStateBackend(getUserCodeClassLoader());

        return StateBackendLoader.fromApplicationOrConfigOrDefault(
                fromApplication,
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    private CheckpointStorage createCheckpointStorage(StateBackend backend) throws Exception {
        final CheckpointStorage fromApplication =
                configuration.getCheckpointStorage(getUserCodeClassLoader());
        final Path savepointDir = configuration.getSavepointDir(getUserCodeClassLoader());

        return CheckpointStorageLoader.load(
                fromApplication,
                savepointDir,
                backend,
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    /**
     * Returns the {@link TimerService} responsible for telling the current processing time and
     * registering actual timers.
     */
    @VisibleForTesting
    TimerService getTimerService() {
        return timerService;
    }

    @VisibleForTesting
    OP getMainOperator() {
        return this.mainOperator;
    }

    @VisibleForTesting
    StreamTaskActionExecutor getActionExecutor() {
        return actionExecutor;
    }

    public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
        return mailboxExecutor ->
                new ProcessingTimeServiceImpl(
                        timerService,
                        callback -> deferCallbackToMailbox(mailboxExecutor, callback));
    }

    /**
     * Handles an exception thrown by another thread (e.g. a TriggerTask), other than the one
     * executing the main task by failing the task entirely.
     *
     * <p>In more detail, it marks task execution failed for an external reason (a reason other than
     * the task code itself throwing an exception). If the task is already in a terminal state (such
     * as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
     * Otherwise it sets the state to FAILED, and, if the invokable code is running, starts an
     * asynchronous thread that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void handleAsyncException(String message, Throwable exception) {
        if (isRunning) {
            // only fail if the task is still running
            asyncExceptionHandler.handleAsyncException(message, exception);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return getName();
    }

    // ------------------------------------------------------------------------

    /** Utility class to encapsulate the handling of asynchronous exceptions. */
    static class StreamTaskAsyncExceptionHandler {
        private final Environment environment;

        StreamTaskAsyncExceptionHandler(Environment environment) {
            this.environment = environment;
        }

        void handleAsyncException(String message, Throwable exception) {
            environment.failExternally(new AsynchronousException(message, exception));
        }
    }

    public final CloseableRegistry getCancelables() {
        return cancelables;
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static <OUT>
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>>
                    createRecordWriterDelegate(
                            StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites =
                createRecordWriters(configuration, environment);
        if (recordWrites.size() == 1) {
            return new SingleRecordWriter<>(recordWrites.get(0));
        } else if (recordWrites.size() == 0) {
            return new NonRecordWriter<>();
        } else {
            return new MultipleRecordWriters<>(recordWrites);
        }
    }

    private static <OUT>
            List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
                    StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters =
                new ArrayList<>();
        List<StreamEdge> outEdgesInOrder =
                configuration.getOutEdgesInOrder(
                        environment.getUserCodeClassLoader().asClassLoader());

        for (int i = 0; i < outEdgesInOrder.size(); i++) {
            StreamEdge edge = outEdgesInOrder.get(i);
            recordWriters.add(
                    createRecordWriter(
                            edge,
                            i,
                            environment,
                            environment.getTaskInfo().getTaskName(),
                            edge.getBufferTimeout()));
        }
        return recordWriters;
    }

    @SuppressWarnings("unchecked")
    private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
            StreamEdge edge,
            int outputIndex,
            Environment environment,
            String taskName,
            long bufferTimeout) {

        StreamPartitioner<OUT> outputPartitioner = null;

        // Clones the partition to avoid multiple stream edges sharing the same stream partitioner,
        // like the case of https://issues.apache.org/jira/browse/FLINK-14087.
        try {
            outputPartitioner =
                    InstantiationUtil.clone(
                            (StreamPartitioner<OUT>) edge.getPartitioner(),
                            environment.getUserCodeClassLoader().asClassLoader());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }

        LOG.debug(
                "Using partitioner {} for output {} of task {}",
                outputPartitioner,
                outputIndex,
                taskName);

        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }

        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        .setChannelSelector(outputPartitioner)
                        .setTimeout(bufferTimeout)
                        .setTaskName(taskName)
                        .build(bufferWriter);
        output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
        return output;
    }

    private void handleTimerException(Exception ex) {
        handleAsyncException("Caught exception while processing timer.", new TimerException(ex));
    }

    @VisibleForTesting
    ProcessingTimeCallback deferCallbackToMailbox(
            MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
        int key;
        if(hackCallbackMap.containsValue(callback)){
            key = hackCallbackMap.inverse().get(callback);
        }else{
            key = hackCallbackMap.size();
            hackCallbackMap.put(key,callback);
        }
        return timestamp -> {
            mailboxExecutor.execute(
                    () -> invokeProcessingTimeCallback(callback, timestamp),
                    "Timer callback",
                    key,
                    timestamp);
        };
    }

    private void invokeProcessingTimeCallback(ProcessingTimeCallback callback, long timestamp) {
        try {
            callback.onProcessingTime(timestamp);
        } catch (Throwable t) {
            handleAsyncException("Caught exception while processing timer.", new TimerException(t));
        }
    }

    protected long getAsyncCheckpointStartDelayNanos() {
        return latestAsyncCheckpointStartDelayNanos;
    }

    private static class ResumeWrapper implements Runnable {
        private final Suspension suspendedDefaultAction;
        private final TimerGauge timer;

        public ResumeWrapper(Suspension suspendedDefaultAction, TimerGauge timer) {
            this.suspendedDefaultAction = suspendedDefaultAction;
            timer.markStart();
            this.timer = timer;
        }

        @Override
        public void run() {
            timer.markEnd();
            suspendedDefaultAction.resume();
        }
    }

    @Override
    public boolean isUsingNonBlockingInput() {
        return true;
    }
}
