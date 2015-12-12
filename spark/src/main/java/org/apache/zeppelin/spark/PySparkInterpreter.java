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

package org.apache.zeppelin.spark;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.repl.SparkIMain;
import org.apache.spark.repl.SparkJLineCompletion;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.WrappedInterpreter;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.spark.dep.DependencyContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.*;
import lombok.Getter;
import py4j.GatewayServer;
import scala.None;
import scala.Some;
import scala.tools.nsc.Settings;

/**
 *
 */
public class PySparkInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(PySparkInterpreter.class);
  Map<String, PythonInterpretRequest> pythonInterpretRequestMap;

  private int port;

  private String scriptPath;

  private LoadingCache<String, PySparkInterpreterContext> pySparkInterpreterContextMap;

  static {
    Interpreter.register(
      "pyspark",
      "spark",
      PySparkInterpreter.class.getName(),
      new InterpreterPropertyBuilder()
        .add("zeppelin.pyspark.python",
          SparkInterpreter.getSystemDefault("PYSPARK_PYTHON", null, "python"),
          "Python command to run pyspark with").build());
  }

  public PySparkInterpreter(Properties property) {
    super(property);

    scriptPath = System.getProperty("java.io.tmpdir") + "/zeppelin_pyspark.py";
    pySparkInterpreterContextMap = CacheBuilder.newBuilder()
      .expireAfterAccess(24, TimeUnit.HOURS) //TODO: make it parameter
      .removalListener(PySparkInterpreterContextRemovalListener)
      .build(PySparkInterpreterContextLoader);
  }

  private class PySparkInterpreterContext implements ExecuteResultHandler {

    private String noteID;
    Boolean pythonScriptInitialized = Boolean.FALSE; // track if the process was initialized
    private Boolean pythonscriptRunning = Boolean.FALSE; // track if process was created
    private ByteArrayOutputStream outputStream;
    private BufferedWriter ins;
    private PipedInputStream in;
    private ByteArrayOutputStream input;
    private GatewayServer gatewayServer;
    private DefaultExecutor executor;
    PythonInterpretRequest pythonInterpretRequest = null;


    PySparkInterpreterContext(String noteID, DefaultExecutor executor, GatewayServer gatewayServer, ByteArrayOutputStream outputStream,
                              BufferedWriter ins, PipedInputStream in, ByteArrayOutputStream input) {
      this.gatewayServer = gatewayServer;
      this.outputStream = outputStream;
      this.ins = ins;
      this.in = in;
      this.input = input;
      pythonInterpretRequestMap = new HashMap<String, PythonInterpretRequest>();
    }

    public void init() {}
    @Override
    public void onProcessComplete(int exitValue) {
      pythonscriptRunning = Boolean.FALSE;
      logger.info(noteID + " : python process terminated. exit code " + exitValue);
    }

    @Override
    public void onProcessFailed(ExecuteException e) {
      pythonscriptRunning = Boolean.FALSE;
      logger.error(noteID + " : python process failed", e);
    }
  }

  // end of PySparkInterpreterContext class

  CacheLoader<String, PySparkInterpreterContext> PySparkInterpreterContextLoader =
    new CacheLoader<String, PySparkInterpreterContext>() {
      public PySparkInterpreterContext load(String key) {
        return open(key);
      }
    };

  RemovalListener<String, PySparkInterpreterContext> PySparkInterpreterContextRemovalListener =
    new RemovalListener<String, PySparkInterpreterContext>() {
      @Override
      public void onRemoval(RemovalNotification<String, PySparkInterpreterContext> removalNotification) {
        logger.info("removed Key =" + removalNotification.getKey() + "due to " + removalNotification.getCause());
      }
    };

  private void createPythonScript(String noteID) {
    ClassLoader classLoader = getClass().getClassLoader();
    File out = new File(scriptPath+"."+noteID);

    if (out.exists() && out.isDirectory()) {
      throw new InterpreterException("Can't create python script " + out.getAbsolutePath());
    }

    try {
      FileOutputStream outStream = new FileOutputStream(out);
      IOUtils.copy(
        classLoader.getResourceAsStream("python/zeppelin_pyspark.py"),
        outStream);
      outStream.close();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }

    logger.info("File {} created", scriptPath+"."+noteID);
  }

  @Override
  public void open() {

  }

  public PySparkInterpreterContext open(String noteID) {
    synchronized (this) {
      DefaultExecutor executor;
      GatewayServer gatewayServer;
      ByteArrayOutputStream outputStream;
      BufferedWriter ins;
      PipedInputStream in;
      ByteArrayOutputStream input;

      if (pySparkInterpreterContextMap.asMap().containsKey(noteID)){
        try {
          return pySparkInterpreterContextMap.get(noteID);
        } catch (ExecutionException e) {
          throw new InterpreterException("Error getting PySparkInterpreterContext");
        }
      }

      DepInterpreter depInterpreter = getDepInterpreter();
      // load libraries from Dependency Interpreter
      URL[] urls = new URL[0];

      if (depInterpreter != null) {
        DependencyContext depc = depInterpreter.getDependencyContext();
        if (depc != null) {
          List<File> files = depc.getFiles();
          List<URL> urlList = new LinkedList<URL>();
          if (files != null) {
            for (File f : files) {
              try {
                urlList.add(f.toURI().toURL());
              } catch (MalformedURLException e) {
                logger.error("Error", e);
              }
            }

            urls = urlList.toArray(urls);
          }
        }
      }

      ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
      try {
        URLClassLoader newCl = new URLClassLoader(urls, oldCl);
        Thread.currentThread().setContextClassLoader(newCl);

        createPythonScript(noteID);
      } catch (Exception e) {
        logger.error("Error", e);
        throw new InterpreterException(e);
      } finally {
        Thread.currentThread().setContextClassLoader(oldCl);
      }

      port = findRandomOpenPortOnAllLocalInterfaces();

      gatewayServer = new GatewayServer(this, port);
      gatewayServer.start();

      // Run python shell
      CommandLine cmd = CommandLine.parse(getProperty("zeppelin.pyspark.python"));
      cmd.addArgument(scriptPath + "." + noteID, false);
      cmd.addArgument(Integer.toString(port), false);
      cmd.addArgument(Integer.toString(getSparkInterpreter().getSparkVersion().toNumber()), false);
      cmd.addArgument(noteID, false);
      executor = new DefaultExecutor();
      outputStream = new ByteArrayOutputStream();
      PipedOutputStream ps = new PipedOutputStream();
      in = null;
      try {
        in = new PipedInputStream(ps);
      } catch (IOException e1) {
        throw new InterpreterException(e1);
      }
      ins = new BufferedWriter(new OutputStreamWriter(ps));

      input = new ByteArrayOutputStream();

      PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, outputStream, in);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));

      PySparkInterpreterContext pySparkInterpreterContext = new PySparkInterpreterContext(noteID, executor, gatewayServer, outputStream, ins, in, input);

      try {
        Map env = EnvironmentUtils.getProcEnvironment();
        logger.info("Launching : " + cmd.getExecutable() + " " + scriptPath
          + "." + noteID + " " + port + " " + getJavaSparkContext().version() + " " + noteID);
        executor.execute(cmd, env, pySparkInterpreterContext);
        pySparkInterpreterContext.pythonscriptRunning = Boolean.TRUE;
      } catch (IOException e) {
        throw new InterpreterException(e);
      }


      try {
        input.write("import sys, getopt\n".getBytes());
        ins.flush();
      } catch (IOException e) {
        throw new InterpreterException(e);
      }

      return pySparkInterpreterContext;
    }
  }

  private int findRandomOpenPortOnAllLocalInterfaces() {
    int port;
    try (ServerSocket socket = new ServerSocket(0);) {
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
    return port;
  }

  @Override
  public void close() {
    for (PySparkInterpreterContext pySparkInterpreterContext : pySparkInterpreterContextMap.asMap().values()) {
      pySparkInterpreterContext.executor.getWatchdog().destroyProcess();
      pySparkInterpreterContext.gatewayServer.shutdown();
    }
  }


  /**
   *
   */

  public class PythonInterpretRequest {
    public String statements;
    public String jobGroup;
    public String noteID;
    public String paragraphID;
    public Boolean completed;
    public String statementOutput;
    public Boolean statementError;

    public PythonInterpretRequest(String statements, String jobGroup, String noteID, String paragraphID) {
      this.statements = statements;
      this.jobGroup = jobGroup;
      this.noteID = noteID;
      this.paragraphID = paragraphID;
      completed = Boolean.FALSE;
      statementOutput = null;
      statementError = Boolean.FALSE;
    }

    public String statements() {
      return statements;
    }

    public String jobGroup() {
      return jobGroup;
    }

    @Override
    public String toString() {
      return "NoteID :" + noteID + " : ParagraphID :" + paragraphID + " : statements : " + statements;
    }

  }

  // end of PythonInterpretRequest class

  public PythonInterpretRequest getStatements(String noteID) {
    synchronized (pythonInterpretRequestMap) {
      PythonInterpretRequest pythonInterpretRequest = pythonInterpretRequestMap.get(noteID);
      if (pythonInterpretRequest == null) {
        return null;
      } else {
        return pythonInterpretRequest;
      }
    }
  }

  public void setStatementsFinished(String noteID, String out, boolean error) {
    if (!pythonInterpretRequestMap.containsKey(noteID)) {
      logger.error("Something wrong happened. How can pythonInterpretRequestMap not have entry for " + noteID);
      return;
    }
    synchronized (pythonInterpretRequestMap.get(noteID).completed) {
      pythonInterpretRequestMap.get(noteID).statementOutput = out;
      pythonInterpretRequestMap.get(noteID).statementError = error;
      pythonInterpretRequestMap.get(noteID).completed = Boolean.TRUE;
    }
    synchronized (pythonInterpretRequestMap) {
      pythonInterpretRequestMap.remove(noteID);
    }
  }

  public void onPythonScriptInitialized(String noteID) {
    synchronized (pySparkInterpreterContextMap) {
      while (true) {
        // it can take a while before the pySparkInterpreterContextMap LoadingCache is really populated
        if (pySparkInterpreterContextMap.asMap().containsKey(noteID)) {
          try {
            pySparkInterpreterContextMap.get(noteID).pythonScriptInitialized = Boolean.TRUE;
            break;
          } catch (ExecutionException e) {}
        } else {
          try {
            pySparkInterpreterContextMap.wait(500);
          } catch (InterruptedException e) {
          }
        }
      }
    }
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelNotebookScheduler("interpreter_" + this.hashCode(), 100);
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    if (sparkInterpreter.getSparkVersion().isUnsupportedVersion()) {
      return new InterpreterResult(Code.ERROR, "Spark "
        + sparkInterpreter.getSparkVersion().toString() + " is not supported");
    }

    PySparkInterpreterContext pySparkInterpreterContext;

    try {
      pySparkInterpreterContext = pySparkInterpreterContextMap.get(context.getNoteId());
    } catch (ExecutionException | InterpreterException e) {
      logger.error("Unable to fetch pySparkInterpreterContext for " + context.getNoteId(), e);
      return new InterpreterResult(Code.ERROR, "Unable to fetch pySparkInterpreterContext for " + context.getNoteId() + " " + e.getMessage());
    }

    if (!pySparkInterpreterContext.pythonscriptRunning) {
      return new InterpreterResult(Code.ERROR, "python process not running"
        + pySparkInterpreterContext.outputStream.toString());
    }

    pySparkInterpreterContext.outputStream.reset();

    synchronized (pySparkInterpreterContext.pythonScriptInitialized) {
      long startTime = System.currentTimeMillis();
      while (pySparkInterpreterContext.pythonScriptInitialized == false
        && pySparkInterpreterContext.pythonscriptRunning
        && System.currentTimeMillis() - startTime < 10 * 1000) {
        try {
          pySparkInterpreterContext.pythonScriptInitialized.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    if (pySparkInterpreterContext.pythonscriptRunning == false) {
      // python script failed to initialize and terminated
      return new InterpreterResult(Code.ERROR, "failed to start pyspark"
        + pySparkInterpreterContext.outputStream.toString());
    }
    if (pySparkInterpreterContext.pythonScriptInitialized == false) {
      // timeout. didn't get initialized message
      return new InterpreterResult(Code.ERROR, "pyspark is not responding "
        + pySparkInterpreterContext.outputStream.toString());
    }


    if (!sparkInterpreter.getSparkVersion().isPysparkSupported()) {
      return new InterpreterResult(Code.ERROR, "pyspark "
        + sparkInterpreter.getSparkContext().version() + " is not supported");
    }
    String jobGroup = sparkInterpreter.getJobGroup(context);
    ZeppelinContext z = sparkInterpreter.getZeppelinContext(context.getNoteId());
    z.setInterpreterContext(context);
    z.setGui(context.getGui());
    PythonInterpretRequest pythonInterpretRequest = new PythonInterpretRequest(st, jobGroup, context.getNoteId(), context.getParagraphId());
    pySparkInterpreterContext.pythonInterpretRequest = pythonInterpretRequest;
    //statementOutput = null;
    synchronized (pythonInterpretRequestMap) {
      if (pythonInterpretRequestMap.containsKey(context.getNoteId())) {
        throw new RuntimeException("Recieved two interpret requests for noteID :" + context.getNoteId()
          + " Previous para details are : " + pythonInterpretRequestMap.get(context.getNoteId()));
      }
      pythonInterpretRequestMap.put(context.getNoteId(), pythonInterpretRequest);
    }

    synchronized (pySparkInterpreterContext.pythonInterpretRequest.completed) {
      while(pySparkInterpreterContext.pythonInterpretRequest.statementOutput == null) {
        try {
          pySparkInterpreterContext.pythonInterpretRequest.completed.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    if (pySparkInterpreterContext.pythonInterpretRequest.statementError) {
      return new InterpreterResult(Code.ERROR, pySparkInterpreterContext.pythonInterpretRequest.statementOutput);
    } else {
      return new InterpreterResult(Code.SUCCESS, pySparkInterpreterContext.pythonInterpretRequest.statementOutput);
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    sparkInterpreter.cancel(context);
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    return sparkInterpreter.getProgress(context);
  }

  @Override
  public List<String> completion(String buf, int cursor, InterpreterContext context) {
    // not supported
    return new LinkedList<String>();
  }

  private SparkInterpreter getSparkInterpreter() {
    InterpreterGroup intpGroup = getInterpreterGroup();
    synchronized (intpGroup) {
      for (Interpreter intp : getInterpreterGroup()){
        if (intp.getClassName().equals(SparkInterpreter.class.getName())) {
          Interpreter p = intp;
          while (p instanceof WrappedInterpreter) {
            if (p instanceof LazyOpenInterpreter) {
              ((LazyOpenInterpreter) p).open();
            }
            p = ((WrappedInterpreter) p).getInnerInterpreter();
          }
          return (SparkInterpreter) p;
        }
      }
    }
    return null;
  }

  public ZeppelinContext getZeppelinContext(String key) {
    SparkInterpreter sparkIntp = getSparkInterpreter();
    if (sparkIntp != null) {
      return getSparkInterpreter().getZeppelinContext(key);
    } else {
      return null;
    }
  }

  public JavaSparkContext getJavaSparkContext() {
    SparkInterpreter intp = getSparkInterpreter();
    if (intp == null) {
      return null;
    } else {
      return new JavaSparkContext(intp.getSparkContext());
    }
  }

  public SparkConf getSparkConf() {
    JavaSparkContext sc = getJavaSparkContext();
    if (sc == null) {
      return null;
    } else {
      return getJavaSparkContext().getConf();
    }
  }

  public SQLContext getSQLContext() {
    SparkInterpreter intp = getSparkInterpreter();
    if (intp == null) {
      return null;
    } else {
      return intp.getSQLContext();
    }
  }

  private DepInterpreter getDepInterpreter() {
    InterpreterGroup intpGroup = getInterpreterGroup();
    if (intpGroup == null) return null;
    synchronized (intpGroup) {
      for (Interpreter intp : intpGroup) {
        if (intp.getClassName().equals(DepInterpreter.class.getName())) {
          Interpreter p = intp;
          while (p instanceof WrappedInterpreter) {
            p = ((WrappedInterpreter) p).getInnerInterpreter();
          }
          return (DepInterpreter) p;
        }
      }
    }
    return null;
  }


}
