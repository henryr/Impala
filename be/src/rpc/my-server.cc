/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <rpc/my-server.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/concurrency/PlatformThreadFactory.h>

#include <string>
#include <iostream>

#include "util/time.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "util/thread-pool.h"

namespace apache { namespace thrift { namespace server {

using boost::shared_ptr;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;
using namespace impala;

class MyServer::Task: public Runnable {

public:

  Task(MyServer& server,
       shared_ptr<TProcessor> processor,
       shared_ptr<TProtocol> input,
       shared_ptr<TProtocol> output,
       shared_ptr<TTransport> transport) :
    server_(server),
    processor_(processor),
    input_(input),
    output_(output),
    transport_(transport) {
  }

  ~Task() {}

  void run() {
    boost::shared_ptr<TServerEventHandler> eventHandler =
      server_.getEventHandler();
    void* connectionContext = NULL;
    if (eventHandler != NULL) {
      connectionContext = eventHandler->createContext(input_, output_);
    }
    try {
      for (;;) {
        if (eventHandler != NULL) {
          eventHandler->processContext(connectionContext, transport_);
        }
        if (!processor_->process(input_, output_, connectionContext) ||
            !input_->getTransport()->peek()) {
          break;
        }
      }
    } catch (const TTransportException& ttx) {
      if (ttx.getType() != TTransportException::END_OF_FILE) {
        string errStr = string("MyServer client died: ") + ttx.what();
        GlobalOutput(errStr.c_str());
      }
    } catch (const std::exception &x) {
      GlobalOutput.printf("MyServer exception: %s: %s",
                          typeid(x).name(), x.what());
    } catch (...) {
      GlobalOutput("MyServer uncaught exception.");
    }
    if (eventHandler != NULL) {
      eventHandler->deleteContext(connectionContext, input_, output_);
    }

    try {
      input_->getTransport()->close();
    } catch (TTransportException& ttx) {
      string errStr = string("MyServer input close failed: ") + ttx.what();
      GlobalOutput(errStr.c_str());
    }
    try {
      output_->getTransport()->close();
    } catch (TTransportException& ttx) {
      string errStr = string("MyServer output close failed: ") + ttx.what();
      GlobalOutput(errStr.c_str());
    }

    // Remove this task from parent bookkeeping
    {
      Synchronized s(server_.tasksMonitor_);
      server_.tasks_.erase(this);
      if (server_.tasks_.empty()) {
        server_.tasksMonitor_.notify();
      }
    }

  }

 private:
  MyServer& server_;
  friend class MyServer;

  shared_ptr<TProcessor> processor_;
  shared_ptr<TProtocol> input_;
  shared_ptr<TProtocol> output_;
  shared_ptr<TTransport> transport_;
};

void MyServer::init() {
  stop_ = false;

  if (!threadFactory_) {
    threadFactory_.reset(new PlatformThreadFactory);
  }

  for (int i = 0; i < 16; ++i) {
    locks_.push_back(make_unique<mutex>());
    thread_tasks_.push_back(set<Task*>());
  }
}

MyServer::~MyServer() {}

void MyServer::do_accept(boost::shared_ptr<TTransport> client) {
  {
    lock_guard<mutex> l(big_lock_);
    shared_ptr<TTransport> inputTransport = inputTransportFactory_->getTransport(client);
    shared_ptr<TTransport> outputTransport = outputTransportFactory_->getTransport(client);
    shared_ptr<TProtocol> inputProtocol = inputProtocolFactory_->getProtocol(inputTransport);
    shared_ptr<TProtocol> outputProtocol = outputProtocolFactory_->getProtocol(outputTransport);

    shared_ptr<TProcessor> processor = getProcessor(inputProtocol,
        outputProtocol, client);

    MyServer::Task* task = new MyServer::Task(*this,
        processor,
        inputProtocol,
        outputProtocol,
        client);

    // Create a task
    shared_ptr<Runnable> runnable =
        shared_ptr<Runnable>(task);

    // Create a thread for this task
    shared_ptr<Thread> thread =
        shared_ptr<Thread>(threadFactory_->newThread(runnable));

    // Insert thread into the set of threads
    {
      //   Synchronized s(tasksMonitor_);
      tasks_.insert(task);
    }

    // Start the thread!
    thread->start();

  }

}

void MyServer::serve() {

  shared_ptr<TTransport> client;
  shared_ptr<TTransport> inputTransport;
  shared_ptr<TTransport> outputTransport;
  shared_ptr<TProtocol> inputProtocol;
  shared_ptr<TProtocol> outputProtocol;

  // Start the server listening
  serverTransport_->listen();

  // Run the preServe event
  if (eventHandler_ != NULL) {
    eventHandler_->preServe();
  }

  while (!stop_) {
    ThreadPool<shared_ptr<TTransport>> pool("server-accept", "test", 4, 10000,
        [this](int tid, const shared_ptr<TTransport>& item) {
          this->do_accept(item);
        }
                                            );
    try {
      // client.reset();
      // inputTransport.reset();
      // outputTransport.reset();
      // inputProtocol.reset();
      // outputProtocol.reset();

      // Fetch client from server
      shared_ptr<TTransport> client = serverTransport_->accept();

      pool.Offer(client);

      // Make IO transports
      // inputTransport = inputTransportFactory_->getTransport(client);
      // outputTransport = outputTransportFactory_->getTransport(client);
      // inputProtocol = inputProtocolFactory_->getProtocol(inputTransport);
      // outputProtocol = outputProtocolFactory_->getProtocol(outputTransport);


      // ::impala::SleepForMs(50000);
    } catch (TTransportException& ttx) {
      // if (inputTransport != NULL) { inputTransport->close(); }
      // if (outputTransport != NULL) { outputTransport->close(); }
      // if (client != NULL) { client->close(); }
      // if (!stop_ || ttx.getType() != TTransportException::INTERRUPTED) {
      //   string errStr = string("MyServer: TServerTransport died on accept: ") + ttx.what();
      //   GlobalOutput(errStr.c_str());
      // }
      continue;
    } catch (TException& tx) {
      // if (inputTransport != NULL) { inputTransport->close(); }
      // if (outputTransport != NULL) { outputTransport->close(); }
      // if (client != NULL) { client->close(); }
      // string errStr = string("MyServer: Caught TException: ") + tx.what();
      // GlobalOutput(errStr.c_str());
      continue;
    } catch (string s) {
      // if (inputTransport != NULL) { inputTransport->close(); }
      // if (outputTransport != NULL) { outputTransport->close(); }
      // if (client != NULL) { client->close(); }
      // string errStr = "MyServer: Unknown exception: " + s;
      // GlobalOutput(errStr.c_str());
      break;
    }
  }

  // If stopped manually, make sure to close server transport
  if (stop_) {
    try {
      serverTransport_->close();
    } catch (TException &tx) {
      string errStr = string("MyServer: Exception shutting down: ") + tx.what();
      GlobalOutput(errStr.c_str());
    }
    try {
      Synchronized s(tasksMonitor_);
      while (!tasks_.empty()) {
        tasksMonitor_.wait();
      }
    } catch (TException &tx) {
      string errStr = string("MyServer: Exception joining workers: ") + tx.what();
      GlobalOutput(errStr.c_str());
    }
    stop_ = false;
  }

}

}}} // apache::thrift::server
