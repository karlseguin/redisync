package redisync

import (
  "flag"
  "time"
)

var configFile = flag.String("config", "config.json", "the configuration file")

func Run() {
  flag.Parse()
  config := loadConfig(*configFile)
  var current *Worker
  for {
    ready := make(chan bool, 1)
    newWorker := New(config)
    go newWorker.Start(ready)
    <- ready
    if current != nil {
      current.Stop()
    }
    current = newWorker
    time.Sleep(time.Second * config.TTL)
  }
}
