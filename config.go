package redisync

import(
  "time"
  "io/ioutil"
  "encoding/json"
)

type Config struct {
  Cleanup bool
  Storage string
  Network string
  Address string
  TTL time.Duration
}


func loadConfig(path string) *Config {
  file, err := ioutil.ReadFile(path)
  if err != nil { panic(err) }

  config := new(Config)
  err = json.Unmarshal(file, config)
  if err != nil { panic(err) }
  return config
}
