package redisync

import (
  "os"
  "log"
  "net"
  "path"
  "time"
  "errors"
  "strconv"
)

const BUFFER_SIZE = 32768

type Worker struct {
  read int
  position int
  conn net.Conn
  buffer []byte
  config *Config
  stop chan bool
  stopped chan bool
}

func New(config *Config) *Worker {
  return &Worker{
    config: config,
    stop: make(chan bool, 1),
    stopped: make(chan bool, 1),
    buffer: make([]byte, BUFFER_SIZE),
  }
}

func (w *Worker) Run(ready chan bool) {
  lastPrefix := ""
  for {
    prefix := time.Now().Format("20060102_150405")
    if err := w.connect(); err != nil {
      w.failure(err)
      continue
    }
    if err := w.dump(prefix); err != nil {
      w.failure(err)
      w.cleanup(prefix)
      continue
    }
    ready <- true
    w.cleanup(lastPrefix)
    lastPrefix = prefix
    if err := w.aof(prefix); err != nil {
      w.failure(err)
      continue
    }
    break
  }
  w.close()
  os.Rename(path.Join(w.config.Storage, lastPrefix + ".rdb"), path.Join(w.config.Storage, "prev" + ".rdb"))
  os.Rename(path.Join(w.config.Storage, lastPrefix + ".aof"), path.Join(w.config.Storage, "prev" + ".aof"))
  w.stopped <- true
}

func (w *Worker) Stop() {
  w.stop <- true
  select {
  case <- w.stopped:
  case <- time.After(time.Minute):
    log.Println("worker failed to stop after 1 minute")
  }
}

func (w *Worker) cleanup(prefix string) {
  if len(prefix) == 0 { return }
  os.Remove(path.Join(w.config.Storage, prefix + ".rdb"))
  os.Remove(path.Join(w.config.Storage, prefix + ".aof"))
}

func (w *Worker) failure(err error) {
  log.Println(err)
  w.close()
  time.Sleep(time.Second * 5)
}

func (w *Worker) connect() error {
  conn, err := net.Dial(w.config.Network, w.config.Address)
  if err != nil { return err }
  w.conn = conn
  return nil
}

func (w *Worker) dump(prefix string) error {
  w.read = 0
  w.conn.Write([]byte("*1\r\n$4\r\nSYNC\r\n"))
  length, err := w.readDumpInfo()
  if err != nil { return err }
  file, err := os.Create(path.Join(w.config.Storage, prefix + ".rdb"))
  if err != nil { return err }
  defer file.Close()

  for {
    end := w.position + length
    if end > w.read {
      end = w.read
    }
    file.Write(w.buffer[w.position:end])
    length -= end - w.position
    if length == 0 {
      w.position = end
      break
    }
    w.read, err = w.conn.Read(w.buffer)
    if err != nil { return err }
    w.position = 0
  }

  return nil
}

func (w *Worker) readDumpInfo() (int, error) {
  for {
    n, err := w.conn.Read(w.buffer[w.read:])
    if err != nil { return 0, err }
    if n == 0 { continue }
    if n == 1 && w.buffer[0] == '\n' { continue } //common short circuit

    w.read += n
    i := 0
    for ; i < n; i++ {
      if w.buffer[i] == '$' {
        i++
        for j := i; j < n; j++ {
          if w.buffer[j+1] == '\r' && w.buffer[j+2] == '\n' {
            length, err := strconv.Atoi(string(w.buffer[i:j]))
            if err != nil { return 0, err }
            w.position = i + 2
            return length, nil
          }
        }
      }
    }
    if w.read > 128 {
      return 0, errors.New("Expecting dump length to be in the first 128 bytes")
    }
  }
}

func (w *Worker) aof(prefix string) error {
  file, err := os.Create(path.Join(w.config.Storage, prefix + ".aof"))
  if err != nil { return err }
  defer file.Close()
  for {
    select {
    case <-w.stop:
      return nil
    default:
      if w.position < w.read {
        file.Write(w.buffer[w.position:w.read])
      }
      n, err := w.conn.Read(w.buffer)
      if err != nil { return err }
      w.position = 0
      w.read = n
    }
  }
}

func (w *Worker) close() {
  if w.conn != nil { w.conn.Close() }
}

