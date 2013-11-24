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
  prefix string
  conn net.Conn
  buffer []byte
  config *Config
  stop chan bool
}

func New(config *Config) *Worker {
  return &Worker{
    config: config,
    stop: make(chan bool, 1),
    buffer: make([]byte, BUFFER_SIZE),
  }
}

func (w *Worker) Start(ready chan bool) {
  lastOk := ""
  for {
    w.prefix = time.Now().Format("20060102_150405")
    if err := w.connect(); err != nil {
      w.failure(err)
      continue
    }
    if err := w.dump(); err != nil {
      w.failure(err)
      w.cleanup(w.prefix)
      continue
    }
    ready <- true
    w.cleanup(lastOk)
    lastOk = w.prefix
    if err := w.aof(); err != nil {
      w.failure(err)
      continue
    }
    break
  }
  w.close()
}

func (w *Worker) Stop() {
  w.stop <- true
  if w.config.Cleanup {
    w.cleanup(w.prefix)
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

func (w *Worker) dump() error {
  w.read = 0
  w.conn.Write([]byte("*1\r\n$4\r\nSYNC\r\n"))
  length, err := w.readDumpInfo()
  if err != nil { return err }
  file, err := os.Create(path.Join(w.config.Storage, w.prefix + ".rdb"))
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
    if n == 1 && w.buffer[0] == '\n' { continue } //what is this?

    if w.buffer[0] != '$' {
      return 0, errors.New("expecting synchornization dump to start with a bulk reply")
    }
    w.read += n
    for i := 0; i < n; i++ {
      if w.buffer[i] == '\r' && w.buffer[i+1] == '\n' {
        length, err := strconv.Atoi(string(w.buffer[1:i]))
        if err != nil { return 0, err }
        w.position = i + 2
        return length, nil
      }
    }
    if w.read > 128 {
      return 0, errors.New("Expecting dump length to be in the first 128 bytes")
    }
  }
}

func (w *Worker) aof() error {
  file, err := os.Create(path.Join(w.config.Storage, w.prefix + ".aof"))
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

