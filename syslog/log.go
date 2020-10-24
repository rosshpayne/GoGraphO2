package syslog

import (
	"fmt"
	"log"
	//      "math"
	"os"
	"strconv"
)

const (
	logrFlags = log.LstdFlags | log.Lshortfile
)

const (
	Force = true
)

type MyLogger struct {
	on   bool
	logr *log.Logger
}

func (l *MyLogger) On() {
	l.on = true
}

func init() {
	logf := openLogFile()
	logr := log.New(logf, "DB:", logrFlags)
	SetLogger(logr)
	Logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	On()
}

func openLogFile() *os.File {
	logf, err := os.OpenFile("/home/ec2-user/environment/project/DynamoGraph/logs/DyG.sys.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}
	return logf
}

func (l *MyLogger) Off() {
	l.on = false
}

func (l *MyLogger) Log(s string, force ...bool) {

	if len(force) > 0 && force[0] {
		l.logr.Print(s)
		return
	}
	if !l.on {
		return
	}

	l.logr.Print(s)
}

// create a private logger = typically used within a routine
func New(prefix string, f string, i int) *MyLogger {
	f += "_" + strconv.Itoa(i) + ".log"
	f = "/home/ec2-user/environment/project/DynamoGraph/logs/" + f
	logf, err := os.OpenFile(f, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}
	logr := log.New(logf, prefix, logrFlags)
	logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	mylogr := MyLogger{on: true, logr: logr}
	return &mylogr
}

// global logger - accessible from any routine
var Logr *log.Logger

func SetLogger(logr *log.Logger) { // TODO: does this need to be exposed?
	if Logr == nil && logr != nil {
		Logr = logr
		Logr.Println("====================== SetLogger ===============================================")
	}
	// TODO: error here (logr is nil)
}

//var logit int
var loggingOn bool

func Off() {
	loggingOn = false
}
func On() {
	loggingOn = true
}

func Log(prefix string, s string, panic ...bool) {

	if !loggingOn {
		return
	}

	//      if math.Mod(float64(logit), 10) == 0 {
	Logr.SetPrefix(prefix)
	if len(panic) != 0 && panic[0] {
		Logr.Panic(s)
		return
	}
	Logr.Print(s)
	//      }

}

func Logf(prefix string, format string, v ...interface{}) {

	Logr.SetPrefix(prefix)
	Logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	fmt.Println(format)
	switch len(v) {
	case 1:
		Logr.Printf(format, v[0])
	case 2:
		Logr.Printf(format, v[0], v[1])
	case 3:
		Logr.Printf(format, v[0], v[1], v[2])
	}

}
