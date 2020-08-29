package syslog

import (
	"fmt"
	"log"
	//	"math"
	"os"
)

const (
	logrFlags = log.LstdFlags | log.Lshortfile
)

var Logr *log.Logger

func SetLogger(logr *log.Logger) {
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

func init() {
	logf := openLogFile()
	logr := log.New(logf, "DB:", logrFlags)
	SetLogger(logr)
	Logr.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	On()
}

func openLogFile() *os.File {
	logf, err := os.OpenFile("DyG.sys.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal(err)
	}
	return logf
}

func Log(prefix string, s string, panic ...bool) {

	if !loggingOn {
		return
	}

	//	if math.Mod(float64(logit), 10) == 0 {
	Logr.SetPrefix(prefix)
	if len(panic) != 0 && panic[0] {
		Logr.Panic(s)
		return
	}
	Logr.Print(s)
	//	}

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
