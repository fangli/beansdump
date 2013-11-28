// Copyright 2013 Funplus Game. All rights reserved.
// This file is a part of big data system, it collects
// entries from beanstalkd and upload them to S3 in batch

// Author:  Fang Li <fang.li@funplusgame.com>
// Date:    06/26/2013
// Release: 1.0.0

package main

import (
	"io"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"math/rand"
	"bytes"
	"github.com/kr/beanstalk"
	"github.com/kr/s3/s3util"
	"log"
	"path"
	"time"
)

type BeansConfig struct {
	Host string
	Tube string
}

var concurrent_beans = 20
var concurrent_filesaving = 1
var concurrent_s3upload = 5

var q = make(chan []byte)
var f = make(chan string, 100)
var quitsig = make(chan bool)

var beanConfig BeansConfig

var beansHost = flag.String("s", "localhost:11300", "IP:Port point to beanstalkd server (default to localhost:11300)")
var beansTube = flag.String("t", "default", "Message tube for metrics (default to 'default')")
var interval = flag.Int("i", 300, "The interval to cache the jobs and send to S3 in bulk, default to 300(s)")
var s3key = flag.String("u", "", "The S3 accesskey, required")
var s3secret = flag.String("p", "", "The S3 secret, required")
var s3folder = flag.String("r", "https://mybucket.s3.amazonaws.com/test/", "Point to the folder URL of target S3 bucket")
var tmpdir = flag.String("f", "/mnt/", "The tmp dir to cache the S3 file, default to /mnt/")

func BeansHandler() (*beanstalk.Conn, *beanstalk.TubeSet, error) {
	client, err := beanstalk.Dial("tcp", beanConfig.Host)
	tube := beanstalk.NewTubeSet(client, beanConfig.Tube)
	if err != nil {
		return client, tube, err
	}
	return client, tube, nil
}

func beansProcessor() {
	var cli *beanstalk.TubeSet
	var conn *beanstalk.Conn
	var err error
	var i = true
	conn, cli, err = BeansHandler()
	if err != nil {
		log.Fatal("Unable to connect to beanstalkd server")
	}
	for {
		id, msg, err := cli.Reserve(time.Hour)
		if cerr, ok := err.(beanstalk.ConnError); ok && cerr.Err == beanstalk.ErrTimeout {
			continue
		} else if err == nil {
			q <- msg
			conn.Delete(id)
		} else {
			log.Println("Beanstalkd server has gone")
			i = true
			for i == true {
				log.Println("Reconnecting to beanstalkd server...")
				conn, cli, err = BeansHandler()
				if err != nil {
					time.Sleep(time.Second)
				} else {
					log.Println("Successful connected to beanstalkd server again")
					i = false
				}
			}
		}
	}
}

func randInt(min int , max int) int {  
		rand.Seed( time.Now().UTC().UnixNano())  
		return min + rand.Intn(max-min)  
}

func randomString (l int) string {  
	var result bytes.Buffer  
	var temp string  
	for i:=0 ; i<l ;  {  
		if string(randInt(97,122))!=temp {  
		temp = string(randInt(97,122))  
		result.WriteString(temp)  
		i++  
	  }  
	}  
	return result.String()  
} 

func save2File() {
	var ts = time.Now()
	var chunk_size int64 = 0
	var flag_new = true
	var file *os.File
	var err error
	for {
		if (int(time.Since(ts)/time.Second) >= *interval) && (chunk_size > 0) {
			fileName := file.Name()
			file.Close()
			f <- fileName
			flag_new = true
			ts = time.Now()
			log.Println("Rotating temp file " + fileName + ", entries " + strconv.FormatInt(chunk_size, 10))
			chunk_size = 0
		} else {
			if flag_new == true {
				file, err = os.Create(*tmpdir + "beansdump_" + strconv.FormatInt(time.Now().Unix(), 10) + "_" + randomString(8) + ".dat")
				defer file.Close()
				if err != nil {log.Fatal("Unable to create tmp file in " + *tmpdir)}
				flag_new = false
			}
			// Append or Timeout
			select {
			  case _q := <-q:
				file.Write(append(_q, '\n'))
				chunk_size += 1
			  case <-time.After(time.Second):
				continue
			}
		}
	}
}

func uploadS3(fileName string) (int64, error)  {
	s3path := *s3folder + path.Base(fileName)

	r, err := os.Open(fileName)
	defer r.Close()
	if err != nil {log.Fatal("Unable to open the temp file " + fileName)}

	w, err := s3util.Create(s3path, nil, nil)
	if err != nil {return 0, err}

	length, err := io.Copy(w, r)
	if err != nil {return 0, err}

	err = w.Close()
	if err != nil {return 0, err}

	r.Close()
	os.Remove(fileName)
	return length, nil
}

func backgroundUpload() {
	for {
		fileName := <-f
		log.Println("Background uploading " + fileName + " to S3...")
		uploadS3(fileName)
		log.Println("File uploaded: " + fileName)
	}
}

func formatArgs() {
	return
}

func run() {
	for i := 0; i < concurrent_beans; i++ {
		go beansProcessor()
	}
	for i := 0; i < concurrent_filesaving; i++ {
		go save2File()
	}
	for i := 0; i < concurrent_s3upload; i++ {
		go backgroundUpload()
	}

	for {time.Sleep(time.Second*10000)}

}

func gracefulShutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
	  for sig := range c {
		log.Printf("Captured %v, now saving cached data and exiting,", sig)
		log.Printf("This will takes a little while, please be patient...")
		// for i := 0; i < concurrent_beans + concurrent_filesaving + concurrent_s3upload; i++ {
		// 	quitsig <- true
		// }
		os.Exit(0)
	  }
	}()
}

func main() {
	log.Println("Starting daemon...")
	flag.Parse()
	formatArgs()
	beanConfig = BeansConfig{
		Host: *beansHost,
		Tube: *beansTube}
	s3util.DefaultConfig.AccessKey = *s3key
	s3util.DefaultConfig.SecretKey = *s3secret
	gracefulShutdown()
	run()
}
