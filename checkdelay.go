package main

import (
    "fmt"
    //"net"
    //"net/url"
    //"net/http"
    //"io/ioutil"
    "strings"
    "log"
    "os"
    "os/exec"
    "path/filepath"
    "time"
    //"io"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "github.com/larspensjo/config"
    "flag"
    "github.com/robfig/cron"
)

var TOPIC = make(map[string]string)
var MASTER = make(map[string]string)

func GetCurrPath() string {
    
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	return path

}

func substr(s string, pos, length int) string {  

	runes := []rune(s)  
	l := pos + length  
	if l > len(runes) {  
        	l = len(runes)  
	}  
    
	return string(runes[pos:l])  

}

func get_config() {

        configpath := substr(GetCurrPath(),0,strings.LastIndex(GetCurrPath(),"/")) + "/config.ini"
        //fmt.Println(configpath)
        var configFile = flag.String("configfile", configpath, "General configuration file")

        cfg, err := config.ReadDefault(*configFile)
        if err != nil {
                log.Fatalf("Fail to find",*configFile, err)
        }

        if cfg.HasSection("mysql") {
                section, err := cfg.SectionOptions("mysql")
                if err == nil {
                        for _, v := range section {
                                options, err := cfg.String("mysql",v)
                                if err == nil {
                                        TOPIC[v] = options
                                }
                        }
                }
        }

        if len(TOPIC["ip"])==0 || len(TOPIC["port"])==0 || len(TOPIC["dbname"])==0 || len(TOPIC["user"])==0 || len(TOPIC["passwd"])==0 || len(TOPIC["cron"])==0 {
                fmt.Println("config.ini Error...")
                os.Exit(1)
        }
}

func main() {

	get_config()
    	get_master()

        if TOPIC["cron"] == "1" {
	
		go master_cron()

	}

	//time.Sleep(1000 * time.Second)

	spec_diff := "*/1 * * * * ?"
        diff := cron.New()
        diff.AddFunc(spec_diff, diff_timestamp)
        diff.Start()
        select {}
}

func master_cron() {

	spec := "*/1 * * * * ?"
        c := cron.New()
        c.AddFunc(spec, update_timestamp)
        c.Start()
        select {}

}


func get_master() {
	
 	mysqlstring := TOPIC["user"] + ":" + TOPIC["passwd"] + "@tcp(" + TOPIC["ip"] + ":" + TOPIC["port"] + ")/" + TOPIC["dbname"] + "?charset=utf8mb4,utf8"
      	db, err := sql.Open("mysql",mysqlstring)
        if err != nil {
                panic(err.Error())
        }
        defer db.Close()

        stmtOut, err := db.Prepare("select Host,Port from mysql.slave_master_info where 1=?")
        if err != nil {
                panic(err.Error())
        }
        defer stmtOut.Close()

        var master_host string 
        var master_port string
        Try(func() {
		
		err = stmtOut.QueryRow(1).Scan(&master_host,&master_port)
        	if err != nil {
                	panic(err.Error())
       		}
		MASTER["ip"] = master_host
		MASTER["port"] = master_port
		//fmt.Println("MASTER_HOST:",MASTER["ip"],"MASTER_PORT:",MASTER["port"])
	}, func(e interface{}) {
                fmt.Println("mysql replication role may be not slave...")
                os.Exit(1)
        })

}

func update_timestamp() {

        mysqlstring := TOPIC["user"] + ":" + TOPIC["passwd"] + "@tcp(" + MASTER["ip"] + ":" + MASTER["port"] + ")/" + TOPIC["dbname"] + "?charset=utf8mb4,utf8"
	db, err := sql.Open("mysql",mysqlstring)
        if err != nil {
                panic(err.Error())
        }
        defer db.Close()

        stmtIns, err := db.Prepare("replace into chkslavedelay(id,ts) values(?,now());")
        if err != nil {
                panic(err.Error())
        }
        defer stmtIns.Close()

        _, err = stmtIns.Exec("1")
        if err != nil {
                panic(err.Error())
        }
	//log.Println("cron running...")

}

func diff_timestamp() {

	mysqlstring_slave := TOPIC["user"] + ":" + TOPIC["passwd"] + "@tcp(" + TOPIC["ip"] + ":" + TOPIC["port"] + ")/" + TOPIC["dbname"] + "?charset=utf8mb4,utf8"
        db_slave, err := sql.Open("mysql",mysqlstring_slave)
        if err != nil {
                panic(err.Error())
        }
        defer db_slave.Close()

        stmtOut, err := db_slave.Prepare("select ts from chkslavedelay where id=?")
        if err != nil {
                panic(err.Error())
        }
        defer stmtOut.Close()

	var slave_ts string
	err = stmtOut.QueryRow(1).Scan(&slave_ts)
        if err != nil {
        	panic(err.Error())
        }
	//fmt.Println("slave_ts:",slave_ts)
	//log.Println("diff...")

	mysqlstring_master := TOPIC["user"] + ":" + TOPIC["passwd"] + "@tcp(" + MASTER["ip"] + ":" + MASTER["port"] + ")/" + TOPIC["dbname"] + "?charset=utf8mb4,utf8"
        db_master, err := sql.Open("mysql",mysqlstring_master)
        if err != nil {
                panic(err.Error())
        }
        defer db_master.Close()

        stmtOut_master, err := db_master.Prepare("select ts from chkslavedelay where id=?")
        if err != nil {
                panic(err.Error())
        }
        defer stmtOut_master.Close()

        var master_ts string
        err = stmtOut_master.QueryRow(1).Scan(&master_ts)
        if err != nil {
                panic(err.Error())
        }
        //fmt.Println("master_ts:",master_ts)
	
	
	//time format
	t_master, _ := time.Parse("2006-01-02 15:04:05", master_ts)
	t_slave, _ := time.Parse("2006-01-02 15:04:05", slave_ts)
	diff := t_master.Sub(t_slave)
	fmt.Println("Seconds_Behind_Master:",diff.Seconds())

}

func Try(fun func(), handler func(interface{})) {
        defer func() {
                if err := recover(); err != nil {
                        handler(err)
                }
        }()
        fun()
}
