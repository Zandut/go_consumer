package main

import (
  "database/sql"
  "bytes"
  "log"
  "sync"
  "strings"
  "fmt"
  "time"
  "strconv"
  "github.com/nsqio/go-nsq"
  "github.com/tidwall/gjson"
  _ "github.com/lib/pq"
)


func main() {

  wg := &sync.WaitGroup{}
  wg.Add(5000);
  config := nsq.NewConfig()
  config.Set("max_in_flight", 2000)
  q, _ := nsq.NewConsumer("prod-stream", "prod_cons_gpslog", config)
  q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {

      jsonData := string(message.Body)
    //  _from := gjson.Get(json, "_from")
     // _value := gjson.Get(jsonData, "_value")
     // timeF := gjson.Get(_value.String(), "time")
     // timeFF := strings.Split(timeF.String(), "T")
     // time := timeFF[0] + " " + timeFF[1] 
     //_payload := gjson.Get(_value.String(), "payload")
     // println(_value.String())
     db, err1 := sql.Open("postgres",
         "postgresql://root@192.168.9.91:26257/runner_zzlog?sslmode=disable")
     if err1 != nil {
         log.Fatal("error connecting to the database: ", err1)
         //fmt.Println("Kesalahan Koneksi : ",err)
     }

     timeF := gjson.Get(jsonData, "_value.time")
     //timeFF := strings.Split(timeF.String(), "T")
     timeFF := strings.Split(timeF.String(), "+")
     timeFF2 := strings.Split(timeF.String(), "T")
     //tsF := timeFF[0] + " " + timeFF[1] 
     tsF := timeFF[0] + "Z"
     tsF2 := timeFF2[0] + " " + timeFF2[1]
     
     tsFF, err := time.Parse(time.RFC3339, tsF)
     if err != nil {
        fmt.Println(err)
     }
     ts := tsFF.Unix()

     fmt.Println("cek : ",ts)
     fmt.Println("cek : ",tsF2)

     alt := gjson.Get(jsonData, "_value.altitude")
     location := gjson.Get(jsonData, "_value.location")

     sat := gjson.Get(jsonData, "_value.satellite")
     head := gjson.Get(jsonData, "_value.angle")
     imei := gjson.Get(jsonData, "_value.imei")
     speed := gjson.Get(jsonData, "_value.speed")
     eventID := gjson.Get(jsonData, "_value.event_id")
     payload := gjson.Get(jsonData, "_value.payload")
     coordinates := gjson.Get(location.String(), "Coordinates")
     dataCoordinates := coordinates.Array()
     // fmt.Printf("%s\n", tsF)
     // fmt.Printf("%s\n", alt)
     // fmt.Printf("%s,%s\n", dataCoordinates[0].String(), dataCoordinates[1].String())
     // fmt.Printf("%v\n", coordinates.String())
     
    var b bytes.Buffer
    b.WriteString(`{`)

    b.WriteString(`"ts":`)
    b.WriteString(strconv.FormatInt(ts, 10))

    b.WriteString(`,"alt":`)
    b.WriteString(alt.String())

    b.WriteString(`,"lat":`)
    b.WriteString(dataCoordinates[1].String())

    b.WriteString(`,"lon":`)
    b.WriteString(dataCoordinates[0].String())

    b.WriteString(`,"sat":`)
    b.WriteString(sat.String())

    b.WriteString(`,"head":`)
    b.WriteString(head.String())

    b.WriteString(`,"imei":`)
    b.WriteString(imei.String())

    b.WriteString(`,"speed":`)
    b.WriteString(speed.String())

    b.WriteString(`,"eventID":`)
    b.WriteString(eventID.String())

    b.WriteString(`,"payload":`)
    b.WriteString(payload.String())

    //---------------------------------------

    // b.WriteString(`"imei": `)
    // b.WriteString(`"` + imei.String() + `"`)

    // b.WriteString(`, "time": `)
    // b.WriteString(`"` + tsF + `"`)

    //tutup
    b.WriteString(`}`)
   fmt.Println(b.String())
    
   	qr := "INSERT INTO gpslog (time,imei,data) VALUES ('" + tsF2 + "','" + imei.String() + "','" + b.String() + "')"
    if _, err1 := db.Exec(qr); err1 != nil {
       fmt.Println("Kesalahan : ",err1)
   }

   	defer db.Close()

      return nil
  }))
  err := q.ConnectToNSQDs([]string{"real1.ktbfuso.id:4150","real2.ktbfuso.id:4150","real3.ktbfuso.id:4150"})
  if err != nil {
      log.Panic("Could not connect")
  }
  wg.Wait()

}
