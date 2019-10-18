package main

import (
  "io"
  "context"
	"encoding/json" // for use of jason
	"fmt"  //for io in go
	"log"
	"net/http"//Package http provides HTTP client and server implementations.
	"strconv"
  "reflect"
  "gopkg.in/olivere/elastic.v6" //ElasticSearch
  "github.com/pborman/uuid" //Universally Unique Identifier
  "cloud.google.com/go/storage" // Google Cloud Storage
  "cloud.google.com/go/bigtable" //bigtable
  "github.com/gorilla/mux"
  jwtmiddleware "github.com/auth0/go-jwt-middleware"
  jwt "github.com/dgrijalva/jwt-go"

)

const (
  POST_INDEX = "post"
  POST_TYPE = "post"

	DISTANCE = "200km"
  ES_URL = "http://35.202.115.110:9200"
  BUCKET_NAME = "khuang-post-image"
  BIGTABLE_PROJECT_ID = "orbital-airfoil-237121"
  BT_INSTANCE = "around-post"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
  Url    string `json:"url"`   //the url to visit photo posted
}

func main() {
	fmt.Println("started-service")
  createIndexIfNotExist()

  jwtMiddleware := jwtmiddleware.New(jwtmiddleware.Options{
      ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
     return []byte(mySigningKey), nil
      },
      SigningMethod: jwt.SigningMethodHS256,
 })


    r := mux.NewRouter()

    r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST", "OPTIONS")  //protect the post
    r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET", "OPTIONS") //protect the search
    r.Handle("/signup", http.HandlerFunc(handlerSignup)).Methods("POST", "OPTIONS")
    r.Handle("/login", http.HandlerFunc(handlerLogin)).Methods("POST", "OPTIONS")

    http.Handle("/", r)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func createIndexIfNotExist() {
  client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
    if err != nil {
        panic(err)
    }
    // if the index does not exist, create one
    exists, err := client.IndexExists(POST_INDEX).Do(context.Background())
    if err != nil {
        panic(err)
    }

    if !exists {
      //determines how the document will be indexed
      //initialize the number of shards as 1 and number of replicas as 0
      //mappings means how multiple fileds are indexed and stored
      //post is the type in es
      //location(field) is of data type "geo-point" which accepts latitude-longitude pairs
        mapping := `{
          "settings":{
            "number_of_shards": 1,
            "number_of_replicas": 0
          },
            "mappings": {
                "post": {
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                }
            }
        }`
        _, err = client.CreateIndex(POST_INDEX).Body(mapping).Do(context.Background())
        if err != nil {
            panic(err)
        }
    }

    exists, err = client.IndexExists(USER_INDEX).Do(context.Background())
      if err != nil {
          panic(err)
      }

      if !exists {
          _, err = client.CreateIndex(USER_INDEX).Do(context.Background())
          if err != nil {
              panic(err)
          }
      }
}

// Save a post to ElasticSearch
func saveToES(post *Post, id string) error {
    client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
    if err != nil {
        return err
    }

    _, err = client.Index().
        Index(POST_INDEX).
        Type(POST_TYPE).
        Id(id).
        BodyJson(post).
        Refresh("wait_for").
        Do(context.Background())
    if err != nil {
        return err
    }

    fmt.Printf("Post is saved to index: %s\n", post.Message)
    return nil
}


// Save a post to BigTable
func saveToBigTable(p *Post, id string) error {
	ctx := context.Background()
	bt_client, err := bigtable.NewClient(ctx, BIGTABLE_PROJECT_ID, BT_INSTANCE)
	if err != nil {
		return nil
	}

	tbl := bt_client.Open("post") //create a client connected to "post"
	mut := bigtable.NewMutation() //standard way to access a row in bigtable, one mutation -> one row
	t := bigtable.Now() //timestamp
	mut.Set("post", "user", t, []byte(p.User)) //column family name, column name, timestamp, convert the user field in post to byte array
	mut.Set("post", "message", t, []byte(p.Message))
	mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

	err = tbl.Apply(ctx, id, mut) //write to bigtable (id is the row key)
	if err != nil {
		return err
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
  return nil
}





func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Access-Control-Allow-Origin", "*")
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

  if r.Method == "OPTIONS" {
      return
  }

	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64) //change string to float, bitSize is 64(accuracy, 32 means less accurate)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	// range is optional
	ran := DISTANCE //const
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Println("range is ", ran)

  posts, err := readFromES(lat, lon, ran)
   if err != nil {
       http.Error(w, "Failed to read post from ElasticSearch", http.StatusInternalServerError)
       fmt.Printf("Failed to read post from ElasticSearch %v.\n", err)
       return
   }

   js, err := json.Marshal(posts)
   if err != nil {
       http.Error(w, "Failed to parse posts into JSON format", http.StatusInternalServerError)
       fmt.Printf("Failed to parse posts into JSON format %v.\n", err)
       return
   }

   w.Write(js)
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
  // Parse from body of request to get a json object.
  fmt.Println("Received one post request")

  w.Header().Set("Content-Type", "application/json")
  w.Header().Set("Access-Control-Allow-Origin", "*")
  w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")


  if r.Method == "OPTIONS" {
      return
  }


  user := r.Context().Value("user")
  claims := user.(*jwt.Token).Claims
  username := claims.(jwt.MapClaims)["username"] //claims stans for PAYLOAD in JWT Token, "username" and "exp" in our project



  lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
  lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

  p := &Post{
      User:    username.(string),
      Message: r.FormValue("message"),
      Location: Location{
          Lat: lat,
          Lon: lon,
      },
  }

  id := uuid.New()
  //parse multiform request
  file, _, err := r.FormFile("image")
  if err != nil {
      http.Error(w, "Image is not available", http.StatusBadRequest)
      fmt.Printf("Image is not available %v.\n", err)
      return
  }
  //save image to GCS
  attrs, err := saveToGCS(file, BUCKET_NAME, id)
  if err != nil {
      http.Error(w, "Failed to save image to GCS", http.StatusInternalServerError)
      fmt.Printf("Failed to save image to GCS %v.\n", err)
      return
  }
  p.Url = attrs.MediaLink

  err = saveToES(p, id)
  if err != nil {
      http.Error(w, "Failed to save post to ElasticSearch", http.StatusInternalServerError)
      fmt.Printf("Failed to save post to ElasticSearch %v.\n", err)
      return
  }
  fmt.Printf("Saved one post to ElasticSearch: %s", p.Message)
  err = saveToBigTable(p, id)
  if err != nil {
          http.Error(w, "Failed to save post to BigTable", http.StatusInternalServerError)
          fmt.Printf("Failed to save post to BigTable %v.\n", err)
          return
  }

}

//latitude, longtitude, range
func readFromES(lat, lon float64, ran string) ([]Post, error) {
    //It means we create a connection to ES. If there is err, return
    client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
    if err != nil {
        return nil, err
    }
    //Prepare a geo based query to find posts within a geo box
    query := elastic.NewGeoDistanceQuery("location")
    query = query.Distance(ran).Lat(lat).Lon(lon)


    //Get the results based on Index and query (query that we just prepared). Pretty means to format the output.
    searchResult, err := client.Search().
        Index(POST_INDEX).
        Query(query).
        Pretty(true).
        Do(context.Background())
    if err != nil {
        return nil, err
    }

    // searchResult is of type SearchResult and returns hits, suggestions,
    // and all kinds of other information from Elasticsearch.
    fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

    // Each is a convenience function that iterates over hits in a search result.
    // It makes sure you don't need to check for nil values in the response.
    // However, it ignores errors in serialization. If you want full control
    // over iterating the hits, see below.
    var ptyp Post
    var posts []Post
    for _, item := range searchResult.Each(reflect.TypeOf(ptyp)) {
        if p, ok := item.(Post); ok {
            posts = append(posts, p) //read multiple posts from es
        }
    }

    return posts, nil
}

func saveToGCS(r io.Reader, bucketName, objectName string) (*storage.ObjectAttrs, error) {
  //Go Concurrency Patterns: Context
  //context package makes it easy to pass request-scoped values, cancelation signals,
  //and deadlines across API boundaries to all the goroutines involved in handling a request
    ctx := context.Background()

    // Creates a client.
    client, err := storage.NewClient(ctx)
    if err != nil {
        return nil, err
    }

    bucket := client.Bucket(bucketName)
    if _, err := bucket.Attrs(ctx); err != nil {
        return nil, err
    }

    object := bucket.Object(objectName)
    wc := object.NewWriter(ctx)
    if _, err = io.Copy(wc, r); err != nil {
        return nil, err
    }
    if err := wc.Close(); err != nil {
        return nil, err
    }

    if err = object.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {  //ACL stands for access control -> who can access the object?
        return nil, err
    }


    attrs, err := object.Attrs(ctx)
    if err != nil {
        return nil, err
    }

    fmt.Printf("Image is saved to GCS: %s\n", attrs.MediaLink)
    return attrs, nil //return the attribute(URL)
}
