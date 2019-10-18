package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "net/http"
    "reflect"
    "regexp"
    "time"

    jwt "github.com/dgrijalva/jwt-go"
    "gopkg.in/olivere/elastic.v6"
)

const (
    USER_INDEX = "user"   //index for es
    USER_TYPE  = "user"  //type for es
)

type User struct {
    Username string `json:"username"` //important
    Password string `json:"password"` //important
    Age      int64  `json:"age"` //optional
    Gender   string `json:"gender"` //optional
}

var mySigningKey = []byte("secret")

//for login verification purpose
func checkUser(username, password string) error {
    client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
    if err != nil {
        return err
    }
    //similar to SELECT * from user where username = ? in SQL syntax
    query := elastic.NewTermQuery("username", username)

    searchResult, err := client.Search().
        Index(USER_INDEX).
        Query(query).
        Pretty(true).
        Do(context.Background())
    if err != nil {
        return err
    }

    var utyp User
    for _, item := range searchResult.Each(reflect.TypeOf(utyp)) {
        if u, ok := item.(User); ok {
            if username == u.Username && password == u.Password {
                fmt.Printf("Login as %s\n", username)
                return nil
            }
        }
    }

    return errors.New("Wrong username or password")
}
//for registration purpose
func addUser(user User) error {
  client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
    if err != nil {
        return err
    }


    query := elastic.NewTermQuery("username", user.Username)

    searchResult, err := client.Search().
        Index(USER_INDEX).
        Query(query).
        Pretty(true).
        Do(context.Background())
    if err != nil {
        return err
    }

    if searchResult.TotalHits() > 0 { //check if username has already been used
        return errors.New("User already exists")
    }

    _, err = client.Index().
        Index(USER_INDEX).
        Type(USER_TYPE).
        Id(user.Username).  //does not need uuid because each username is unique
        BodyJson(user).
        Refresh("wait_for").
        Do(context.Background())
    if err != nil {
        return err
    }

    fmt.Printf("User is added: %s\n", user.Username)
    return nil
}

//handle login request, it will call checkUser
func handlerLogin(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Received one login request")
    w.Header().Set("Content-Type", "text/plain")
    w.Header().Set("Access-Control-Allow-Origin", "*")

    if r.Method == "OPTIONS" {
        return
    }

    decoder := json.NewDecoder(r.Body)
    var user User
    if err := decoder.Decode(&user); err != nil {
        http.Error(w, "Cannot decode user data from client", http.StatusBadRequest)
        fmt.Printf("Cannot decode user data from client %v.\n", err)
        return
    }

    if err := checkUser(user.Username, user.Password); err != nil {
        if err.Error() == "Wrong username or password" {
            http.Error(w, "Wrong username or password", http.StatusUnauthorized)
        } else {
            http.Error(w, "Failed to read from ElasticSearch", http.StatusInternalServerError)
        }
        return
    }
    // Create a new token object, specifying signing method and the claims
    // you would like it to contain.
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
        "username": user.Username,  //first key-value pair : username
        "exp": time.Now().Add(time.Hour * 24).Unix(), //second key-value pair : expire time
    })
    //Sign and get the complete encoded token as a string using the secret
    tokenString, err := token.SignedString(mySigningKey)
    if err != nil {
        http.Error(w, "Failed to generate token", http.StatusInternalServerError)
        fmt.Printf("Failed to generate token %v.\n", err)
        return
    }

    w.Write([]byte(tokenString))
}
//handle signup request, it will call addUser
func handlerSignup(w http.ResponseWriter, r *http.Request) {
    fmt.Println("Received one signup request")
    w.Header().Set("Content-Type", "text/plain")
    w.Header().Set("Access-Control-Allow-Origin", "*")

    if r.Method == "OPTIONS" {
        return
    }

    decoder := json.NewDecoder(r.Body)
    var user User
    if err := decoder.Decode(&user); err != nil {
        http.Error(w, "Cannot decode user data from client", http.StatusBadRequest)
        fmt.Printf("Cannot decode user data from client %v.\n", err)
        return
    }
                                                                 //ensure lower case because Elasticsearch is not case-sensitive
    if user.Username == "" || user.Password == "" || !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(user.Username) {
        http.Error(w, "Invalid username or password", http.StatusBadRequest)
        fmt.Printf("Invalid username or password.\n")
        return
    }

    if err := addUser(user); err != nil {
        if err.Error() == "User already exists" {
            http.Error(w, "User already exists", http.StatusBadRequest)
        } else {
            http.Error(w, "Failed to save to ElasticSearch", http.StatusInternalServerError)
        }
        return
    }

    w.Write([]byte("User added successfully.")) //
}
