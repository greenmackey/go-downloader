package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const size = 100
const div = 10
const tout = 15

var url string
var dch = make(chan []byte)
var clch = make(chan int)
var endch = make(chan int)

func main() {
	// 取得先のURLを取得
	url = os.Getenv("GET_URL")

	// Content-Lengthを取得
	cl, err := Head(url)
	if err != nil {
		log.Fatal(err)
	}

	// ダウンロードデータの保存先を用意
	full := make([]byte, 0, cl+100)

	// divの分だけgo routineを作成
	g := new(errgroup.Group)
	for i := 0; i < div; i++ {
		g.Go(Download)
	}

	// すべてのgo routineが完了したことを管理
	go func() {
		if err := g.Wait(); err != nil {
			log.Fatal(err)
		}
		close(endch)
	}()

	// タイムアウト用のchannelを用意
	tch := time.After(time.Duration(tout) * time.Second)

	// 最初のinput
	clch <- 0
L:
	for {
		select {
		// ダウンロードデータを受信，格納済みデータサイズを送信
		case b := <-dch:
			full = append(full, b...)
			if len(full) >= cl {
				close(clch)
			} else {
				clch <- len(full)
			}
		// タイムアウトを確認
		case <-tch:
			fmt.Println("timeout")
			break L
		// すべてのgo routineが完了したか確認
		case <-endch:
			break L
		}
	}

	// ファイルを書き込み
	fname := filepath.Base(url)
	if err := os.WriteFile(fname, full, 0600); err != nil {
		log.Fatal(err)
	}
	//　ダウンロードデータを表示
	fmt.Println(string(full))
}

func Send(url string, s, e int) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "NewRequest failed")
	}

	r := fmt.Sprintf("bytes=%v-%v", s, e)
	req.Header.Add("Range", r)
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "ClientDo failed.")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "ReadBody failed.")
	}
	return body, nil
}

func Head(url string) (int, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, errors.Wrap(err, "Head failed")
	}
	defer resp.Body.Close()

	if resp.Header.Get("Accept-Ranges") != "bytes" {
		return 0, errors.New("Range access failed.")
	}

	cl, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return 0, errors.Wrap(err, "strconv content-length failed.")
	}
	return cl, nil
}

func Download() error {
	for {
		if s, ok := <-clch; !ok {
			return nil
		} else {
			body, err := Send(url, s, s+size)
			if err != nil {
				return errors.Wrap(err, "Send failed.")
			}
			dch <- body
		}
	}
}
