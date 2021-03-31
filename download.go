package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const sizePerRoutine = 100
const numRoutines = 10
const timeout = 15

type partialData struct {
	start int
	body  []byte
}

var url string
var dch = make(chan partialData)
var sch = make(chan int)
var endch = make(chan int)
var data = make(map[int][]byte)

func main() {
	// 取得先のURLを取得
	url = os.Getenv("GET_URL")

	// Content-Lengthを取得
	cl, err := Head(url)
	if err != nil {
		log.Fatal(err)
	}

	// コンテキスト生成
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	// error groupを作成
	g, ctx := errgroup.WithContext(ctx)

	// サブルーチンの終了をコントロール
	// サブルーチンの数だけwg.Done()が通ったら，終了シグナルをブロードキャストしてサブルーチンを終了する
	// 終了シグナルをメインルーチンに送信
	var wg sync.WaitGroup
	wg.Add(numRoutines)
	go func() {
		wg.Wait()
		close(sch)
		close(endch)
	}()

	// numRoutinesの分だけgo routineを作成
	for i := 0; i < numRoutines; i++ {
		g.Go(func() error {
			for {
				select {
				case s, ok := <-sch:
					if !ok {
						return nil
					}
					p := partialData{start: s}
					var err error
					p.body, err = Send(ctx, s, s+sizePerRoutine)
					if err != nil {
						return errors.Wrap(err, "Send failed")
					}

					select {
					case dch <- p:
					case <-ctx.Done():
						return ctx.Err()
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	// 最初のinput
	currentMax := 0
	for i := 0; i < numRoutines; i++ {
		currentMax = sizePerRoutine * i
		sch <- currentMax
	}
L:
	for {
		select {
		// ダウンロードデータを受信，次の読み込み位置を送信
		// 終端まで読み込んだいたら受信した回数分wg.Done()をする
		case p := <-dch:
			data[p.start] = p.body
			if currentMax+sizePerRoutine > cl {
				wg.Done()
			} else {
				currentMax += sizePerRoutine
				select {
				case sch <- currentMax:
				case <-ctx.Done():
				}
			}
		case <-endch:
			break L
		case <-ctx.Done():
			break L
		}
	}

	// すべてのサブルーチンが完了したらunlock
	// どこかでエラーが発生したかをチェック
	if err := g.Wait(); err != nil {
		log.Fatal(errors.Wrap(err, "An error occurred in go routines"))
	}

	// 受信したデータをマージする
	full := merge(data, currentMax+sizePerRoutine)

	// ファイルを書き込み
	fname := filepath.Base(url)
	if err := os.WriteFile(fname, full, 0600); err != nil {
		log.Fatal(err)
	}
	//　ダウンロードデータを表示
	fmt.Println(string(full))
}

// s以上e未満のrange accessを行う
func Send(ctx context.Context, s, e int) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "NewRequest failed")
	}

	r := fmt.Sprintf("bytes=%v-%v", s, e-1)
	req.Header.Add("Range", r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "ClientDo failed")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "ReadBody failed")
	}

	return body, nil
}

// Headリクエストを送り，Content-Lengthを取得
func Head(url string) (int, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, errors.Wrap(err, "Head failed")
	}
	defer resp.Body.Close()

	if resp.Header.Get("Accept-Ranges") != "bytes" {
		return 0, errors.New("Range access failed")
	}

	cl, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return 0, errors.Wrap(err, "strconv content-length failed")
	}
	return cl, nil
}

// 分割ダウンロードした[]byteをマージする
func merge(m map[int][]byte, length int) []byte {
	b := make([]byte, 0, length)
	s := 0
	b = append(b, m[s]...)
	for {
		s += sizePerRoutine
		if p, ok := m[s]; !ok {
			break
		} else {
			b = append(b, p...)
		}
	}
	return b
}
