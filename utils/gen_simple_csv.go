package main

import "fmt"

func main() {
	fmt.Println("i")
	for i := 0; i < 10000000; i++ {
		fmt.Println(i%5)
	}
}
