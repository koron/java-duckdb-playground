package main

import "fmt"

func main() {
	fmt.Println("i,j")
	for i := 0; i < 10000000; i++ {
		fmt.Printf("%d,%d\n", i%5, i%100)
	}
}
