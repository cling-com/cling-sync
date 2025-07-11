//go:build wasm

package main

import (
	"fmt"
	"runtime"
	"time"

	"syscall/js"
)

func main() {
	js.Global().Set("repositoryAPI", BuildRepositoryAPI())
	monitorMemory()

	// Keep the program running, because the Wasm module is unloaded when `main` returns.
	select {}
}

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf(" Alloc = %v MB", m.Alloc/1024/1024)
	fmt.Printf(" TotalAlloc = %v MB", m.TotalAlloc/1024/1024)
	fmt.Printf(" Sys = %v MB", m.Sys/1024/1024)
	// `m.NumGC` is not available when compiling with TinyGo.
	// fmt.Printf(" NumGC = %v\n", m.NumGC)
	fmt.Printf(" HeapAlloc = %v MB", m.HeapAlloc/1024/1024)
	fmt.Printf(" HeapSys = %v MB", m.HeapSys/1024/1024)
	fmt.Printf(" HeapInuse = %v MB", m.HeapInuse/1024/1024)
	fmt.Printf(" HeapReleased = %v MB\n", m.HeapReleased/1024/1024)
}

func monitorMemory() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			printMemStats()
		}
	}()
}
