package main

//
// simple sequential MapReduce.
//

import (
	"fmt"
	"plugin"
	"os"
	"log"
	"io"
	"sort"
	"lab4/wc"
)

// for sorting by key.
type ByKey []wc.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Mapper function
// TODO: Modify function to distribute mapping work to nodes based on data chunks
func Mapper() {
	dataFiles := []string{"./data/pg-being_ernest.txt", "./data/pg-metamorphosis.txt"}

	intermediate := []wc.KeyValue{}
	// read each input file,
	for _, file := range dataFiles {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		content, err := io.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		f.Close()
		kva := wc.Map(file, string(content))
		intermediate = append(intermediate, kva...)
	}

	// Sort the intermediate key/value pairs by key
	sort.Sort(ByKey(intermediate))
}

// Distributed Reduce function that takes number of nodes to distribute to and intermediate key/value pairs
func DistributedReduce(numNodes int, intermediate []wc.KeyValue) {
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// TODO: Send intermediate[i] and values to each node for processing

		i = j
	}
}

func main() {
	// if len(os.Args) < 3 {
	// 	fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
	// 	os.Exit(1)
	// }

	// mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []wc.KeyValue{}
	for _, filename := range os.Args[1:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := wc.Map(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := wc.Reduce(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []wc.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []wc.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
