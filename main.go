package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	lz4 "rzstd/src"

	lz4lib "github.com/pierrec/lz4/v4"
)

func main() {
	var (
		decompress = flag.Bool("d", false, "Decompress the input file")
		input      = flag.String("i", "C:\\Users\\199-4\\labs\\hasd\\lab4\\data\\lorem.txt", "Input file path")
		output     = flag.String("o", "", "Output file path (optional)")
		useLibrary = flag.Bool("lib", false, "Use standard library LZ4 instead of custom implementation")
	)

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [-d] [-lib] -i INPUT [-o OUTPUT]\n", filepath.Base(os.Args[0]))
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *input == "" {
		log.Fatal("Error: input file (-i) is required")
	}

	// Determine output filename if not provided
	if *output == "" {
		if *decompress {
			*output = *input + ".dec"
		} else {
			*output = *input + ".lz4"
		}
	}

	inFile, err := os.Open(*input)
	if err != nil {
		log.Fatalf("Error opening input file: %v", err)
	}
	defer inFile.Close()

	outFile, err := os.Create(*output)
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outFile.Close()

	if *decompress {
		if *useLibrary {
			log.Println("Decomressing with lz4 lib")
			err = decompressWithLibrary(inFile, outFile)
		} else {
			log.Println("Decomressing with custom impl")
			err = lz4.DecompressStream(inFile, outFile)
		}
		if err != nil {
			log.Fatalf("Decompression failed: %v", err)
		}
		fmt.Printf("Decompressed '%s' -> '%s'\n", *input, *output)
	} else {
		if *useLibrary {
			log.Println("Comressing with lz4 lib")
			err = compressWithLibrary(inFile, outFile)
		} else {
			log.Println("Compressing with custom impl")
			err = lz4.CompressStream(inFile, outFile)
		}
		if err != nil {
			log.Fatalf("Compression failed: %v", err)
		}
		fmt.Printf("Compressed '%s' -> '%s'\n", *input, *output)
	}
}

func compressWithLibrary(src io.Reader, dst io.Writer) error {
	w := lz4lib.NewWriter(dst)
	w.Apply(lz4lib.BlockSizeOption(lz4lib.Block4Mb))
	defer w.Close()

	_, err := io.Copy(w, src)
	return err
}

func decompressWithLibrary(src io.Reader, dst io.Writer) error {
	r := lz4lib.NewReader(src)
	_, err := io.Copy(dst, r)
	return err
}
