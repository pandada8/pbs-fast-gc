pbs-fast-gc: *.go go.mod go.sum
	CGO_ENABLED=0 go build -o pbs-fast-gc .