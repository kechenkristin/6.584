First, make sure the word-count plugin is freshly built:
```
go build -buildmode=plugin ../mrapps/wc.go
```

start the coordinator
```
go run mrcoordinator.go pg-*.txt
```

start the worker
```
go run mrworker.go wc.so
```

clean up
```
rm -f mr-*-*
```