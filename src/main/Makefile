.PHONY: master
master:
	go build -buildmode=plugin ../mrapps/wc.go
	go run mrmaster.go pg-*.txt

.PHONY: worker
worker:
	go build -buildmode=plugin ../mrapps/wc.go
	go run mrworker.go wc.so

.PHONY: clean
clean:
	rm -rf temp_map