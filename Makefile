.PHONY: buf build

buf:
	buf generate

build:
	$(MAKE) -C anyserve build
	$(MAKE) -C client-python build

clean:
	$(MAKE) -C anyserve clean
	$(MAKE) -C client-python clean
