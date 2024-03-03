all: install clean

install:
	python -m pip install .

proto:
	protoc --cpp_out=. bm25/bm25.proto

clean:
	rm -r build dist *.egg-info .cache
