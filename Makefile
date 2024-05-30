all: install clean

install:
	python -m pip install .

clean:
	rm -r build dist *.egg-info .cache bm25_model
